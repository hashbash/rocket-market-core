import logging
import requests
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.dates import parse_execution_date

import numpy as np
import pandas as pd
import yfinance as yf


default_args = {
    'owner': 'airflow',
    'email': ['ambob@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='yf_load_tickers_daily',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    start_date=days_ago(3),
    catchup=False,
    max_active_runs=1
)


def load_data(**context):
    postgres_hook = PostgresHook('admin_postgres')
    tickers = postgres_hook.get_records('select yf_code from tickers where fetch_from_yahoo_finance')
    logging.info('Loaded %d tickers from db.' % len(tickers))
    tickers = [x[0] for x in tickers]
    frequency = '1d'
    start_dt = parse_execution_date(context['yesterday_ds']) - timedelta(days=7)
    data = yf.download(
        tickers=tickers,
        start=start_dt,
        end=context['tomorrow_ds'],
        interval=frequency,
        auto_adjust=True,
        group_by='ticker',
        progress=False,
        threads=True
    )

    columns_mapping = {'Date': 'ts', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume',
                       'Adj Close': 'adj_close'}
    ch_columns = ['ticker', 'frequency', 'source', 'type'] + list(columns_mapping.values())
    df = None
    for ticker in tickers:
        try:
            _df = data[ticker].copy()
        except KeyError:
            logging.error('Ticker %s not found in data' % ticker)
            continue
        _df = _df.reset_index()
        _df['ticker'] = ticker
        _df['frequency'] = frequency
        _df['source'] = 'yfinance'
        _df['type'] = 'history'
        _df = _df.rename(columns=columns_mapping)
        if 'adj_close' not in _df.columns:
            _df['adj_close'] = np.nan
        _df = _df[ch_columns]
        _df = _df[~_df.close.isna()]

        if df is None:
            df = _df
        else:
            df = pd.concat([df, _df])

    logging.info('Prepared df with shape (%s, %s)' % df.shape)
    ch_hook = BaseHook(None)
    ch_conn = ch_hook.get_connection('rocket_clickhouse')
    data_json_each = ''
    df.reset_index(drop=True, inplace=True)
    for i in df.index:
        json_str = df.loc[i].to_json(date_format='iso')
        data_json_each += json_str + '\n'

    result = requests.post(url=ch_conn.host, data=data_json_each,
                           params=dict(
                               query='insert into rocket.events format JSONEachRow',
                               user=ch_conn.login,
                               password=ch_conn.password,
                               date_time_input_format='best_effort',
                           ))
    if result.ok:
        logging.info('Insert ok.')
    else:
        raise requests.HTTPError('Request response code: %d. Message: %s' % (result.status_code, result.text))


load_data_op = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)
