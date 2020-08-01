import logging
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import yfinance as yf
import pandahouse as ph


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
)


def load_data(**context):
    postgres_hook = PostgresHook('admin_postgres')
    tickers = postgres_hook.get_records('select yf_code from tickers where fetch_from_yahoo_finance')
    logging.info('Loaded %d tickers from db.' % len(tickers))
    tickers = [x[0] for x in tickers]
    frequency = '1d'
    data = yf.download(
        tickers=tickers,
        start=context['yesterday_ds'],
        end=context['tomorrow_ds'],
        interval=frequency,
        auto_adjust=True,
        group_by='ticker',
        threads=20
    )

    columns_mapping = {'Date': 'ts', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume',
                       'Adj Close': 'adj_close'}
    ch_columns = ['ticker', 'frequency', 'source', 'type'] + list(columns_mapping.values())
    df = None
    for ticker in tickers:
        try:
            _df = data[ticker]
        except KeyError:
            logging.error('Ticker %s not found in data' % ticker)
            continue
        _df.reset_index(inplace=True)
        _df['ticker'] = ticker
        _df['frequency'] = frequency
        _df['source'] = 'yfinance'
        _df['type'] = 'history'
        _df.rename(columns=columns_mapping, inplace=True)
        _df = _df[ch_columns]
        _df = _df[~_df.close.isna()]

        if df is None:
            df = _df
        else:
            df = pd.concat([df, _df])

    logging.info('Prepared df with shape (%s, %s)' % df.shape)
    ch_hook = BaseHook(None)
    ch_conn = ch_hook.get_connection('rocket_clickhouse')
    affected_rows = ph.to_clickhouse(df=df, table='rocket.events',
                                     connection={'host': ch_conn.host, 'database': ch_conn.schema,
                                                 'user': ch_conn.login, 'password': ch_conn.password})
    logging.info('Inserted %d rows' % affected_rows)


load_data_op = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)
