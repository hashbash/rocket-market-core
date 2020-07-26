create table rocket.events (
    ticker LowCardinality(String),
    type LowCardinality(String),
    ts DateTime,
    tz LowCardinality(String),
    low Float64,
    high Float64,
    open Float64,
    close Float64,
    volume Float64,
    frequency LowCardinality(String),
    source LowCardinality(String),
    model_id Int16,
    prediction_id Int64,
    created DateTime default now()
)
engine ReplacingMergeTree(created)
partition by toDate(ts)
order by (ticker, type, frequency, tz, model_id, prediction_id, ts);