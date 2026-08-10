-- Raw staging layer: one row per AESO record as returned, plus load metadata
CREATE TABLE IF NOT EXISTS raw_pool_price (
    begin_datetime_mpt TIMESTAMP NOT NULL,
    pool_price NUMERIC,
    forecast_pool_price NUMERIC,
    ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    source_run_id TEXT NOT NULL,
    PRIMARY KEY (begin_datetime_mpt)
);

-- Clean mart layer: one row per calendar date
CREATE TABLE IF NOT EXISTS daily_pool_price_agg (
    price_date DATE PRIMARY KEY,
    avg_price NUMERIC,
    min_price NUMERIC,
    max_price NUMERIC,
    stddev_price NUMERIC,
    hours_reported INTEGER,
    null_hours_dropped INTEGER,
    computed_at TIMESTAMP NOT NULL DEFAULT now()
);
