-- Raw staging layer: one row per hour, both datasets already joined at load time
CREATE TABLE IF NOT EXISTS raw_hourly_grid (
    begin_datetime_mpt TIMESTAMP NOT NULL,
    pool_price NUMERIC,
    forecast_pool_price NUMERIC,
    rolling_30day_avg NUMERIC,
    alberta_internal_load NUMERIC,
    forecast_alberta_internal_load NUMERIC,
    ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    source_run_id TEXT NOT NULL,
    PRIMARY KEY (begin_datetime_mpt)
);

-- Mart layer: daily aggregates, computed FROM raw_hourly_grid via SQL
CREATE TABLE IF NOT EXISTS daily_grid_agg (
    grid_date DATE PRIMARY KEY,
    avg_price NUMERIC,
    min_price NUMERIC,
    max_price NUMERIC,
    stddev_price NUMERIC,
    avg_load NUMERIC,
    hours_reported INTEGER,
    computed_at TIMESTAMP NOT NULL DEFAULT now()
);