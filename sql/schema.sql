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

-- View layer: daily battery arbitrage profit, computed FROM daily_grid_agg via SQL

CREATE OR REPLACE VIEW v_daily_battery_arbitrage AS 
SELECT 	
    grid_date,
    max_price,
    min_price,
    ROUND(min_price * 10, 2) AS charge_cost,
    ROUND(9 * max_price, 2) AS discharge_revenue,
    ROUND((9 * max_price) - (min_price * 10), 2) AS net_daily_profit
FROM daily_grid_agg;
