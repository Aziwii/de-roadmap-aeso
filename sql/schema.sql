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
    created_run_id TEXT NOT NULL,
    updated_run_id TEXT NOT NULL,
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


------------------- AI GENERATED SCHEMA -------------------------
-- DDL for AESO Market Data and Battery Arbitrage Views

CREATE TABLE IF NOT EXISTS silver_pool_price (
    begin_datetime_utc TIMESTAMP PRIMARY KEY,
    begin_datetime_mpt TIMESTAMP NOT NULL,
    pool_price NUMERIC(10, 2),
    forecast_pool_price NUMERIC(10, 2),
    rolling_30day_avg NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver_actual_forecast_load (
    begin_datetime_utc TIMESTAMP PRIMARY KEY,
    begin_datetime_mpt TIMESTAMP NOT NULL,
    alberta_internal_load NUMERIC(10, 2),
    forecast_alberta_internal_load NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Daily Daily Max/Min Prices for Arbitrage (10 MWh Battery, 90% RTE)
CREATE OR REPLACE VIEW view_daily_battery_arbitrage AS
WITH daily_prices AS (
    SELECT
        DATE(begin_datetime_mpt) AS trade_date_mpt,
        MIN(pool_price) AS min_charge_price,
        MAX(pool_price) AS max_discharge_price
    FROM silver_pool_price
    GROUP BY DATE(begin_datetime_mpt)
),
battery_specs AS (
    SELECT
        10.0 AS battery_capacity_mwh,  -- 10 MWh capacity
        0.90 AS round_trip_efficiency  -- 90% round trip efficiency
)
SELECT
    dp.trade_date_mpt,
    dp.min_charge_price,
    dp.max_discharge_price,
    bs.battery_capacity_mwh,
    bs.round_trip_efficiency,
    ROUND(((bs.battery_capacity_mwh / bs.round_trip_efficiency) * dp.min_charge_price)::NUMERIC, 2) AS daily_charge_cost_cad,
    ROUND((bs.battery_capacity_mwh * dp.max_discharge_price)::NUMERIC, 2) AS daily_discharge_revenue_cad,
    ROUND((
        (bs.battery_capacity_mwh * dp.max_discharge_price) -
        ((bs.battery_capacity_mwh / bs.round_trip_efficiency) * dp.min_charge_price)
    )::NUMERIC, 2) AS daily_net_profit_cad
FROM daily_prices dp
CROSS JOIN battery_specs bs;

-- Rolling 10-Day Window Financial Metric View
CREATE OR REPLACE VIEW view_battery_arbitrage_10d_rolling AS
SELECT
    trade_date_mpt,
    min_charge_price,
    max_discharge_price,
    daily_charge_cost_cad,
    daily_discharge_revenue_cad,
    daily_net_profit_cad,
    ROUND(
        SUM(daily_net_profit_cad) OVER (
            ORDER BY trade_date_mpt 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2
    ) AS rolling_10d_cumulative_profit_cad,
    ROUND(
        AVG(daily_net_profit_cad) OVER (
            ORDER BY trade_date_mpt 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        )::NUMERIC, 2
    ) AS rolling_10d_avg_daily_profit_cad
FROM view_daily_battery_arbitrage;