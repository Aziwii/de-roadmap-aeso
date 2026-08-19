-- ====================================================================
-- DBEAVER ANALYSIS: 10-DAY ROLLING BATTERY ARBITRAGE & MARKET METRICS
-- ====================================================================

-- 1. Query the 10-Day Rolling Window Financial Performance
SELECT 
    trade_date_mpt,
    min_charge_price AS charge_price_per_mwh,
    max_discharge_price AS discharge_price_per_mwh,
    daily_charge_cost_cad,
    daily_discharge_revenue_cad,
    daily_net_profit_cad,
    rolling_10d_cumulative_profit_cad,
    rolling_10d_avg_daily_profit_cad
FROM view_battery_arbitrage_10d_rolling
ORDER BY trade_date_mpt DESC
LIMIT 30;

-- 2. Correlate Alberta Internal Load against Pool Price Spreads
SELECT
    p.begin_datetime_mpt,
    p.pool_price,
    p.forecast_pool_price,
    l.alberta_internal_load,
    l.forecast_alberta_internal_load,
    (p.pool_price - p.forecast_pool_price) AS price_forecast_error,
    (l.alberta_internal_load - l.forecast_alberta_internal_load) AS load_forecast_error
FROM silver_pool_price p
JOIN silver_actual_forecast_load l 
  ON p.begin_datetime_utc = l.begin_datetime_utc
ORDER BY p.begin_datetime_mpt DESC
LIMIT 48;