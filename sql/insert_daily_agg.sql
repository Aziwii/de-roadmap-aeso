WITH daily_summary AS (
    SELECT 
        DATE_TRUNC('day', begin_datetime_mpt)::DATE AS grid_date,
        ROUND(AVG(pool_price)::NUMERIC, 2) AS price_avg,
        MIN(pool_price) AS min_price,
        MAX(pool_price) AS max_price,
        MAX(pool_price) - MIN(pool_price) AS daily_price_spread,
        ROUND(STDDEV(pool_price)::NUMERIC, 2) AS stddev_price,
        ROUND(AVG(alberta_internal_load)::NUMERIC, 2) AS avg_load, 
        count(*) as hours_reported
    FROM raw_hourly_grid
    GROUP BY 1
)
SELECT * 
FROM daily_summary
ORDER BY grid_date;