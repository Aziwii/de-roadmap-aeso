--step 1: lowest pool price buy(price), highest (sell pri
--assume 10 MWh battery capacity

--  the view for daily 10 MWh battery arbitrage (90% round-trip efficiency)
CREATE OR REPLACE VIEW v_daily_battery_arbitrage AS 
SELECT 	
    grid_date,
    max_price,
    min_price,
    ROUND(min_price * 10, 2) AS charge_cost,
    ROUND(9 * max_price, 2) AS discharge_revenue,
    ROUND((9 * max_price) - (min_price * 10), 2) AS net_daily_profit
FROM daily_grid_agg;


-- Q1: View daily profit and cumulative running total profit over time
SELECT 
    grid_date,
    net_daily_profit,
    SUM(net_daily_profit) OVER (ORDER BY grid_date) AS total_profit_acc
FROM v_daily_battery_arbitrage
ORDER BY grid_date ASC;


-- Q2: Get total profit over past 10 days
SELECT 
    sub.grid_date,
    sub.net_daily_profit,
    SUM(sub.net_daily_profit) OVER () AS total_10_day_profit, 
    Sum(sub.net_daily_profit) over (order by sub.grid_date rows between 9 preceding and current row) as rolling_10d_sum
FROM (
    SELECT 
        grid_date,
        net_daily_profit,
        ROW_NUMBER() OVER (ORDER BY grid_date DESC) AS rn
    FROM v_daily_battery_arbitrage
) sub
WHERE sub.rn <= 10
ORDER BY sub.grid_date DESC;

--
--select * from raw_hourly_grid rhg 
--select * from daily_grid_agg 

---------------------------------------------------

select 
	corr(pool_price, alberta_internal_load) as demand_price_correlation
from raw_hourly_grid rhg 