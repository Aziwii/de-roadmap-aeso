
-- Query 1 - view the daily battery arbitrage profit and cumulative profit since project start 
SELECT 
    grid_date,
    net_daily_profit,
    SUM(net_daily_profit) OVER (ORDER BY grid_date) AS total_profit_acc
FROM v_daily_battery_arbitrage
ORDER BY grid_date ASC;

--  Query 2 - get the total profit for the last 10 days
SELECT 
    SUM(net_daily_profit) AS ten_day_profit
FROM (
    SELECT 
        net_daily_profit,
        ROW_NUMBER() OVER (ORDER BY grid_date DESC) AS rn
    FROM v_daily_battery_arbitrage
) sub
WHERE sub.rn <= 10;