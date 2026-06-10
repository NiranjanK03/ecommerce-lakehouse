-- MV Q01: Daily revenue — reads from pre-materialized Gold MV, not Silver.
-- This is what the text-to-SQL agent actually runs. No join, no scan — point read.
SELECT order_date, category, total_revenue, order_count
FROM gold.daily_revenue
ORDER BY order_date DESC, total_revenue DESC
LIMIT 100
