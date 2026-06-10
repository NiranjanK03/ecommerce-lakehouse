-- MV Q02: Top sellers — reads from pre-materialized Gold MV.
SELECT seller_id, seller_state, total_orders, gmv, delivery_rate_pct
FROM gold.top_sellers
ORDER BY gmv DESC
LIMIT 50
