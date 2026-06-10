-- MV Q04: Order funnel — reads from pre-materialized Gold MV.
-- Note: Q05 (cohort retention) has no MV equivalent — it requires a self-join
-- that cannot be expressed as a static pre-aggregation. This is a deliberate
-- design choice: simple recurring queries get MVs; ad-hoc complex queries
-- go through the StarRocks iceberg_catalog scan path.
SELECT order_week, total_created, delivered, canceled, delivery_rate_pct
FROM gold.order_funnel
ORDER BY order_week DESC
LIMIT 52
