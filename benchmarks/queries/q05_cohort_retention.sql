-- Q05: 90-day customer cohort retention
-- Complexity: CTE + self-join + INTERVAL date arithmetic + multi-level COUNT DISTINCT
-- Tests: complex planning (self-join with range predicate), heavy aggregation, window-style logic
-- Note: uses INTERVAL arithmetic (not DATEDIFF) for cross-engine compatibility with Trino + StarRocks.
WITH first_orders AS (
    SELECT
        customer_id,
        MIN(purchased_at)                       AS first_purchase_at,
        DATE_TRUNC('month', MIN(purchased_at))  AS cohort_month
    FROM silver.orders
    WHERE status    != 'canceled'
      AND is_deleted = false
    GROUP BY 1
)
SELECT
    fo.cohort_month,
    COUNT(DISTINCT fo.customer_id)                                                          AS cohort_size,
    COUNT(DISTINCT CASE
            WHEN o.purchased_at >  fo.first_purchase_at
             AND o.purchased_at <= fo.first_purchase_at + INTERVAL '30' DAY
            THEN o.customer_id END)                                                         AS retained_30d,
    COUNT(DISTINCT CASE
            WHEN o.purchased_at >  fo.first_purchase_at
             AND o.purchased_at <= fo.first_purchase_at + INTERVAL '60' DAY
            THEN o.customer_id END)                                                         AS retained_60d,
    COUNT(DISTINCT CASE
            WHEN o.purchased_at >  fo.first_purchase_at
             AND o.purchased_at <= fo.first_purchase_at + INTERVAL '90' DAY
            THEN o.customer_id END)                                                         AS retained_90d,
    ROUND(
        100.0 * COUNT(DISTINCT CASE
                        WHEN o.purchased_at >  fo.first_purchase_at
                         AND o.purchased_at <= fo.first_purchase_at + INTERVAL '90' DAY
                        THEN o.customer_id END)
              / NULLIF(COUNT(DISTINCT fo.customer_id), 0),
        2
    )                                                                                       AS retention_90d_pct
FROM first_orders fo
LEFT JOIN silver.orders o
       ON o.customer_id  = fo.customer_id
      AND o.status      != 'canceled'
      AND o.is_deleted   = false
      AND o.purchased_at >  fo.first_purchase_at
      AND o.purchased_at <= fo.first_purchase_at + INTERVAL '90' DAY
GROUP BY 1
ORDER BY 1
