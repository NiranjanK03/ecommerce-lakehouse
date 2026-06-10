-- Q04: Weekly order funnel from created to delivered
-- Complexity: DATE_TRUNC + 6 conditional COUNTs on a single wide table
-- Tests: date bucketing, multiple CASE expressions over the same column scan
SELECT
    DATE_TRUNC('week', o.purchased_at)                                        AS order_week,
    COUNT(DISTINCT o.order_id)                                                AS total_created,
    COUNT(DISTINCT CASE WHEN o.approved_at IS NOT NULL
                        THEN o.order_id END)                                  AS approved,
    COUNT(DISTINCT CASE WHEN o.delivered_carrier_at IS NOT NULL
                        THEN o.order_id END)                                  AS shipped,
    COUNT(DISTINCT CASE WHEN o.status = 'delivered'
                        THEN o.order_id END)                                  AS delivered,
    COUNT(DISTINCT CASE WHEN o.status = 'canceled'
                        THEN o.order_id END)                                  AS canceled,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN o.status = 'delivered' THEN o.order_id END)
              / NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                         AS delivery_rate_pct
FROM silver.orders o
WHERE o.is_deleted = false
GROUP BY 1
ORDER BY 1 DESC
