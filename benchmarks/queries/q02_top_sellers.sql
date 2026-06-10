-- Q02: Top sellers by gross merchandise value + delivery performance
-- Complexity: 3-table join + conditional SUM + NULLIF + ORDER + LIMIT
-- Tests: hash join, CASE expression aggregation, sort + limit
SELECT
    s.seller_id,
    s.seller_state,
    COUNT(DISTINCT o.order_id)                                                AS total_orders,
    SUM(oi.price)                                                             AS gmv,
    ROUND(
        100.0 * SUM(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                         AS delivery_rate_pct
FROM silver.sellers     s
JOIN silver.order_items oi ON s.seller_id = oi.seller_id
JOIN silver.orders      o  ON oi.order_id = o.order_id
WHERE s.is_deleted  = false
  AND oi.is_deleted = false
  AND o.is_deleted  = false
GROUP BY 1, 2
ORDER BY 4 DESC
LIMIT 100
