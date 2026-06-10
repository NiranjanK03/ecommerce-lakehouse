-- Q01: Daily revenue by product category (delivered orders only)
-- Complexity: 3-table join + GROUP BY date + SUM/COUNT aggregation
-- Tests: join broadcast, date function, multi-column GROUP BY
SELECT
    DATE(o.purchased_at)           AS order_date,
    p.product_category             AS category,
    SUM(oi.price)                  AS total_revenue,
    COUNT(DISTINCT o.order_id)     AS order_count
FROM silver.orders      o
JOIN silver.order_items oi ON o.order_id = oi.order_id
JOIN silver.products    p  ON oi.product_id = p.product_id
WHERE o.status     = 'delivered'
  AND o.is_deleted  = false
  AND oi.is_deleted = false
  AND p.is_deleted  = false
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
