-- Q03: Review score distribution by customer state
-- Complexity: 3-table join across orders + reviews + customers + multi-bucket conditional SUM
-- Tests: many-to-one join fanout, conditional aggregation, state-level rollup
SELECT
    c.customer_state,
    COUNT(r.review_id)                                          AS review_count,
    ROUND(AVG(r.review_score), 2)                              AS avg_score,
    SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END)       AS positive_count,
    SUM(CASE WHEN r.review_score = 3  THEN 1 ELSE 0 END)       AS neutral_count,
    SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END)       AS negative_count
FROM silver.order_reviews r
JOIN silver.orders         o ON r.order_id = o.order_id
JOIN silver.customers      c ON o.customer_id = c.customer_id
WHERE r.is_deleted = false
  AND o.is_deleted = false
  AND c.is_deleted = false
GROUP BY 1
ORDER BY 2 DESC
