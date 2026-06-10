{{
    config(
        materialized='materialized_view',
        refresh_async=True
    )
}}

-- Review score distribution by customer state.
-- Week 4: aggregates the raw review_score (1–5 integer).
-- Week 5 (AI enrichment): sentiment_label will be added to Silver order_reviews
-- via a Claude API enrichment job — update this model then to include it.
SELECT
    c.customer_state,
    COUNT(r.review_id)                                        AS review_count,
    ROUND(AVG(r.review_score), 2)                            AS avg_score,
    SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END)    AS positive_count,
    SUM(CASE WHEN r.review_score = 3  THEN 1 ELSE 0 END)    AS neutral_count,
    SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END)    AS negative_count,
    ROUND(
        100.0 * SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(r.review_id), 0),
        2
    )                                                         AS positive_rate_pct
FROM {{ source('silver', 'order_reviews') }}  r
JOIN {{ source('silver', 'orders') }}         o  USING (order_id)
JOIN {{ source('silver', 'customers') }}      c  USING (customer_id)
WHERE
    r.is_deleted = false
    AND o.is_deleted = false
    AND c.is_deleted = false
GROUP BY 1
