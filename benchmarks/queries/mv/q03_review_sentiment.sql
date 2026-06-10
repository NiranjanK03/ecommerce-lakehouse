-- MV Q03: Review sentiment by state — reads from pre-materialized Gold MV.
SELECT customer_state, review_count, avg_score, positive_count, positive_rate_pct
FROM gold.review_sentiment_by_state
ORDER BY review_count DESC
