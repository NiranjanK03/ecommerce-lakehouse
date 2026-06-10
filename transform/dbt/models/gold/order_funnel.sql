{{
    config(
        materialized='materialized_view',
        refresh_async=True
    )
}}

-- Order funnel by week: counts orders at each stage of the fulfillment lifecycle.
-- Reveals bottlenecks (e.g. high created-but-not-approved rate signals payment issues).
-- Stages follow the Olist order status progression:
--   created → approved → invoiced → processing → shipped → delivered (or canceled)
SELECT
    DATE_TRUNC('week', o.purchased_at)                                       AS order_week,
    COUNT(DISTINCT o.order_id)                                               AS total_created,
    COUNT(DISTINCT CASE WHEN o.approved_at IS NOT NULL
                        THEN o.order_id END)                                 AS approved,
    COUNT(DISTINCT CASE WHEN o.status IN
                        ('invoiced','processing','shipped','delivered')
                        THEN o.order_id END)                                 AS invoiced_or_later,
    COUNT(DISTINCT CASE WHEN o.delivered_carrier_at IS NOT NULL
                        THEN o.order_id END)                                 AS shipped,
    COUNT(DISTINCT CASE WHEN o.status = 'delivered'
                        THEN o.order_id END)                                 AS delivered,
    COUNT(DISTINCT CASE WHEN o.status = 'canceled'
                        THEN o.order_id END)                                 AS canceled,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN o.status = 'delivered' THEN o.order_id END)
              / NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                        AS delivery_rate_pct
FROM {{ source('silver', 'orders') }} o
WHERE o.is_deleted = false
GROUP BY 1
