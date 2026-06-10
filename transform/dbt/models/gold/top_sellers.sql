{{
    config(
        materialized='materialized_view',
        refresh_async=True
    )
}}

-- Top sellers by gross merchandise value (GMV) and delivery performance.
-- Used by the text-to-SQL agent for marketplace analytics queries.
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT o.order_id)                                           AS total_orders,
    SUM(oi.price)                                                        AS gmv,
    SUM(oi.freight_value)                                                AS total_freight,
    ROUND(
        100.0 * SUM(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(DISTINCT o.order_id), 0),
        2
    )                                                                    AS delivery_rate_pct,
    AVG(
        DATEDIFF(o.delivered_customer_at, o.purchased_at)
    )                                                                    AS avg_delivery_days
FROM {{ source('silver', 'sellers') }}      s
JOIN {{ source('silver', 'order_items') }}  oi USING (seller_id)
JOIN {{ source('silver', 'orders') }}       o  USING (order_id)
WHERE
    s.is_deleted  = false
    AND oi.is_deleted = false
    AND o.is_deleted  = false
GROUP BY 1, 2, 3
