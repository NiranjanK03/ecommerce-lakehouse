{{
    config(
        materialized='materialized_view',
        refresh_async=True
    )
}}

-- Daily revenue by product category.
-- Drives the revenue trend chart and the text-to-SQL agent's most common query.
-- Only counts delivered orders — status='delivered' means the sale is confirmed.
SELECT
    DATE(o.purchased_at)              AS order_date,
    p.product_category                AS category,
    SUM(oi.price)                     AS total_revenue,
    SUM(oi.freight_value)             AS total_freight,
    COUNT(DISTINCT o.order_id)        AS order_count,
    COUNT(DISTINCT o.customer_id)     AS unique_customers
FROM {{ source('silver', 'orders') }}       o
JOIN {{ source('silver', 'order_items') }}  oi USING (order_id)
JOIN {{ source('silver', 'products') }}     p  USING (product_id)
WHERE
    o.status    = 'delivered'
    AND o.is_deleted  = false
    AND oi.is_deleted = false
    AND p.is_deleted  = false
GROUP BY 1, 2
