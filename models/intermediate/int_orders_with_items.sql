{{ config(materialized='ephemeral') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

joined AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.is_delivered,
        o.is_cancelled,
        oi.order_item_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
)

SELECT * FROM joined
