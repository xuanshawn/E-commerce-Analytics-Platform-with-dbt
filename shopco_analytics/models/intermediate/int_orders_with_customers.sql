{{ config(materialized='ephemeral') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

joined AS (
    SELECT
        o.order_id,
        o.order_date,
        o.order_status,
        o.total_amount,
        o.is_delivered,
        c.customer_id,
        c.email,
        c.full_name,
        c.country,
        c.customer_tier
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c.customer_id
)

SELECT * FROM joined
