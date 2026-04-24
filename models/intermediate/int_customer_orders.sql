{{ config(materialized='ephemeral') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

aggregated AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN is_delivered = TRUE THEN 1 ELSE 0 END) AS delivered_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS most_recent_order_date
    FROM orders
    GROUP BY customer_id
)

SELECT * FROM aggregated
