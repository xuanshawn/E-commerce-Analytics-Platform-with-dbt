{{ config(materialized='table', schema='marts') }}

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        DATEDIFF('day', MIN(order_date), MAX(order_date)) AS customer_lifetime_days
    FROM {{ ref('fact_orders') }}
    WHERE is_delivered = TRUE
    GROUP BY customer_id
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

final AS (
    SELECT
        c.customer_id, c.full_name, c.email, c.customer_tier, c.country,
        COALESCE(co.total_orders, 0) AS total_orders,
        COALESCE(co.total_revenue, 0) AS lifetime_value,
        COALESCE(co.avg_order_value, 0) AS avg_order_value,
        co.first_order_date, co.last_order_date,
        DATEDIFF('day', co.last_order_date, CURRENT_DATE) AS days_since_last_order,
        CASE
            WHEN co.customer_lifetime_days > 0
            THEN co.total_orders::FLOAT / (co.customer_lifetime_days / 30.0)
            ELSE 0
        END AS orders_per_month,
        CASE
            WHEN co.total_orders >= 2
            THEN DATEADD('day', ROUND(co.customer_lifetime_days / NULLIF(co.total_orders - 1, 0))::INT, co.last_order_date)
            ELSE NULL
        END AS predicted_next_order_date,
        CASE
            WHEN DATEDIFF('day', co.last_order_date, CURRENT_DATE) > 180 THEN 'high'
            WHEN DATEDIFF('day', co.last_order_date, CURRENT_DATE) > 90 THEN 'medium'
            WHEN DATEDIFF('day', co.last_order_date, CURRENT_DATE) > 30 THEN 'low'
            ELSE 'active'
        END AS churn_risk,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM customers c
    LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
)

SELECT * FROM final
