{{ config(materialized='table', schema='marts') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_orders AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
),

final AS (
    SELECT
        c.customer_id, c.email, c.first_name, c.last_name,
        c.full_name, c.country, c.customer_tier,
        c.created_at AS customer_since,
        COALESCE(co.total_orders, 0) AS total_orders,
        COALESCE(co.delivered_orders, 0) AS delivered_orders,
        COALESCE(co.total_revenue, 0) AS lifetime_revenue,
        COALESCE(co.avg_order_value, 0) AS avg_order_value,
        co.first_order_date,
        co.most_recent_order_date,
        DATEDIFF('day', co.most_recent_order_date, CURRENT_DATE) AS days_since_last_order,
        CASE
            WHEN co.total_orders = 0 THEN 'never_purchased'
            WHEN co.total_orders = 1 THEN 'one_time'
            WHEN co.total_orders BETWEEN 2 AND 5 THEN 'repeat'
            ELSE 'loyal'
        END AS customer_segment,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM customers c
    LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
)

SELECT * FROM final
