{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail',
    schema='marts'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_items_agg AS (
    SELECT order_id, COUNT(*) AS num_items, SUM(quantity) AS total_quantity, SUM(line_total) AS calculated_total
    FROM {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        WHERE order_id IN (SELECT order_id FROM orders)
    {% endif %}
    GROUP BY order_id
),

final AS (
    SELECT
        o.order_id, o.customer_id, o.order_date, o.order_month, o.order_quarter, o.order_year,
        o.order_status, o.total_amount, o.payment_method, o.is_delivered, o.is_cancelled, o.days_since_order,
        c.full_name AS customer_name, c.email AS customer_email, c.country AS customer_country, c.customer_tier,
        oi.num_items, oi.total_quantity, oi.calculated_total,
        o.total_amount / NULLIF(oi.num_items, 0) AS avg_item_value,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN order_items_agg oi ON o.order_id = oi.order_id
)

SELECT * FROM final
