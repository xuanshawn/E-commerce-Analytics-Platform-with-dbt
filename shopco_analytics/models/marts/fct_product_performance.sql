{{ config(materialized='table', schema='marts') }}

WITH order_items AS (
    SELECT
        product_id,
        DATE_TRUNC('month', oi.order_date) AS order_month,
        SUM(oi.quantity) AS quantity_sold,
        SUM(oi.line_total) AS revenue,
        COUNT(DISTINCT oi.order_id) AS orders
    FROM {{ ref('int_orders_with_items') }} oi
    GROUP BY 1, 2
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

final AS (
    SELECT
        oi.order_month, oi.product_id,
        p.product_name, p.category, p.subcategory, p.brand,
        oi.quantity_sold, oi.revenue, oi.orders,
        oi.revenue / NULLIF(oi.quantity_sold, 0) AS avg_selling_price,
        p.margin_percent
    FROM order_items oi
    LEFT JOIN products p ON oi.product_id = p.product_id
)

SELECT * FROM final
