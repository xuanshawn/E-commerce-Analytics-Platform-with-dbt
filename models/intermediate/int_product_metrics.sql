{{ config(materialized='ephemeral') }}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

item_metrics AS (
    SELECT
        product_id,
        SUM(quantity) AS total_quantity_sold,
        SUM(line_total) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders,
        AVG(unit_price) AS avg_selling_price
    FROM order_items
    GROUP BY product_id
),

final AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.current_price,
        p.margin,
        p.margin_percent,
        p.price_tier,
        COALESCE(m.total_quantity_sold, 0) AS total_quantity_sold,
        COALESCE(m.total_revenue, 0) AS total_revenue,
        COALESCE(m.total_orders, 0) AS total_orders,
        COALESCE(m.avg_selling_price, p.current_price) AS avg_selling_price
    FROM products p
    LEFT JOIN item_metrics m ON p.product_id = m.product_id
)

SELECT * FROM final
