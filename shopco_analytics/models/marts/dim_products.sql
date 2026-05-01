{{ config(materialized='table', schema='marts') }}

WITH product_metrics AS (
    SELECT * FROM {{ ref('int_product_metrics') }}
),

final AS (
    SELECT
        product_id, product_name, category, subcategory, brand,
        current_price, margin, margin_percent, price_tier,
        total_quantity_sold, total_revenue, total_orders, avg_selling_price,
        CASE
            WHEN total_revenue = 0 THEN 'no_sales'
            WHEN total_revenue < 1000 THEN 'low_performer'
            WHEN total_revenue BETWEEN 1000 AND 10000 THEN 'medium_performer'
            ELSE 'top_performer'
        END AS performance_tier,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM product_metrics
)

SELECT * FROM final
