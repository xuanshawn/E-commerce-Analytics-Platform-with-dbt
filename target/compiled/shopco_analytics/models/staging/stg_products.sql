

WITH source AS (
    SELECT * FROM raw_data.shopco_production.raw_products
),

cleaned AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        LOWER(TRIM(category)) AS category,
        LOWER(TRIM(subcategory)) AS subcategory,
        TRIM(brand) AS brand,
        current_price,
        cost,
        current_price - cost AS margin,
        ROUND((current_price - cost) / NULLIF(current_price, 0) * 100, 2) AS margin_percent,
        CASE
            WHEN current_price < 20 THEN 'budget'
            WHEN current_price BETWEEN 20 AND 100 THEN 'mid_range'
            WHEN current_price BETWEEN 101 AND 500 THEN 'premium'
            ELSE 'luxury'
        END AS price_tier,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
    WHERE product_id IS NOT NULL
      AND current_price > 0
)

SELECT * FROM cleaned