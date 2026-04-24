
  create or replace   view ANALYTICS.DEV_xdai_staging.stg_order_items
  
   as (
    

WITH source AS (
    SELECT * FROM raw_data.shopco_production.raw_order_items
),

cleaned AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        CASE
            WHEN quantity = 1 THEN 'single'
            WHEN quantity BETWEEN 2 AND 5 THEN 'small_batch'
            WHEN quantity BETWEEN 6 AND 20 THEN 'medium_batch'
            ELSE 'bulk'
        END AS quantity_tier,
        unit_price,
        quantity * unit_price AS line_total,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
    WHERE order_item_id IS NOT NULL
      AND quantity > 0
      AND unit_price > 0
)

SELECT * FROM cleaned
  );

