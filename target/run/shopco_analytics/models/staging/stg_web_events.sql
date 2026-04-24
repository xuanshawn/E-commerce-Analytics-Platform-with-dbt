
  
    

        create or replace transient table ANALYTICS.DEV_xdai_staging.stg_web_events
         as
        (

WITH source AS (
    SELECT * FROM raw_data.shopco_production.raw_web_events
    
),

cleaned AS (
    SELECT
        event_id, session_id, customer_id,
        LOWER(TRIM(event_type)) AS event_type,
        event_timestamp,
        DATE_TRUNC('hour', event_timestamp) AS event_hour,
        DATE_TRUNC('day', event_timestamp) AS event_date,
        page_url,
        SPLIT_PART(page_url, '/', 3) AS page_domain,
        SPLIT_PART(page_url, '/', 4) AS page_section,
        product_id,
        CASE WHEN LOWER(event_type) = 'page_view' THEN 1 ELSE 0 END AS is_page_view,
        CASE WHEN LOWER(event_type) = 'product_view' THEN 1 ELSE 0 END AS is_product_view,
        CASE WHEN LOWER(event_type) = 'add_to_cart' THEN 1 ELSE 0 END AS is_add_to_cart,
        CASE WHEN LOWER(event_type) = 'checkout_start' THEN 1 ELSE 0 END AS is_checkout_start,
        CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'anonymous' ELSE 'logged_in' END AS user_type,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
    WHERE event_id IS NOT NULL AND event_timestamp IS NOT NULL
)

SELECT * FROM cleaned
        );
      
  