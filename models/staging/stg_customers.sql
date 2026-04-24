{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'raw_customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        LOWER(TRIM(email)) AS email,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        INITCAP(TRIM(first_name)) || ' ' || INITCAP(TRIM(last_name)) AS full_name,
        created_at,
        UPPER(TRIM(country)) AS country,
        LOWER(TRIM(customer_tier)) AS customer_tier,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned
