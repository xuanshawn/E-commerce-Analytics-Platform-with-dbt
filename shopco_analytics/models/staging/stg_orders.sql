{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'raw_orders') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        DATE_TRUNC('month', order_date) AS order_month,
        DATE_TRUNC('quarter', order_date) AS order_quarter,
        DATE_TRUNC('year', order_date) AS order_year,
        EXTRACT(YEAR FROM order_date) AS order_year_number,
        EXTRACT(MONTH FROM order_date) AS order_month_number,
        DATEDIFF('day', order_date, CURRENT_DATE) AS days_since_order,
        LOWER(TRIM(order_status)) AS order_status,
        total_amount,
        LOWER(TRIM(payment_method)) AS payment_method,
        CASE WHEN LOWER(order_status) = 'delivered' THEN TRUE ELSE FALSE END AS is_delivered,
        CASE WHEN LOWER(order_status) = 'cancelled' THEN TRUE ELSE FALSE END AS is_cancelled,
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM source
    WHERE order_id IS NOT NULL
      AND total_amount > 0
)

SELECT * FROM cleaned
