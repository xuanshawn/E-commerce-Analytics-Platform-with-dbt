{{ config(materialized='table', schema='marts') }}

WITH orders AS (
    SELECT customer_id, order_date, total_amount, order_status
    FROM {{ ref('fact_orders') }}
),

first_orders AS (
    SELECT customer_id, MIN(order_date) AS cohort_month
    FROM orders GROUP BY customer_id
),

cohort_data AS (
    SELECT
        DATE_TRUNC('month', fo.cohort_month) AS cohort_month,
        DATE_TRUNC('month', o.order_date) AS order_month,
        DATEDIFF('month', fo.cohort_month, o.order_date) AS months_since_first_order,
        COUNT(DISTINCT o.customer_id) AS customers,
        COUNT(*) AS orders,
        SUM(o.total_amount) AS revenue
    FROM orders o
    INNER JOIN first_orders fo ON o.customer_id = fo.customer_id
    GROUP BY 1, 2, 3
)

SELECT * FROM cohort_data
