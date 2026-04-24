{{ config(
    materialized='incremental',
    unique_key='order_id',
    cluster_by=['order_date', 'customer_id']
) }}

-- Clustering physically organizes data for faster queries
-- on commonly filtered columns
