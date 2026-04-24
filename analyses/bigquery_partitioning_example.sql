{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
        "field": "order_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=['customer_id', 'order_status']
) }}
