{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_tier', 'email', 'country'],
    )
}}

SELECT
    customer_id, email, first_name, last_name, full_name,
    country, customer_tier, created_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
