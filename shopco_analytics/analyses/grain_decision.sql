-- GRAIN DECISION: one row per order (not per item)
--
-- Option A: one row per order_item
--   Pro: maximum granularity
--   Con: SUM(revenue) over orders is wrong without DISTINCT or pre-agg
--
-- Option B: one row per order  <-- we choose this
--   Pro: revenue, status, customer_id all live at order level
--   Con: can't analyze per-item metrics directly from fact_orders
--
-- Item-level analysis uses int_orders_with_items (ephemeral)
-- or fct_product_performance (pre-aggregated by product + month)

SELECT
    order_id,           -- surrogate key, one row per order
    customer_id,
    order_date,
    total_amount        -- order-level revenue, no duplication
FROM {{ ref('stg_orders') }}
