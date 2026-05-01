-- Check record counts
SELECT
    'stg_customers' AS model, COUNT(*) AS records FROM staging.stg_customers
UNION ALL
SELECT 'stg_orders', COUNT(*) FROM staging.stg_orders
UNION ALL
SELECT 'stg_order_items', COUNT(*) FROM staging.stg_order_items
UNION ALL
SELECT 'stg_products', COUNT(*) FROM staging.stg_products
UNION ALL
SELECT 'stg_web_events', COUNT(*) FROM staging.stg_web_events;

-- Sample cleaned customer data
SELECT * FROM staging.stg_customers LIMIT 10;

-- Check order status distribution
SELECT order_status, COUNT(*)
FROM staging.stg_orders
GROUP BY order_status;
