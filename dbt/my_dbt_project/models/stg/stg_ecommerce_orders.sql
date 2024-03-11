{{ config(materialized='view') }}

SELECT
    order_id,
    CAST(order_date AS DATE) AS order_date,
    user_id,
    order_status,
    order_amount,
    CAST(last_updated AS TIMESTAMP) AS last_updated
FROM {{ref('src_ecommerce_orders') }}
