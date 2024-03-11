WITH raw_ecommerce_orders AS (
    SELECT
        *
    FROM
       `dbt-analytics-engineer-403622.dbt_stage.raw_ecommerce_orders_1`
)
SELECT
    order_id
,order_date
,user_id
,order_status
,order_amount
,last_updated
FROM
    raw_ecommerce_orders