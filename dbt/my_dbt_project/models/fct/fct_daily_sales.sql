{{ config(materialized='incremental', unique_key='sale_date') }}

WITH sales_data AS (
  SELECT
    order_date AS sale_date,
    SUM(order_amount) AS total_sales
  FROM {{ ref('stg_ecommerce_orders') }}
  WHERE order_status NOT IN ('cancelled', 'returned') -- Exclude cancelled or returned orders
  GROUP BY order_date
)

SELECT * FROM sales_data

-- Incremental logic
{% if is_incremental() %}
  -- Add a condition to select only new or updated records
  WHERE sale_date > (SELECT MAX(sale_date) FROM {{ this }})
{% endif %}
