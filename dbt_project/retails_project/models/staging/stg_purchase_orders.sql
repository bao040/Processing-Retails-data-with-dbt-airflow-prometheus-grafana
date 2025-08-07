{{ config(materialized='table') }}

WITH cte_purchase_orders AS (
    SELECT
        order_id,
        supplier_id,
        order_date::DATE AS order_date,
        status
    FROM  {{ source('raw_retails', 'raw_purchase_orders') }}
)
SELECT
    order_id,
    supplier_id,
    order_date::DATE AS order_date,
    status
FROM cte_purchase_orders
