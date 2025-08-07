{{ config(materialized='table') }}

WITH cte_sales_items AS (
    SELECT
        item_id,
        transaction_id,
        product_id,
        quantity,
        unit_price,
        discount,
        tax
    FROM  {{ source('raw_retails', 'raw_sales_items') }}
)
SELECT
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    discount,
    tax
FROM cte_sales_items
