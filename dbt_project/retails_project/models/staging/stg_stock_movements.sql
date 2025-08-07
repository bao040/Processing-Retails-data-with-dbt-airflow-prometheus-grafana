{{ config(materialized='table') }}

WITH cte_stock_movements AS (
    SELECT
        movement_id,
        product_id,
        store_id,
        movement_type,
        quantity,
        movement_date::DATE AS movement_date
    FROM  {{ source('raw_retails', 'raw_stock_movements') }}
)
SELECT
    movement_id,
    product_id,
    store_id,
    movement_type,
    quantity,
    movement_date
FROM cte_stock_movements
