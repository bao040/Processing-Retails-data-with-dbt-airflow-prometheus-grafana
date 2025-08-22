{{ config(
    materialized='incremental',
    unique_key='movement_id',
    incremental_strategy='merge'
) }}
with max_ts as (
    {% if is_incremental() %}
        select max(load_timestamp) as max_ts
        from {{ this }}
    {% else %}
        select null::timestamp as max_ts
    {% endif %}
),
cte_stock_movements AS (
    SELECT
        movement_id,
        product_id,
        store_id,
        movement_type,
        quantity,
        movement_date::DATE AS movement_date,
        load_timestamp
    FROM  {{ source('raw_retails', 'raw_stock_movements') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    movement_id,
    product_id,
    store_id,
    movement_type,
    quantity,
    movement_date,
    load_timestamp
FROM cte_stock_movements
