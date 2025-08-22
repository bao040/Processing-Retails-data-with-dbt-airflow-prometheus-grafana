{{ config(
    materialized='incremental',
    unique_key='item_id',
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
cte_sales_items AS (
    SELECT
        item_id,
        transaction_id,
        product_id,
        quantity,
        unit_price,
        discount,
        tax,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_sales_items') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    discount,
    tax,
    load_timestamp
FROM cte_sales_items


