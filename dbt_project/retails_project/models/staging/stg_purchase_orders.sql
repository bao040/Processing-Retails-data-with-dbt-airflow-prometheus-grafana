{{ config(
    materialized='incremental',
    unique_key='order_id',
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
cte_purchase_orders AS (
    SELECT
        order_id,
        supplier_id,
        order_date::DATE AS order_date,
        status,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_purchase_orders') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    order_id,
    supplier_id,
    order_date,
    status,
    load_timestamp
FROM cte_purchase_orders


