{{ config(
    materialized='incremental',
    unique_key='shipment_id',
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
cte_shipments AS (
    SELECT
        shipment_id,
        order_id,
        store_id,
        shipped_date::DATE AS shipped_date,
        received_date::DATE AS received_date,
        load_timestamp
    FROM  {{ source('raw_retails', 'raw_shipments') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    shipment_id,
    order_id,
    store_id,
    shipped_date,
    received_date,
    load_timestamp
FROM cte_shipments
