{{ config(
    materialized='incremental',
    unique_key='payment_id',
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
cte_payments AS (
    SELECT
        payment_id,
        method,
        status,
        paid_at::DATE AS paid_at,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_payments') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    payment_id,
    method,
    status,
    paid_at,
    load_timestamp
FROM cte_payments

