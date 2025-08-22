{{ config(
    materialized='incremental',
    unique_key='transaction_id',
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
cte_sales_transactions AS (
    SELECT
        transaction_id,
        customer_id,
        store_id,
        employee_id,
        transaction_date::DATE AS transaction_date,
        total_amount,
        payment_id,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_sales_transactions') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    transaction_id,
    customer_id,
    store_id,
    employee_id,
    transaction_date,
    total_amount,
    payment_id,
    load_timestamp
FROM cte_sales_transactions

