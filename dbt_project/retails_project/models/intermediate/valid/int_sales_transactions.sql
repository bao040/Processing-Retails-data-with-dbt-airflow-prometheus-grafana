{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_sales_transactions') }}
),

valid_sales_transactions as (
    select
        transaction_id,
        customer_id,
        store_id,
        employee_id,
        transaction_date,
        total_amount,
        payment_id
    from source
    where
        transaction_id is not null
        and customer_id is not null
        and store_id is not null
        and employee_id is not null
        and transaction_date is not null and transaction_date <= current_date
        and total_amount is not null and total_amount >= 0
        and payment_id is not null
)

select * from valid_sales_transactions
