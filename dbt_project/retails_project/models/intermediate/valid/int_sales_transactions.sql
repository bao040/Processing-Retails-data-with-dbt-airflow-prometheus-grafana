{{ config(
    materialized='view'
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
        case when total_amount < 0 then null else total_amount end as total_amount,
        payment_id
    from source
)

select * from valid_sales_transactions
