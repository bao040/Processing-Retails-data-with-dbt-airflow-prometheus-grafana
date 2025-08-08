{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_payments') }}
),

valid_payments as (
    select
        payment_id,
        method,
        status,
        paid_at
    from source
    where
        payment_id is not null
        and method is not null
        and status in ('Pending', 'Completed')
        and paid_at is not null
        and paid_at <= current_date
)

select * from valid_payments
