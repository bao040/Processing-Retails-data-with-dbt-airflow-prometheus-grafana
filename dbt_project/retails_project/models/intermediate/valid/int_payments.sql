{{ config(
    materialized='view'
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
)

select * from valid_payments
