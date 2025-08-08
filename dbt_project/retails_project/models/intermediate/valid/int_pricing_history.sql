{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_pricing_history') }}
),

valid_prices as (
    select
        history_id,
        product_id,
        price,
        effective_date
    from source
    where
        history_id is not null
        and product_id is not null
        and price is not null
        and price >= 0
        and effective_date is not null
        and effective_date <= current_timestamp
)

select * from valid_prices
