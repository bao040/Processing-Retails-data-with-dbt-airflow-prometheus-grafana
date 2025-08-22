{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_pricing_history') }}
    where dbt_valid_to is null
),

valid_prices as (
    select
        history_id,
        product_id,
        price,
        effective_date
    from source
)

select * from valid_prices
