{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_stock_movements') }}
),

valid_stock_movements as (
    select
        movement_id,
        product_id,
        store_id,
        movement_type,
        quantity,
        movement_date
    from source
    where
        movement_id is not null
        and product_id is not null
        and store_id is not null
        and movement_type in ('IN', 'OUT', 'TRANSFER')
        and quantity is not null and quantity >= 0
        and movement_date is not null and movement_date <= current_date
)

select * from valid_stock_movements
