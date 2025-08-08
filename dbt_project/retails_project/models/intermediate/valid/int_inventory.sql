{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_inventory') }}
),

valid_inventory as (
    select
        inventory_id,
        store_id,
        product_id,
        quantity,
        last_updated
    from source
    where
        inventory_id is not null
        and store_id is not null
        and product_id is not null
        and quantity is not null and quantity >= 0
        and last_updated is not null and last_updated <= current_timestamp
)

select * from valid_inventory
