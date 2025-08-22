{{ config(
    materialized='view'
) }}

with source as (
    select * 
    from {{ ref('snapshot_inventory') }}
    where dbt_valid_to is null
),

valid_inventory as (
    select
        inventory_id,
        store_id,
        product_id,
        quantity::int as quantity,
        last_updated,

        case
            when quantity is null or quantity = 0 then 'out_of_stock'
            when quantity <= 10 then 'low_stock'
            else 'in_stock'
        end as status

    from source
)

select * from valid_inventory
