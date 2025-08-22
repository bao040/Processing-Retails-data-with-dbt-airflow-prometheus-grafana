{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_stock_movements') }}
),

valid_stock_movements as (
    select
        movement_id,
        product_id,
        store_id,
        case 
            when upper(movement_type) in ('IN', 'OUT', 'TRANSFER') 
                then upper(movement_type)
            else null
        end as movement_type,

        case
            when quantity >= 0 then quantity
            else null
        end as quantity,
        
        case 
            when movement_date <= current_date then movement_date
            else null
        end as movement_date
    from source
)

select * from valid_stock_movements
