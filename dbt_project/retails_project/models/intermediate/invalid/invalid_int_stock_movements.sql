{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_stock_movements') }}
),

flagged as (
    select
        movement_id,
        product_id,
        store_id,
        movement_type,
        quantity,
        movement_date,

        case when movement_id is null then 'Missing movement_id' else null end as err_id,
        case when product_id is null then 'Missing product_id' else null end as err_product,
        case when store_id is null then 'Missing store_id' else null end as err_store,
        case
            when movement_type is null then 'Missing movement_type'
            when movement_type not in ('IN', 'OUT', 'TRANSFER') then 'Invalid movement_type'
            else null
        end as err_type,
        case
            when quantity is null then 'Missing quantity'
            when quantity < 0 then 'quantity < 0'
            else null
        end as err_quantity,
        case
            when movement_date is null then 'Missing movement_date'
            when movement_date > current_date then 'movement_date > current_date'
            else null
        end as err_date

    from source
),

invalid as (
    select
        movement_id,
        product_id,
        store_id,
        movement_type,
        quantity,
        movement_date,
        array_to_string(
            array_remove(array[
                err_id,
                err_product,
                err_store,
                err_type,
                err_quantity,
                err_date
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_product is not null
        or err_store is not null
        or err_type is not null
        or err_quantity is not null
        or err_date is not null
)

select * from invalid
