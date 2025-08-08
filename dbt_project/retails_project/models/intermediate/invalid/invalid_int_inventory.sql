{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_inventory') }}
),

flagged as (
    select
        inventory_id,
        store_id,
        product_id,
        quantity,
        last_updated,

        case when inventory_id is null then 'Missing inventory_id' else null end as err_id,
        case when store_id is null then 'Missing store_id' else null end as err_store,
        case when product_id is null then 'Missing product_id' else null end as err_product,
        case
            when quantity is null then 'Missing quantity'
            when quantity < 0 then 'quantity < 0'
            else null
        end as err_quantity,
        case
            when last_updated is null then 'Missing last_updated'
            when last_updated > current_timestamp then 'last_updated > current_timestamp'
            else null
        end as err_updated

    from source
),

invalid as (
    select
        inventory_id,
        store_id,
        product_id,
        quantity,
        last_updated,
        array_to_string(
            array_remove(array[
                err_id,
                err_store,
                err_product,
                err_quantity,
                err_updated
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_store is not null
        or err_product is not null
        or err_quantity is not null
        or err_updated is not null
)

select * from invalid
