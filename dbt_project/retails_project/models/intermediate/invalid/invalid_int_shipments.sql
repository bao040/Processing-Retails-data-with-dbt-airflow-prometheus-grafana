{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_shipments') }}
),

flagged as (
    select
        shipment_id,
        order_id,
        store_id,
        shipped_date,
        received_date,

        case when shipment_id is null then 'Missing shipment_id' else null end as err_id,
        case when order_id is null then 'Missing order_id' else null end as err_order,
        case when store_id is null then 'Missing store_id' else null end as err_store,
        case 
            when shipped_date is null then 'Missing shipped_date'
            when shipped_date > current_date then 'shipped_date > current_date'
            else null
        end as err_shipped,
        case
            when received_date is null then 'Missing received_date'
            when received_date > shipped_date + interval '45 days' then 'received_date too late'
            else null
        end as err_received

    from source
),

invalid as (
    select
        shipment_id,
        order_id,
        store_id,
        shipped_date,
        received_date,
        array_to_string(
            array_remove(array[
                err_id,
                err_order,
                err_store,
                err_shipped,
                err_received
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_order is not null
        or err_store is not null
        or err_shipped is not null
        or err_received is not null
)

select * from invalid
