{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_purchase_orders') }}
),

flagged as (
    select
        order_id,
        supplier_id,
        order_date,

        case when order_id is null then 'Missing order_id' else null end as err_id,
        case when supplier_id is null then 'Missing supplier_id' else null end as err_supplier,
        case
            when order_date is null then 'Missing order_date'
            when order_date > current_date then 'order_date > current_date'
            else null
        end as err_date

    from source
),

invalid as (
    select
        order_id,
        supplier_id,
        order_date,
        array_to_string(
            array_remove(array[
                err_id,
                err_supplier,
                err_date
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_supplier is not null
        or err_date is not null
)

select * from invalid
