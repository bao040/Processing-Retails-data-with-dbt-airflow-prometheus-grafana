{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_shipments') }}
),

valid_shipments as (
    select
        shipment_id,
        order_id,
        store_id,
        shipped_date,
        received_date
    from source
    where
        shipment_id is not null
        and order_id is not null
        and store_id is not null
        and shipped_date is not null
        and received_date is not null
        and shipped_date <= current_date
        and received_date <= shipped_date + interval '45 days'
)

select * from valid_shipments
