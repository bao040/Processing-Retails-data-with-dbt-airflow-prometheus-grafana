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
        received_date,
        (received_date - shipped_date) as delivery_days
    from source
)

select * from valid_shipments

