{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_purchase_orders') }}
),

valid_purchase_orders as (
    select
        order_id,
        supplier_id,
        order_date
    from source
    where
        order_id is not null
        and supplier_id is not null
        and order_date is not null
        and order_date <= current_date
)

select * from valid_purchase_orders
