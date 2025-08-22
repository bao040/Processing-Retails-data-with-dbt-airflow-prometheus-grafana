{{ config(
    materialized='view'
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
)

select * from valid_purchase_orders
