{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_sales_items') }}
),

valid_sales_items as (
    select
        item_id,
        transaction_id,
        product_id,
        quantity,
        unit_price,
        discount,
        tax
    from source
    where
        item_id is not null
        and transaction_id is not null
        and product_id is not null
        and quantity is not null and quantity >= 0
        and unit_price is not null and unit_price >= 0
        and discount is not null and discount >= 0
        and tax is not null and tax >= 0
)

select * from valid_sales_items
