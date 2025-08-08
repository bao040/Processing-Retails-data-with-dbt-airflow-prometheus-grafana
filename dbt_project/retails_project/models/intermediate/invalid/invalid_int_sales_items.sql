{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_sales_items') }}
),

flagged as (
    select
        item_id,
        transaction_id,
        product_id,
        quantity,
        unit_price,
        discount,
        tax,

        -- Lỗi từng trường
        case when item_id is null then 'Missing item_id' else null end as err_item_id,
        case when transaction_id is null then 'Missing transaction_id' else null end as err_txn_id,
        case when product_id is null then 'Missing product_id' else null end as err_product_id,
        case
            when quantity is null then 'Missing quantity'
            when quantity < 0 then 'quantity < 0'
            else null
        end as err_quantity,
        case
            when unit_price is null then 'Missing unit_price'
            when unit_price < 0 then 'unit_price < 0'
            else null
        end as err_price,
        case
            when discount is null then 'Missing discount'
            when discount < 0 then 'discount < 0'
            else null
        end as err_discount,
        case
            when tax is null then 'Missing tax'
            when tax < 0 then 'tax < 0'
            else null
        end as err_tax

    from source
),

invalid as (
    select
        item_id,
        transaction_id,
        product_id,
        quantity,
        unit_price,
        discount,
        tax,
        array_to_string(
            array_remove(array[
                err_item_id,
                err_txn_id,
                err_product_id,
                err_quantity,
                err_price,
                err_discount,
                err_tax
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_item_id is not null
        or err_txn_id is not null
        or err_product_id is not null
        or err_quantity is not null
        or err_price is not null
        or err_discount is not null
        or err_tax is not null
)

select * from invalid
