{{ config(
    materialized='view'
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
        case 
            when discount between 0 and 1 then discount
            when discount between 1 and 100 then discount / 100.0
            else 0
        end as discount,
        coalesce(nullif(tax, -1), 0) as tax,
        round(
            (
                quantity * unit_price * 
                (1 - case 
                        when discount between 0 and 1 then discount
                        when discount between 1 and 100 then discount / 100.0
                        else 0
                    end
                ) * (1 + coalesce(nullif(tax, -1), 0))
            )::numeric,
            2
        ) as line_total
    from source
)

select * from valid_sales_items

