{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_pricing_history') }}
),

flagged as (
    select
        history_id,
        product_id,
        price,
        effective_date,

        case when history_id is null then 'Missing history_id' else null end as err_id,
        case when product_id is null then 'Missing product_id' else null end as err_product,
        case when price is null then 'Missing price'
             when price < 0 then 'Negative price' else null end as err_price,
        case when effective_date is null then 'Missing effective_date'
             when effective_date > current_timestamp then 'Effective date in future' else null end as err_date

    from source
),

invalid as (
    select
        history_id,
        product_id,
        price,
        effective_date,
        array_to_string(
            array_remove(array[
                err_id,
                err_product,
                err_price,
                err_date
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_product is not null
        or err_price is not null
        or err_date is not null
)

select * from invalid
