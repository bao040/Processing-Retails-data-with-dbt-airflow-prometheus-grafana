{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_discount_rules') }}
),

flagged as (
    select
        rule_id,
        product_id,
        discount_type,
        value,
        valid_from,
        valid_to,

        case when rule_id is null then 'Missing rule_id' else null end as err_id,
        case when product_id is null then 'Missing product_id' else null end as err_product,
        case when discount_type is null then 'Missing discount_type'
             when discount_type not in ('percentage', 'fixed') then 'Invalid discount_type' else null end as err_type,
        case when value is null then 'Missing value'
             when value < 0 then 'Negative value' else null end as err_value,
        case when valid_from is null then 'Missing valid_from' else null end as err_from,
        case when valid_to is null then 'Missing valid_to'
             when valid_from > valid_to then 'valid_from > valid_to' else null end as err_to

    from source
),

invalid as (
    select
        rule_id,
        product_id,
        discount_type,
        value,
        valid_from,
        valid_to,
        array_to_string(
            array_remove(array[
                err_id,
                err_product,
                err_type,
                err_value,
                err_from,
                err_to
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_product is not null
        or err_type is not null
        or err_value is not null
        or err_from is not null
        or err_to is not null
)

select * from invalid
