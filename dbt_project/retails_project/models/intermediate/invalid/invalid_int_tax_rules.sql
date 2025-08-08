{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_tax_rules') }}
),

flagged as (
    select
        tax_id,
        product_id,
        tax_rate,
        region,

        case when tax_id is null then 'Missing tax_id' else null end as err_id,
        case when product_id is null then 'Missing product_id' else null end as err_product,
        case when tax_rate is null then 'Missing tax_rate'
             when tax_rate < 0 then 'Negative tax_rate' else null end as err_rate,
        case when region is null then 'Missing region'
             when region not in ('West', 'East', 'South', 'North') then 'Invalid region' else null end as err_region

    from source
),

invalid as (
    select
        tax_id,
        product_id,
        tax_rate,
        region,
        array_to_string(
            array_remove(array[
                err_id,
                err_product,
                err_rate,
                err_region
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_product is not null
        or err_rate is not null
        or err_region is not null
)

select * from invalid
