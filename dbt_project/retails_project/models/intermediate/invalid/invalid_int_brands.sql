{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_brands') }}
),

flagged as (
    select
        brand_id,
        name,
        case when brand_id is null then 'Missing brand_id' else null end as err_brand_id,
        case when name is null then 'Missing name' else null end as err_name
    from source
),

invalid as (
    select
        brand_id,
        name,
        array_to_string(
            array_remove(array[
                err_brand_id,
                err_name
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_brand_id is not null
        or err_name is not null
)

select * from invalid
