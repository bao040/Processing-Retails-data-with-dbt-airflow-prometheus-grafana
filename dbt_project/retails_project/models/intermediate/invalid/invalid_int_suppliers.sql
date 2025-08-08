{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_suppliers') }}
),

flagged as (
    select
        brand_id,
        name,
        contact_info,
        case when brand_id is null then 'Missing brand_id' else null end as err_brand_id,
        case when name is null then 'Missing name' else null end as err_name,
        case when contact_info is null then 'Missing contact_info' else null end as err_contact_info
    from source
),

invalid as (
    select
        brand_id,
        name,
        contact_info,
        array_to_string(
            array_remove(array[
                err_brand_id,
                err_name,
                err_contact_info
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_brand_id is not null
        or err_name is not null
        or err_contact_info is not null
)

select * from invalid
