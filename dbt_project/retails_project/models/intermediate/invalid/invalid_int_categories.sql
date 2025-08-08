{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_categories') }}
),

flagged as (
    select
        category_id,
        name,
        case when category_id is null then 'Missing category_id' else null end as err_category_id,
        case when name is null then 'Missing name' else null end as err_name
    from source
),

invalid as (
    select
        category_id,
        name,
        array_to_string(
            array_remove(array[
                err_category_id,
                err_name
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_category_id is not null
        or err_name is not null
)

select * from invalid
