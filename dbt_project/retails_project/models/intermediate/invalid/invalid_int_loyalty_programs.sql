{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_loyalty_programs') }}
),

flagged as (
    select
        loyalty_program_id,
        name,
        points_per_dollar,
        case when loyalty_program_id is null then 'Missing loyalty_program_id' else null end as err_id,
        case when name is null then 'Missing name' else null end as err_name,
        case when points_per_dollar is null then 'Missing points_per_dollar'
             when points_per_dollar <= 0 then 'points_per_dollar must be > 0'
             else null end as err_points
    from source
),

invalid as (
    select
        loyalty_program_id,
        name,
        points_per_dollar,
        array_to_string(
            array_remove(array[
                err_id,
                err_name,
                err_points
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_name is not null
        or err_points is not null
)

select * from invalid
