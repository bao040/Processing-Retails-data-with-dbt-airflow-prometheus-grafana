{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_promotions') }}
),

flagged as (
    select
        promotion_id,
        name,
        start_date,
        end_date,

        case when promotion_id is null then 'Missing promotion_id' else null end as err_id,
        case when name is null then 'Missing name' else null end as err_name,
        case when start_date is null then 'Missing start_date' else null end as err_start,
        case when end_date is null then 'Missing end_date' else null end as err_end,
        case when start_date is not null and end_date is not null and start_date > end_date then 'start_date > end_date' else null end as err_range

    from source
),

invalid as (
    select
        promotion_id,
        name,
        start_date,
        end_date,
        array_to_string(
            array_remove(array[
                err_id,
                err_name,
                err_start,
                err_end,
                err_range
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_name is not null
        or err_start is not null
        or err_end is not null
        or err_range is not null
)

select * from invalid
