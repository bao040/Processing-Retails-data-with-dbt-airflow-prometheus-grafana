{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_promotions') }}
),

valid_promotions as (
    select
        promotion_id,
        name,
        start_date,
        end_date,
        case
            when start_date is null and end_date is null then 'ONGOING_NO_DATES'
            when start_date > current_date then 'UPCOMING'
            when (start_date is null or start_date <= current_date)
                 and (end_date is null or end_date >= current_date) then 'ACTIVE'
            when end_date < current_date then 'EXPIRED'
            else 'UNKNOWN'
        end as promotion_status
    from source
)

select * from valid_promotions
