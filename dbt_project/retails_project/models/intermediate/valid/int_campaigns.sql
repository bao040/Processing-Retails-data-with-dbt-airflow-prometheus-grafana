{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_campaigns') }}
),

valid_campaigns as (
    select
        campaign_id,
        name,
        budget,
        start_date,
        end_date,
        case
            when start_date is not null and end_date is not null
            then end_date - start_date
            else null
        end as duration
    from source
    where 
        start_date is null
        or end_date is null
        or start_date <= end_date
)

select * from valid_campaigns

