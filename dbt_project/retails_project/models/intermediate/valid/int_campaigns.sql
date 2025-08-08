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
        end_date
    from source
    where
        campaign_id is not null
        and name is not null
        and budget is not null and budget > 0
        and start_date is not null
        and end_date is not null
        and start_date <= end_date
)

select * from valid_campaigns
