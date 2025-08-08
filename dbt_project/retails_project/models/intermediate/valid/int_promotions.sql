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
        end_date
    from source
    where
        promotion_id is not null
        and name is not null
        and start_date is not null
        and end_date is not null
        and start_date <= end_date
)

select * from valid_promotions
