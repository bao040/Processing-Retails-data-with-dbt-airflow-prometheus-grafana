{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_loyalty_programs') }}
),

valid_loyalty_programs as (
    select
        loyalty_program_id,
        name,
        points_per_dollar,

        -- Tier classification (example logic)
        case
            when points_per_dollar >= 8 then 'Gold'
            when points_per_dollar >= 5 then 'Silver'
            else 'Bronze'
        end as tier

    from source
)

select * from valid_loyalty_programs
