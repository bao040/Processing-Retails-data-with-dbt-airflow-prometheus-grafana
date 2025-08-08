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
        points_per_dollar
    from source
    where
        loyalty_program_id is not null
        and name is not null
        and points_per_dollar is not null
        and points_per_dollar > 0
)

select * from valid_loyalty_programs
