{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_categories') }}
),

valid_categories as (
    select
        category_id,
        name
    from source
    where
        category_id is not null
        and name is not null
)

select * from valid_categories
