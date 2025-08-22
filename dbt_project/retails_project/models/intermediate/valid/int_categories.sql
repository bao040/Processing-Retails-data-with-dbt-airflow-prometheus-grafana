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
)

select * from valid_categories
