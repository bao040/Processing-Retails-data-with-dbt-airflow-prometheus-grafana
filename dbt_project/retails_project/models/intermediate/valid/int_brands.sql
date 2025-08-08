{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_brands') }}
),

valid_brands as (
    select
        brand_id,
        name
    from source
    where
        brand_id is not null
        and name is not null
)

select * from valid_brands
