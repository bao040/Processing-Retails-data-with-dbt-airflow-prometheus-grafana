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
)

select * from valid_brands
