{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_suppliers') }}
),

valid_suppliers as (
    select
        brand_id,
        name,
        contact_info
    from source
    where
        brand_id is not null
        and name is not null
        and contact_info is not null
)

select * from valid_suppliers
