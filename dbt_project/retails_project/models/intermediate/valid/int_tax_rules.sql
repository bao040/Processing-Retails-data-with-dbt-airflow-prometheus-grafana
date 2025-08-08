{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_tax_rules') }}
),

valid_rules as (
    select
        tax_id,
        product_id,
        tax_rate,
        region
    from source
    where
        tax_id is not null
        and product_id is not null
        and tax_rate is not null and tax_rate >= 0
        and region in ('West', 'East', 'South', 'North')
)

select * from valid_rules
