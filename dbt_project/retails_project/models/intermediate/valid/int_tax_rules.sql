{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_tax_rules') }}
),

cleaned as (
    select
        tax_id,
        product_id,
        nullif(tax_rate, 'none') as tax_rate,
        region
    from source
),

valid_rules as (
    select
        tax_id,
        product_id,
        tax_rate,
        region
    from cleaned
    where
        tax_id is not null
        and product_id is not null
        and tax_rate is not null and tax_rate::numeric >= 0
        and region in ('West', 'East', 'South', 'North')
)

select * from valid_rules
