{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_tax_rules') }}
    where dbt_valid_to is null
),

cleaned as (
    select
        tax_id,
        product_id,
        nullif(tax_rate, 'none') as tax_rate,
        case 
            when region in ('West', 'East', 'South', 'North') 
                then region
            else null
        end as region
    from source
),

valid_rules as (
    select
        tax_id,
        product_id,
        tax_rate,
        region
    from cleaned
)

select * from valid_rules
