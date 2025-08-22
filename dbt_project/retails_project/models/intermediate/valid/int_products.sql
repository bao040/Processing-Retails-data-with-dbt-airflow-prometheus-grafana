{{ config(
    materialized='view'
) }}

with source as (
    select * 
    from {{ ref('snapshot_products') }}
    where dbt_valid_to is null
),

valid_products as (
    select
        product_id,
        trim(name) as product_name,
        category_id,
        brand_id,
        supplier_id,
        price,
        created_at,
        trim(season) as season,

        case 
            when created_at is not null 
            then current_date - created_at::date
        end as product_age_days

    from source
)

select * 
from valid_products
