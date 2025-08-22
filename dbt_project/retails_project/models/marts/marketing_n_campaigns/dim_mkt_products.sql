{{ config(
    materialized='table'
) }}

with products as (
    select
        product_id,
        product_name,
        category_id,
        brand_id,
        season,
        product_age_days
    from {{ ref('int_products') }}
)

select
    p.product_id,
    p.product_name,
    c.name as category_name,
    b.name as brand_name,
    p.season,
    p.product_age_days
from products p
join {{ ref('int_categories') }} c
    on p.category_id = c.category_id
join {{ ref('int_brands') }} b
    on p.brand_id = b.brand_id