{{ config(
    materialized='table'
) }}

with products as (
    select * from {{ ref('int_products') }}
),

categories as (
    select * from {{ ref('int_categories') }}
),

brands as (
    select * from {{ ref('int_brands') }}
),

suppliers as (
    select * from {{ ref('int_suppliers') }}
)

select
    p.product_id,
    p.product_name,
    c.name as category_name,
    b.name as brand_name,
    s.name as supplier_name,
    s.contact_info as supplier_contact,
    p.price,
    p.season,
    p.product_age_days
from products p
left join categories c on p.category_id = c.category_id
left join brands b on p.brand_id = b.brand_id
left join suppliers s on p.supplier_id = s.supplier_id
