{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_products') }}
),

valid_categories as (
    select category_id from {{ ref('int_categories') }}
),

valid_brands as (
    select brand_id from {{ ref('int_brands') }}
),

valid_suppliers as (
    select supplier_id from {{ ref('int_suppliers') }}
),

valid_products as (
    select
        s.product_id,
        s.name,
        s.category_id,
        s.brand_id,
        s.supplier_id,
        s.price,
        s.created_at,
        s.season
    from source s
    inner join valid_categories c on s.category_id = c.category_id
    inner join valid_brands b on s.brand_id = b.brand_id
    inner join valid_suppliers sp on s.supplier_id = sp.supplier_id
    where
        s.product_id is not null
        and s.name is not null
        and s.category_id is not null
        and s.brand_id is not null
        and s.supplier_id is not null
        and s.price is not null and s.price >= 0
        and s.created_at is not null and s.created_at <= current_timestamp
        and s.season in ('Spring', 'Summer', 'Autumn', 'Winter', 'All Year')
)

select * from valid_products
