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

flagged as (
    select
        s.product_id,
        s.name,
        s.category_id,
        s.brand_id,
        s.supplier_id,
        s.price,
        s.created_at,
        s.season,

        -- Lý do lỗi
        case when s.product_id is null then 'Missing product_id' else null end as err_product_id,
        case when s.name is null then 'Missing name' else null end as err_name,
        case when s.category_id is null then 'Missing category_id'
             when c.category_id is null then 'Invalid category_id' else null end as err_category_id,
        case when s.brand_id is null then 'Missing brand_id'
             when b.brand_id is null then 'Invalid brand_id' else null end as err_brand_id,
        case when s.supplier_id is null then 'Missing supplier_id'
             when sp.supplier_id is null then 'Invalid supplier_id' else null end as err_supplier_id,
        case when s.price is null then 'Missing price'
             when s.price < 0 then 'Negative price' else null end as err_price,
        case when s.created_at is null then 'Missing created_at'
             when s.created_at > current_timestamp then 'created_at in future' else null end as err_created_at,
        case when s.season is null then 'Missing season'
             when s.season not in ('Spring', 'Summer', 'Autumn', 'Winter', 'All Year') then 'Invalid season' else null end as err_season

    from source s
    left join valid_categories c on s.category_id = c.category_id
    left join valid_brands b on s.brand_id = b.brand_id
    left join valid_suppliers sp on s.supplier_id = sp.supplier_id
),

invalid as (
    select
        product_id,
        name,
        category_id,
        brand_id,
        supplier_id,
        price,
        created_at,
        season,
        array_to_string(
            array_remove(array[
                err_product_id,
                err_name,
                err_category_id,
                err_brand_id,
                err_supplier_id,
                err_price,
                err_created_at,
                err_season
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_product_id is not null
        or err_name is not null
        or err_category_id is not null
        or err_brand_id is not null
        or err_supplier_id is not null
        or err_price is not null
        or err_created_at is not null
        or err_season is not null
)

select * from invalid
