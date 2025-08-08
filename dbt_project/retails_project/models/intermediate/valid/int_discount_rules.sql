{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_discount_rules') }}
),

valid_rules as (
    select
        rule_id,
        product_id,
        discount_type,
        value,
        valid_from,
        valid_to
    from source
    where
        rule_id is not null
        and product_id is not null
        and discount_type in ('percentage', 'fixed')
        and value is not null and value >= 0
        and valid_from is not null
        and valid_to is not null
        and valid_from <= valid_to
)

select * from valid_rules
