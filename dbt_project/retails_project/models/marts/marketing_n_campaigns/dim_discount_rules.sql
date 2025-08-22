{{ config(
    materialized='table'
) }}

select
    rule_id,
    product_id,
    discount_type,
    value as discount_value,
    status as discount_status,
    valid_from,
    valid_to,
    duration
from {{ ref('int_discount_rules') }}