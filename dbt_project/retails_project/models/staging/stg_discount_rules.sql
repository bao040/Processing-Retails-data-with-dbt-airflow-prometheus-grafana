{{ config(materialized='table') }}

WITH cte_discount_rules AS (
    SELECT
        rule_id,
        product_id,
        discount_type,
        value,
        valid_from::DATE AS valid_from,
        valid_to::DATE AS valid_to
    FROM  {{ source('raw_retails', 'raw_discount_rules') }}
)
SELECT
    rule_id,
    product_id,
    discount_type,
    value,
    valid_from,
    valid_to
FROM cte_discount_rules
