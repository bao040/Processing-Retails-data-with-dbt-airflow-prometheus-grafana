{{ config(
    materialized='incremental',
    unique_key='rule_id',
    incremental_strategy='merge'
) }}
with max_ts as (
    {% if is_incremental() %}
        select max(load_timestamp) as max_ts
        from {{ this }}
    {% else %}
        select null::timestamp as max_ts
    {% endif %}
),
cte_discount_rules AS (
    SELECT
        rule_id,
        product_id,
        discount_type,
        value,
        valid_from::DATE AS valid_from,
        valid_to::DATE AS valid_to,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_discount_rules') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    rule_id,
    product_id,
    discount_type,
    value,
    valid_from,
    valid_to,
    load_timestamp
FROM cte_discount_rules
