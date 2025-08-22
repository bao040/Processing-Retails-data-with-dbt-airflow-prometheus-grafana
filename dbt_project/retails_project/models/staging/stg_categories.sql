{{ config(
    materialized='incremental',
    unique_key='category_id',
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
cte_categories AS (
    SELECT
        category_id,
        name,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_categories') }}

    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    category_id,
    name,
    load_timestamp
FROM cte_categories

