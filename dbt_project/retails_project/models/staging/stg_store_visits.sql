{{ config(
    materialized='incremental',
    unique_key='visit_id',
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
cte_store_visits AS (
    SELECT
        visit_id,
        customer_id,
        store_id,
        visit_date::DATE AS visit_date,
        load_timestamp
    FROM  {{ source('raw_retails', 'raw_store_visits') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    visit_id,
    customer_id,
    store_id,
    visit_date,
    load_timestamp
FROM cte_store_visits
