{{ config(
    materialized='incremental',
    unique_key='promotion_id',
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
cte_promotions AS (
    SELECT
        promotion_id,
        name,
        start_date::DATE AS start_date,
        end_date::DATE AS end_date,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_promotions') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    promotion_id,
    name,
    start_date,
    end_date,
    load_timestamp
FROM cte_promotions


