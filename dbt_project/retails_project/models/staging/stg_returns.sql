{{ config(
    materialized='incremental',
    unique_key='return_id',
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
cte_returns AS (
    SELECT
        return_id,
        item_id,
        reason,
        return_date::DATE AS return_date,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_returns') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    return_id,
    item_id,
    reason,
    return_date,
    load_timestamp
FROM cte_returns


