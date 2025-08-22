{{ config(
    materialized='incremental',
    unique_key='loyalty_program_id',
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
cte_loyalty_programs AS (
    SELECT
        loyalty_program_id,
        name,
        points_per_dollar,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_loyalty_programs') }}
    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    loyalty_program_id,
    name,
    points_per_dollar,
    load_timestamp
FROM cte_loyalty_programs

