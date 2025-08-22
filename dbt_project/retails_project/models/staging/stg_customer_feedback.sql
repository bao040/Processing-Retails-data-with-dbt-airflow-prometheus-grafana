{{ config(
    materialized='incremental',
    unique_key='feedback_id',
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
cte_customer_feedback AS (
    SELECT
        feedback_id,
        customer_id,
        store_id,
        product_id,
        rating,
        comments,
        feedback_date::DATE AS feedback_date,
        load_timestamp
    FROM {{ source('raw_retails', 'raw_customer_feedback') }}

    
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)
SELECT
    feedback_id,
    customer_id,
    store_id,
    product_id,
    rating,
    comments,
    feedback_date,
    load_timestamp
FROM cte_customer_feedback


