{{ config(
    materialized='incremental',
    unique_key='brand_id',
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
source as (
    select
        brand_id,
        name,
        load_timestamp
    from {{ source('raw_retails', 'raw_brands') }}
    {% if is_incremental() %}
      where load_timestamp > (select max_ts from max_ts)
    {% endif %}
)

select
    brand_id,
    name,
    load_timestamp
from source
