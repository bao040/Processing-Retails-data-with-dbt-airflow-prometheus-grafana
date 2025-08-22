{{ config(
    materialized='table'
) }}

select
    store_id,
    name as store_name,
    location
from {{ ref('int_stores') }}