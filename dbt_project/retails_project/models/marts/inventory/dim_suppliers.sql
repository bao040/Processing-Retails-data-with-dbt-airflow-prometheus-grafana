{{ config(
    materialized='table'
) }}

select
    supplier_id,
    name as supplier_name,
    contact_info
from {{ ref('int_suppliers') }}
