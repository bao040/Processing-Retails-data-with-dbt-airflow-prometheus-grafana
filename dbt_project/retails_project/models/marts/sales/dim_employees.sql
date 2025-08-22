{{ config(
    materialized='table'
) }}

select
    employee_id,
    name as employee_name,
    role,
    store_id
from {{ ref('int_employees') }}
