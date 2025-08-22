{{ config(
    materialized='table'
) }}

with stores as (
    select * from {{ ref('int_stores') }}
),

employees as (
    select * from {{ ref('int_employees') }}
)

select
    s.store_id,
    s.name as store_name,
    s.location,
    m.name as manager_name
from stores s
left join employees m on s.manager_id = m.employee_id
