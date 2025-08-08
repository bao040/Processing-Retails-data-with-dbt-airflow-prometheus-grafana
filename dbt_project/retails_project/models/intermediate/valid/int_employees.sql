{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_employees') }}
),

valid_store_ids as (
    select store_id from {{ ref('int_stores') }}
),

valid_employees as (
    select
        s.employee_id,
        s.name,
        s.role,
        s.store_id
    from source s
    inner join valid_store_ids v
        on s.store_id = v.store_id
    where
        s.employee_id is not null
        and s.name is not null
        and s.role is not null
        and s.store_id is not null
)

select * from valid_employees
