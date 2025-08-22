{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_employees') }}
    where dbt_valid_to is null
),

valid_employees as (
    select
        employee_id,
        name,
        role,
        store_id
    from source 
)

select * from valid_employees
