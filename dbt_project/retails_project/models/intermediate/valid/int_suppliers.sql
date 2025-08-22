{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_suppliers') }}
    where dbt_valid_to is null
),

valid_suppliers as (
    select
        supplier_id,
        name,
        contact_info
    from source
)

select * from valid_suppliers
