{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_suppliers') }}
),

valid_suppliers as (
    select
        supplier_id,
        name,
        contact_info
    from source
    where
        supplier_id is not null
        and name is not null
        and contact_info is not null
)

select * from valid_suppliers
