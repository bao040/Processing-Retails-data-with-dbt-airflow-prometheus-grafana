{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_stores') }}
    where dbt_valid_to is null
),

cleaned as (

    select
        store_id,
        name,
        location,
        manager_id

    from source
)

select * from cleaned
