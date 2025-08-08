{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_stores') }}
),

cleaned as (

    select
        store_id,
        name,
        location,
        manager_id

    from source
    where
        store_id is not null
        and name is not null
        and location is not null
        and manager_id is not null

)

select * from cleaned
