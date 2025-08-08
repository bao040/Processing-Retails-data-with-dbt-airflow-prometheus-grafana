{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_returns') }}
),

valid_returns as (
    select
        return_id,
        item_id,
        reason
    from source
    where
        return_id is not null
        and item_id is not null
        and reason is not null
)

select * from valid_returns
