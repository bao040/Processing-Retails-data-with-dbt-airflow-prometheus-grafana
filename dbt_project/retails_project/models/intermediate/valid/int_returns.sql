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
)

select * from valid_returns
