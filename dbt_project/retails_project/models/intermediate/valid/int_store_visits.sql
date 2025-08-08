{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_store_visits') }}
),

valid_visits as (
    select
        visit_id,
        customer_id,
        store_id,
        visit_date
    from source
    where
        visit_id is not null
        and customer_id is not null
        and store_id is not null
        and visit_date is not null
        and visit_date <= current_date
)

select * from valid_visits
