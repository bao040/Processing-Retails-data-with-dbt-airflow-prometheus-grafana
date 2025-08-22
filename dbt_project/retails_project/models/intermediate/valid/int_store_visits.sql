{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_store_visits') }}
),

valid_visits as (
    select
        visit_id,
        customer_id,
        store_id,
        case 
            when visit_date <= current_date then visit_date
            else null
        end as visit_date
    from source
)

select * from valid_visits
