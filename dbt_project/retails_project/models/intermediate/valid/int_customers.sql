{{ config(
    materialized='view'
) }}

with source as (

    select * from {{ ref('snapshot_customers') }}
    where dbt_valid_to is null
),


valid_customers as (

    select
        customer_id,
        name,
        -- Coalesce email if null
        coalesce(email, 'cust' || customer_id || '@example.com') as email,
        phone,
        loyalty_program_id,
        created_at
    from source
    -- where created_at <= current_date
)

select * from valid_customers
