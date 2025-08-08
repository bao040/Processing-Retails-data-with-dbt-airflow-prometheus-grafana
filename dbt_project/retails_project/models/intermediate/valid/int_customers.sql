{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (

    select * from {{ ref('snapshot_customers') }}

),

-- Filter các bản ghi valid theo logic đã test trong schema.yml
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
    where
        customer_id is not null
        and name is not null
        and (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' or email is null)
        and phone is not null and char_length(phone) = 10
        and loyalty_program_id is not null
        and created_at <= current_date

)

select * from valid_customers
