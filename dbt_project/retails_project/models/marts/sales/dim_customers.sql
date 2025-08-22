{{ config(
    materialized='table'
) }}

with customers as (
    select * from {{ ref('int_customers') }}
),

loyalty as (
    select * from {{ ref('int_loyalty_programs') }}
)

select
    c.customer_id,
    c.name as customer_name,
    c.email,
    c.phone,
    l.name as loyalty_program,
    l.tier as loyalty_tier,
    c.created_at
from customers c
left join loyalty l on c.loyalty_program_id = l.loyalty_program_id
