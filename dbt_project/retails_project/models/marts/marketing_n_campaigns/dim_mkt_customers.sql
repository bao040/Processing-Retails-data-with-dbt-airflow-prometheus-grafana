{{ config(
    materialized='table'
) }}

with customers as (
    select
        customer_id,
        name,
        email,
        loyalty_program_id,
        created_at as customer_since
    from {{ ref('int_customers') }}
)

select
    c.customer_id,
    c.name,
    c.email,
    c.loyalty_program_id,
    lp.name as loyalty_program_name,
    lp.tier as loyalty_tier,
    c.customer_since
from customers c
left join {{ ref('int_loyalty_programs') }} lp
    on c.loyalty_program_id = lp.loyalty_program_id