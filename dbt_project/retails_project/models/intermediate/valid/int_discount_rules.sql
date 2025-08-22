{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_discount_rules') }}
),

valid_rules as (
    select
        rule_id,
        product_id,
        discount_type,
        value,
        valid_from,
        valid_to,
        
        -- Valid duration
        case
            when valid_from is not null and valid_to is not null
            then valid_to - valid_from
            else null
        end as duration,

        -- Discount status
        case
            when valid_from is not null and current_date < valid_from then 'upcoming'
            when valid_from is not null and valid_to is not null 
                 and current_date between valid_from and valid_to then 'active'
            when valid_to is not null and current_date > valid_to then 'expired'
            else null
        end as status

    from source
)

select * from valid_rules
