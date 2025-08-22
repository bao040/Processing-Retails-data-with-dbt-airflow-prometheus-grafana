{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_customer_feedback') }}
),

cleaned as (
    select
        feedback_id,
        customer_id,
        store_id,
        product_id,
        -- Convert "bad" into 0
        case 
            when rating = 'bad' then 0
            else rating::int 
        end as rating,
        trim(comments) as comments,
        feedback_date
    from source
    -- where feedback_date is null or feedback_date <= current_date  #add col to check feedback_date
)

select * from cleaned
