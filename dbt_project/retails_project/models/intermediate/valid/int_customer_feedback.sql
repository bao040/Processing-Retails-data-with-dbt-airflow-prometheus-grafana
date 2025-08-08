{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_customer_feedback') }}
),

-- Làm sạch dữ liệu trước khi lọc
cleaned as (
    select
        feedback_id,
        customer_id,
        store_id,
        product_id,
        -- Chuyển "bad" thành 0
        case 
            when rating = 'bad' then 0
            else rating::int 
        end as rating,
        comments,
        feedback_date
    from source
),

valid_feedback as (
    select *
    from cleaned
    where
        feedback_id is not null
        and customer_id is not null
        and store_id is not null
        and product_id is not null
        and rating >= 0
        and comments is not null
        and feedback_date is not null
        and feedback_date <= current_date
)

select * from valid_feedback
