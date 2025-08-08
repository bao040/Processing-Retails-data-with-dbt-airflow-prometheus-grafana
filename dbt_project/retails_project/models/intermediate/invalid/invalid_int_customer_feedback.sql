{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_customer_feedback') }}
),

-- Làm sạch dữ liệu
cleaned as (
    select
        feedback_id,
        customer_id,
        store_id,
        product_id,
        -- Chuyển "bad" thành 0
        case 
            when rating = 'bad' then 0
            else try_cast(rating as int) 
        end as rating,
        comments,
        feedback_date
    from source
),

flagged as (
    select *,
        case when feedback_id is null then 'Missing feedback_id' else null end as err_id,
        case when customer_id is null then 'Missing customer_id' else null end as err_cust,
        case when store_id is null then 'Missing store_id' else null end as err_store,
        case when product_id is null then 'Missing product_id' else null end as err_product,
        case when rating is null then 'Missing or invalid rating'
             when rating < 0 then 'rating < 0' else null end as err_rating,
        case when comments is null then 'Missing comments' else null end as err_comments,
        case when feedback_date is null then 'Missing feedback_date' else null end as err_date,
        case when feedback_date > current_date then 'feedback_date in future' else null end as err_future
    from cleaned
),

invalid as (
    select
        feedback_id,
        customer_id,
        store_id,
        product_id,
        rating,
        comments,
        feedback_date,
        array_to_string(
            array_remove(array[
                err_id,
                err_cust,
                err_store,
                err_product,
                err_rating,
                err_comments,
                err_date,
                err_future
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_cust is not null
        or err_store is not null
        or err_product is not null
        or err_rating is not null
        or err_comments is not null
        or err_date is not null
        or err_future is not null
)

select * from invalid
