{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (

    select * from {{ ref('snapshot_customers') }}

),

-- Bước đánh dấu các lỗi
flagged as (

    select
        customer_id,
        name,
        email,
        phone,
        loyalty_program_id,
        created_at,

        -- Tạo cột flag cho từng lỗi
        case when customer_id is null then 'Missing customer_id' else null end as err_customer_id,
        case when name is null then 'Missing name' else null end as err_name,
        case when email is not null and not (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') then 'Invalid email format' else null end as err_email,
        case when phone is null or char_length(phone) <> 10 then 'Invalid or missing phone' else null end as err_phone,
        case when loyalty_program_id is null then 'Missing loyalty_program_id' else null end as err_loyalty,
        case when created_at > current_date then 'created_at is in the future' else null end as err_created_at

    from source

),

invalid_customers as (

    select
        customer_id,
        name,
        email,
        phone,
        loyalty_program_id,
        created_at,

        -- Tổng hợp lỗi thành 1 chuỗi, phân tách bằng dấu ;
        array_to_string(
            array_remove(
                array[
                    err_customer_id,
                    err_name,
                    err_email,
                    err_phone,
                    err_loyalty,
                    err_created_at
                ],
                null
            ),
            '; '
        ) as error_reason

    from flagged
    where
        err_customer_id is not null
        or err_name is not null
        or err_email is not null
        or err_phone is not null
        or err_loyalty is not null
        or err_created_at is not null

)

select * from invalid_customers
