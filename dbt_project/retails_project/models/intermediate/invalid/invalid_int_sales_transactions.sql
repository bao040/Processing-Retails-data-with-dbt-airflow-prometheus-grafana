{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_sales_transactions') }}
),

flagged as (
    select
        transaction_id,
        customer_id,
        store_id,
        employee_id,
        transaction_date,
        total_amount,
        payment_id,

        -- Gắn nhãn lỗi
        case when transaction_id is null then 'Missing transaction_id' else null end as err_txn_id,
        case when customer_id is null then 'Missing customer_id' else null end as err_cust_id,
        case when store_id is null then 'Missing store_id' else null end as err_store_id,
        case when employee_id is null then 'Missing employee_id' else null end as err_emp_id,
        case
            when transaction_date is null then 'Missing transaction_date'
            when transaction_date > current_date then 'transaction_date is in the future'
            else null
        end as err_date,
        case
            when total_amount is null then 'Missing total_amount'
            when total_amount < 0 then 'total_amount must be >= 0'
            else null
        end as err_amount,
        case when payment_id is null then 'Missing payment_id' else null end as err_payment
    from source
),

invalid as (
    select
        transaction_id,
        customer_id,
        store_id,
        employee_id,
        transaction_date,
        total_amount,
        payment_id,
        array_to_string(
            array_remove(array[
                err_txn_id,
                err_cust_id,
                err_store_id,
                err_emp_id,
                err_date,
                err_amount,
                err_payment
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_txn_id is not null
        or err_cust_id is not null
        or err_store_id is not null
        or err_emp_id is not null
        or err_date is not null
        or err_amount is not null
        or err_payment is not null
)

select * from invalid
