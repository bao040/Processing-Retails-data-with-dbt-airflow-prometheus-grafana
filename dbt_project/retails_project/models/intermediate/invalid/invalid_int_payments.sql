{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_payments') }}
),

flagged as (
    select
        payment_id,
        method,
        status,
        paid_at,

        case when payment_id is null then 'Missing payment_id' else null end as err_id,
        case when method is null then 'Missing method' else null end as err_method,
        case
            when status is null then 'Missing status'
            when status not in ('Pending', 'Completed') then 'Invalid status'
            else null
        end as err_status,
        case
            when paid_at is null then 'Missing paid_at'
            when paid_at > current_date then 'paid_at > current_date'
            else null
        end as err_paid_at

    from source
),

invalid as (
    select
        payment_id,
        method,
        status,
        paid_at,
        array_to_string(
            array_remove(array[
                err_id,
                err_method,
                err_status,
                err_paid_at
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_method is not null
        or err_status is not null
        or err_paid_at is not null
)

select * from invalid
