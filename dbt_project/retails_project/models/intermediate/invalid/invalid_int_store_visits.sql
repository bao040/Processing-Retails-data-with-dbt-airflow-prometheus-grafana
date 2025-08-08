{{ config(
    materialized='incremental',
    strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_store_visits') }}
),

flagged as (
    select
        visit_id,
        customer_id,
        store_id,
        visit_date,

        case when visit_id is null then 'Missing visit_id' else null end as err_id,
        case when customer_id is null then 'Missing customer_id' else null end as err_customer,
        case when store_id is null then 'Missing store_id' else null end as err_store,
        case when visit_date is null then 'Missing visit_date'
             when visit_date > current_date then 'visit_date in the future' else null end as err_date

    from source
),

invalid as (
    select
        visit_id,
        customer_id,
        store_id,
        visit_date,
        array_to_string(
            array_remove(array[
                err_id,
                err_customer,
                err_store,
                err_date
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_customer is not null
        or err_store is not null
        or err_date is not null
)

select * from invalid
