{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_employees') }}
),

valid_store_ids as (
    select store_id from {{ ref('int_stores') }}
),

flagged as (
    select
        s.employee_id,
        s.name,
        s.role,
        s.store_id,

        -- Gắn nhãn lỗi
        case when s.employee_id is null then 'Missing employee_id' else null end as err_employee_id,
        case when s.name is null then 'Missing name' else null end as err_name,
        case when s.role is null then 'Missing role' else null end as err_role,
        case when s.store_id is null then 'Missing store_id'
             when v.store_id is null then 'Invalid store_id' else null end as err_store_id

    from source s
    left join valid_store_ids v
        on s.store_id = v.store_id
),

invalid as (
    select
        employee_id,
        name,
        role,
        store_id,
        array_to_string(
            array_remove(array[
                err_employee_id,
                err_name,
                err_role,
                err_store_id
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_employee_id is not null
        or err_name is not null
        or err_role is not null
        or err_store_id is not null
)

select * from invalid
