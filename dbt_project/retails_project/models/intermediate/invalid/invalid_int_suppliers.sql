{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_suppliers') }}
),

flagged as (
    select
        supplier_id,
        name,
        contact_info,
        case when supplier_id is null then 'Missing supplier_id' else null end as err_supplier_id,
        case when name is null then 'Missing name' else null end as err_name,
        case when contact_info is null then 'Missing contact_info' else null end as err_contact_info
    from source
),

invalid as (
    select
        supplier_id,
        name,
        contact_info,
        array_to_string(
            array_remove(array[
                err_supplier_id,
                err_name,
                err_contact_info
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_supplier_id is not null
        or err_name is not null
        or err_contact_info is not null
)

select * from invalid
