{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('snapshot_stores') }}
),

flagged as (

    select
        store_id,
        name,
        location,
        manager_id,

        -- Flag các lỗi
        case when store_id is null then 'Missing store_id' else null end as err_store_id,
        case when name is null then 'Missing name' else null end as err_name,
        case when location is null then 'Missing location' else null end as err_location,
        case when manager_id is null then 'Missing manager_id' else null end as err_manager_id

    from source

),

invalid as (

    select
        store_id,
        name,
        location,
        manager_id,

        -- Tổng hợp lỗi
        array_to_string(
            array_remove(
                array[
                    err_store_id,
                    err_name,
                    err_location,
                    err_manager_id
                ],
                null
            ),
            '; '
        ) as error_reason

    from flagged
    where
        err_store_id is not null
        or err_name is not null
        or err_location is not null
        or err_manager_id is not null

)

select * from invalid
