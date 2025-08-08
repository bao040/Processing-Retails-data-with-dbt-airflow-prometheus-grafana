{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ ref('stg_returns') }}
),

flagged as (
    select
        return_id,
        item_id,
        reason,

        case when return_id is null then 'Missing return_id' else null end as err_id,
        case when item_id is null then 'Missing item_id' else null end as err_item,
        case when reason is null then 'Missing reason' else null end as err_reason

    from source
),

invalid as (
    select
        return_id,
        item_id,
        reason,
        array_to_string(
            array_remove(array[
                err_id,
                err_item,
                err_reason
            ], null),
            '; '
        ) as error_reason
    from flagged
    where
        err_id is not null
        or err_item is not null
        or err_reason is not null
)

select * from invalid
