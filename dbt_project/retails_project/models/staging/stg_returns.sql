{{ config(materialized='table') }}

WITH cte_returns AS (
    SELECT
        return_id,
        item_id,
        reason,
        return_date::DATE AS return_date
    FROM  {{ source('raw_retails', 'raw_returns') }}
)
SELECT
    return_id,
    item_id,
    reason,
    return_date
FROM cte_returns
