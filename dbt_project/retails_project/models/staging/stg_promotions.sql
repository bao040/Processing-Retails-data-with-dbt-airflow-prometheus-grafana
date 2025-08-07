{{ config(materialized='table') }}

WITH cte_promotions AS (
    SELECT
        promotion_id,
        name,
        start_date::DATE AS start_date,
        end_date::DATE AS end_date
    FROM  {{ source('raw_retails', 'raw_promotions') }}
)
SELECT
    promotion_id,
    name,
    start_date,
    end_date
FROM cte_promotions
