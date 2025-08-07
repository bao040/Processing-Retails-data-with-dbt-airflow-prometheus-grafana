{{ config(materialized='table') }}

WITH cte_brands AS (
    SELECT
        brand_id,
        name
    FROM  {{ source('raw_retails', 'raw_brands') }}
)
SELECT
    brand_id,
    name
FROM cte_brands