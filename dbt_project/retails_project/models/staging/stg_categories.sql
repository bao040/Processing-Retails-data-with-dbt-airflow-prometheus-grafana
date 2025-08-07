{{ config(materialized='table') }}

WITH cte_categories AS (
    SELECT
        category_id,
        name
    FROM  {{ source('raw_retails', 'raw_categories') }}
)
SELECT
    category_id,
    name
FROM cte_categories