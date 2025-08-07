{{ config(materialized='table') }}

WITH cte_loyalty_programs AS (
    SELECT
        loyalty_program_id,
        name,
        points_per_dollar
    FROM  {{ source('raw_retails', 'raw_loyalty_programs') }}
)
SELECT
    loyalty_program_id,
    name,
    points_per_dollar
FROM cte_loyalty_programs