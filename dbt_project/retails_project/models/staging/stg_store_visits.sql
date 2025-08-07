{{ config(materialized='table') }}

WITH cte_store_visits AS (
    SELECT
        visit_id,
        customer_id,
        store_id,
        visit_date::DATE AS visit_date
    FROM  {{ source('raw_retails', 'raw_store_visits') }}
)
SELECT
    visit_id,
    customer_id,
    store_id,
    visit_date
FROM cte_store_visits
