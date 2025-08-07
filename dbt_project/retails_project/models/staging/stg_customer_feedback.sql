{{ config(materialized='table') }}

WITH cte_customer_feedback AS (
    SELECT
        feedback_id,
        customer_id,
        store_id,
        product_id,
        rating,
        comments,
        feedback_date::DATE AS feedback_date
    FROM  {{ source('raw_retails', 'raw_customer_feedback') }}
)
SELECT
    feedback_id,
    customer_id,
    store_id,
    product_id,
    rating,
    comments,
    feedback_date
FROM cte_customer_feedback
