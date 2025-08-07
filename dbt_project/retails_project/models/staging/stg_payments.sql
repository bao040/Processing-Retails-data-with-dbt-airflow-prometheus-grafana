{{ config(materialized='table') }}

WITH cte_payments AS (
    SELECT
        payment_id,
        method,
        status,
        paid_at::DATE AS paid_at
    FROM  {{ source('raw_retails', 'raw_payments') }}
)
SELECT
    payment_id,
    method,
    status,
    paid_at
FROM cte_payments