{{ config(materialized='table') }}

WITH cte_sales_transactions AS (
    SELECT
        transaction_id,
        customer_id,
        store_id,
        employee_id,
        transaction_date::DATE AS transaction_date,
        total_amount,
        payment_id
    FROM  {{ source('raw_retails', 'raw_sales_transactions') }}
)
SELECT
    transaction_id,
    customer_id,
    store_id,
    employee_id,
    transaction_date,
    total_amount,
    payment_id
FROM cte_sales_transactions
