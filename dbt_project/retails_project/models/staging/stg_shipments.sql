{{ config(materialized='table') }}

WITH cte_shipments AS (
    SELECT
        shipment_id,
        order_id,
        store_id,
        shipped_date::DATE AS shipped_date,
        received_date::DATE AS received_date
    FROM  {{ source('raw_retails', 'raw_shipments') }}
)
SELECT
    shipment_id,
    order_id,
    store_id,
    shipped_date,
    received_date
FROM cte_shipments
