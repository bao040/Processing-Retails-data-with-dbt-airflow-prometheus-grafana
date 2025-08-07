{{ config(materialized='table') }}

WITH cte_campaigns AS (
    SELECT
        campaign_id,
        name,
        budget,
        start_date::DATE AS start_date,
        end_date::DATE AS end_date
    FROM  {{ source('raw_retails', 'raw_campaigns') }}
)
SELECT
    campaign_id,
    name,
    budget,
    start_date,
    end_date
FROM cte_campaigns
