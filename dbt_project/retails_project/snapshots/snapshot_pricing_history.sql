{% snapshot snapshot_pricing_history %}

{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='history_id',
  updated_at='effective_date'
) }}

WITH ranked_pricing AS (
    SELECT
        history_id,
        product_id,
        price,
        CAST(effective_date AS TIMESTAMP) AS effective_date,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY history_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ source('raw_retails', 'raw_pricing_history') }}
)

SELECT
    history_id,
    product_id,
    price,
    effective_date
FROM ranked_pricing
WHERE rn = 1

{% endsnapshot %}
