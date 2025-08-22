{% snapshot snapshot_inventory %}

{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='inventory_id',
  updated_at='last_updated'
) }}

WITH ranked_inventory AS (
    SELECT
        inventory_id,
        store_id,
        product_id,
        quantity,
        CAST(last_updated AS TIMESTAMP) AS last_updated,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY inventory_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ source('raw_retails', 'raw_inventory') }}
)

SELECT
    inventory_id,
    store_id,
    product_id,
    quantity,
    last_updated
FROM ranked_inventory
WHERE rn = 1

{% endsnapshot %}
