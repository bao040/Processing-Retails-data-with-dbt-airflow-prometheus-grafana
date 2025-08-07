-- dbt_project/snapshots/snapshot_inventory.sql
{% snapshot snapshot_inventory %}

{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='inventory_id',
  updated_at='last_updated',
) }}

SELECT
  inventory_id,
  store_id,
  product_id,
  quantity,
  CAST(last_updated AS TIMESTAMP) AS last_updated
FROM {{ source('raw_retails', 'raw_inventory') }}

{% endsnapshot %}