-- dbt_project/snapshots/snapshot_pricing_history.sql
{% snapshot snapshot_pricing_history %}

{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='history_id',
  updated_at='effective_date',
) }}

SELECT
  history_id,
  product_id,
  price,
  CAST(effective_date AS TIMESTAMP) AS effective_date
FROM {{ source('raw_retails', 'raw_pricing_history') }}

{% endsnapshot %}