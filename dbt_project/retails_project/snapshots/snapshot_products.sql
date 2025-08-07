-- dbt_project/snapshots/snapshot_products.sql
{% snapshot snapshot_products %}

{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='product_id',
  updated_at='created_at',
) }}

SELECT
  product_id,
  name,
  category_id,
  brand_id,
  supplier_id,
  price,
  CAST(created_at AS TIMESTAMP) AS created_at,
  season
FROM {{ source('raw_retails', 'raw_products') }}

{% endsnapshot %}