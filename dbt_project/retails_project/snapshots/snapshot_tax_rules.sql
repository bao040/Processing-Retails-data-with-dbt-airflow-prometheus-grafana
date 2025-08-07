-- dbt_project/snapshots/snapshot_tax_rules.sql
{% snapshot snapshot_tax_rules %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='tax_id',
  check_cols=['product_id', 'tax_rate', 'region'],
) }}

SELECT
  tax_id,
  product_id,
  tax_rate,
  region
FROM {{ source('raw_retails', 'raw_tax_rules') }}

{% endsnapshot %}