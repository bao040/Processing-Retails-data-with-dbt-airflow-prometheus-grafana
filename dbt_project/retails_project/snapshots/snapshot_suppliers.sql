-- dbt_project/snapshots/snapshot_suppliers.sql
{% snapshot snapshot_suppliers %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='supplier_id',
  check_cols=['name', 'contact_info'],
) }}

SELECT
  supplier_id,
  name,
  contact_info
FROM {{ source('raw_retails', 'raw_suppliers') }}

{% endsnapshot %}