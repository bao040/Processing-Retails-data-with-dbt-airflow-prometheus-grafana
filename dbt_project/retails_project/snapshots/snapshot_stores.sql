-- dbt_project/snapshots/snapshot_stores.sql
{% snapshot snapshot_stores %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='store_id',
  check_cols=['name', 'location', 'manager_id'],
) }}

SELECT
  store_id,
  name,
  location,
  manager_id
FROM {{ source('raw_retails', 'raw_stores') }}

{% endsnapshot %}