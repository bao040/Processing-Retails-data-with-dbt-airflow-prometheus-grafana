-- dbt_project/snapshots/snapshot_employees.sql
{% snapshot snapshot_employees %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='employee_id',
  check_cols=['name', 'role', 'store_id'],
) }}

SELECT
  employee_id,
  name,
  role,
  store_id
FROM {{ source('raw_retails', 'raw_employees') }}

{% endsnapshot %}