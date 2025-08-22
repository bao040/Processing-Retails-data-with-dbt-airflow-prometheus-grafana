{% snapshot snapshot_employees %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='employee_id',
  check_cols=['name', 'role', 'store_id']
) }}

WITH ranked_employees AS (
    SELECT
        employee_id,
        name,
        role,
        store_id,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY employee_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ source('raw_retails', 'raw_employees') }}
)

SELECT
    employee_id,
    name,
    role,
    store_id
FROM ranked_employees
WHERE rn = 1

{% endsnapshot %}
