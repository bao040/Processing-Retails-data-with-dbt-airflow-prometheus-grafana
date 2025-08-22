{% snapshot snapshot_stores %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='store_id',
  check_cols=['name', 'location', 'manager_id']
) }}

WITH ranked_stores AS (
    SELECT
        store_id,
        name,
        location,
        manager_id,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ source('raw_retails', 'raw_stores') }}
)

SELECT
    store_id,
    name,
    location,
    manager_id
FROM ranked_stores
WHERE rn = 1

{% endsnapshot %}
