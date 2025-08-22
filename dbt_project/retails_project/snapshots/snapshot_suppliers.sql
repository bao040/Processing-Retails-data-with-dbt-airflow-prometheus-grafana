{% snapshot snapshot_suppliers %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='supplier_id',
  check_cols=['name', 'contact_info']
) }}

WITH ranked_suppliers AS (
    SELECT
        supplier_id,
        name,
        contact_info,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ source('raw_retails', 'raw_suppliers') }}
)

SELECT
    supplier_id,
    name,
    contact_info
FROM ranked_suppliers
WHERE rn = 1

{% endsnapshot %}
