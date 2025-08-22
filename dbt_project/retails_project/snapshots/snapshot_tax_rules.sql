{% snapshot snapshot_tax_rules %}

{{ config(
  target_schema='snapshots',
  strategy='check',
  unique_key='tax_id',
  check_cols=['product_id', 'tax_rate', 'region']
) }}

WITH ranked_tax_rules AS (
    SELECT
        tax_id,
        product_id,
        tax_rate,
        region,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY tax_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ source('raw_retails', 'raw_tax_rules') }}
)

SELECT
    tax_id,
    product_id,
    tax_rate,
    region
FROM ranked_tax_rules
WHERE rn = 1

{% endsnapshot %}
