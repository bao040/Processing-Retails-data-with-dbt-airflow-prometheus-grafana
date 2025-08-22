{% snapshot snapshot_products %}

{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='product_id',
  updated_at='created_at'
) }}

WITH ranked_products AS (
    SELECT
        product_id,
        name,
        category_id,
        brand_id,
        supplier_id,
        price,
        CAST(created_at AS TIMESTAMP) AS created_at,
        season,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ source('raw_retails', 'raw_products') }}
)

SELECT
    product_id,
    name,
    category_id,
    brand_id,
    supplier_id,
    price,
    created_at,
    season
FROM ranked_products
WHERE rn = 1

{% endsnapshot %}
