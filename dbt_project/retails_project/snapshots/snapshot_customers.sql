{% snapshot snapshot_customers %}

{{ config(
  target_schema='snapshots',
  strategy='timestamp',
  unique_key='customer_id',
  updated_at='created_at'
) }}

SELECT
  customer_id,
  name,
  email,
  phone,
  loyalty_program_id,

  CASE 
    WHEN gender ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$' THEN gender::timestamp
    WHEN gender ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\s(0[0-9]|1[0-9]|2[0-3]):([0-5]\d):([0-5]\d)$' THEN gender::timestamp
    ELSE NULL
  END AS created_at

FROM {{ source('raw_retails', 'raw_customers') }}
WHERE gender ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$' OR gender ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\s(0[0-9]|1[0-9]|2[0-3]):([0-5]\d):([0-5]\d)$'

{% endsnapshot %}
