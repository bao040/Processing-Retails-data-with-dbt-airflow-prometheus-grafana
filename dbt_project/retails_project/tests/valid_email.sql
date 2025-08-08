-- tests/valid_email.sql

SELECT *
FROM {{ ref(model.name) }}
WHERE NOT (
    {{ column_name }} ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
)
