-- tests/valid_email.sql

{% test valid_email(model, column_name) %}
    select *
    from {{ model }}
    where not (
        {{ column_name }} ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    )
{% endtest %}
