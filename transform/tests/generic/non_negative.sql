{# Rows where a count is below zero. Nulls are not counted: they are checked elsewhere. #}
{% test non_negative(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}
