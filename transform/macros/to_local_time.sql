{# Convert a UTC timestamp to local wall-clock time in the service area. #}
{% macro to_local_time(expression) -%}
    timezone('{{ var("local_timezone") }}', timezone('UTC', {{ expression }}))
{%- endmacro %}
