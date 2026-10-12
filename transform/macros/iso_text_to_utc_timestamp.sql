{# Text such as 2026-10-10T17:26:30+00:00 to a UTC timestamp; null if it cannot be read. #}
{% macro iso_text_to_utc_timestamp(expression) -%}
    timezone('UTC', try_cast({{ expression }} as timestamptz))
{%- endmacro %}
