{# Use the schema names from dbt_project.yml as they are (staging, marts), with no prefix. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
