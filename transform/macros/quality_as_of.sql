{#
  The moment the data quality checks treat as "now", as a UTC timestamp. It is the real
  current time unless the quality_as_of variable sets a fixed time, which the tests do so
  that their results do not depend on when they run.
#}
{% macro quality_as_of() -%}
    {%- if var('quality_as_of', none) -%}
        timestamp '{{ var("quality_as_of") }}'
    {%- else -%}
        timezone('UTC', now())
    {%- endif -%}
{%- endmacro %}
