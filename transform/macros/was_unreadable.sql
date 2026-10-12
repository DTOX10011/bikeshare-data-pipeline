{#
  1 if the raw field held a value but the typed column came out null (the value could not
  be read), otherwise 0.
#}
{% macro was_unreadable(raw_expression, typed_column) -%}
    cast(({{ raw_expression }}) is not null and {{ typed_column }} is null as integer)
{%- endmacro %}
