{#
  GBFS feeds write yes/no fields as 1/0 in older versions and true/false in newer ones.
  Returns a boolean for either, and null for anything else.
#}
{% macro gbfs_boolean(expression) -%}
    case
        when lower({{ expression }}) in ('1', 'true') then true
        when lower({{ expression }}) in ('0', 'false') then false
    end
{%- endmacro %}
