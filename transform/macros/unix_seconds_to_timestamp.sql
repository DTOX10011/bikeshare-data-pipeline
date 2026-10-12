{#
  Unix seconds to a UTC timestamp. Values outside 2000-01-01 to 2100-01-01 become null:
  feeds sometimes report 0 or another placeholder for "never".
#}
{% macro unix_seconds_to_timestamp(expression) -%}
    case
        when try_cast({{ expression }} as bigint) between 946684800 and 4102444800
        then make_timestamp(try_cast({{ expression }} as bigint) * 1000000)
    end
{%- endmacro %}
