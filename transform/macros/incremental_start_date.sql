{#
  The first date an incremental run should reload: the newest day already in the table,
  minus the lookback window.

  It is looked up first and written into the query as a plain date, because DuckDB only
  skips whole date folders when it is compared against a fixed value.
#}
{% macro incremental_start_date(relation, timestamp_column) %}
    {% if not execute %}
        {{ return("date '1900-01-01'") }}
    {% endif %}
    {% set query %}
        select cast(max({{ timestamp_column }}) as date) - {{ var('lookback_days') }}
        from {{ relation }}
    {% endset %}
    {% set start_date = run_query(query).columns[0].values()[0] %}
    {{ return("date '" ~ start_date ~ "'" if start_date else "date '1900-01-01'") }}
{% endmacro %}
