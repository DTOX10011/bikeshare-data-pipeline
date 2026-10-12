{#
  The first date an incremental run should reload.

  Normally that is the newest day already in the table, minus the lookback window. A
  backfill sets the backfill_start_date variable instead, to reload a chosen range of days
  (see incremental_end_filter for the other end).

  The date is looked up first and written into the query as a plain date, because DuckDB
  only skips whole date folders when it is compared against a fixed value.
#}
{% macro incremental_start_date(relation, timestamp_column) %}
    {% if var('backfill_start_date', none) %}
        {{ return("date '" ~ var('backfill_start_date') ~ "'") }}
    {% endif %}
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

{#
  During a backfill, also stop at backfill_end_date (inclusive). Otherwise adds nothing,
  so a normal run loads everything from the start date onwards.
#}
{% macro incremental_end_filter(partition_column) %}
    {%- if var('backfill_end_date', none) -%}
        and {{ partition_column }} <= date '{{ var("backfill_end_date") }}'
    {%- endif -%}
{% endmacro %}
