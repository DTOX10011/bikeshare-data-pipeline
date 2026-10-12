{#
  Path pattern for every raw snapshot of one source, for example
  data/raw/station_status/dt=2026-10-10/hour=17/station_status_20261010T172553Z.json.gz

  Calling source() records that the model reads this raw source, so it shows up in the
  lineage graph. The relation it returns is not used.
#}
{% macro raw_files(source_name) -%}
    {%- set _lineage = source('raw', source_name) -%}
    '{{ env_var("RAW_DIR", "data/raw") }}/{{ source_name }}/*/*/*.json.gz'
{%- endmacro %}

{# Path pattern for every day's run log written by the ingest job. #}
{% macro run_log_files() -%}
    {%- set _lineage = source('raw', 'run_log') -%}
    '{{ env_var("RUN_LOG_DIR", "data/run_log") }}/*/runs.jsonl'
{%- endmacro %}
