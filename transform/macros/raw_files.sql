{#
  Path pattern for every raw snapshot of one source, for example
  data/raw/station_status/dt=2026-10-10/hour=17/station_status_20261010T172553Z.json.gz
#}
{% macro raw_files(source_name) -%}
    '{{ env_var("RAW_DIR", "data/raw") }}/{{ source_name }}/*/*/*.json.gz'
{%- endmacro %}
