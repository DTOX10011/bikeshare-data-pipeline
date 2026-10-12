-- Station rows that could not be used, with the reason. Loaded incrementally like the fact
-- table: a reloaded file replaces its earlier quarantined rows.
--
-- The reload window follows the fact table rather than this table, because this table is
-- usually empty and an empty table would otherwise mean "reload everything" on every run.
-- depends_on: {{ ref('fct_station_status') }}

{{
    config(
        materialized = 'incremental',
        unique_key = 'source_file',
        incremental_strategy = 'delete+insert'
    )
}}

select
    source_file,
    snapshot_at,
    partition_date,
    rejection_reason,
    raw_row
from {{ ref('stg_station_status_rejected') }}
{% if is_incremental() %}
where partition_date >= {{ incremental_start_date(ref('fct_station_status'), 'snapshot_at') }}
  {{ incremental_end_filter('partition_date') }}
{% endif %}
