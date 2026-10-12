-- Fact table. One row per station per snapshot.
--
-- Loaded incrementally: each run reloads only the most recent days of raw files and
-- replaces any rows it already had for them, so running it twice changes nothing.

{{
    config(
        materialized = 'incremental',
        unique_key = 'station_snapshot_key',
        incremental_strategy = 'delete+insert'
    )
}}

select
    md5(station_id || '|' || strftime(snapshot_at, '%Y-%m-%dT%H:%M:%S')) as station_snapshot_key,
    station_id,
    snapshot_at,
    cast(snapshot_at as date) as snapshot_date,
    num_bikes_available,
    num_ebikes_available,
    -- The feed's bike count includes e-bikes.
    num_bikes_available - num_ebikes_available as num_classic_bikes_available,
    num_bikes_disabled,
    num_docks_available,
    num_docks_disabled,
    is_installed,
    is_renting,
    is_returning,
    -- Empty and full only count while the station is in service: a station that is switched
    -- off has no bikes to rent, but that is an outage, not a shortage.
    coalesce(is_installed and is_renting and num_bikes_available = 0, false) as is_empty,
    coalesce(is_installed and is_returning and num_docks_available = 0, false) as is_full,
    last_reported_at,
    -- Data quality: fields whose value could not be read, and extra copies of this row
    -- that were removed from the same snapshot.
    unreadable_value_count,
    duplicates_removed,
    source_file
from {{ ref('stg_station_status') }}
{% if is_incremental() %}
where partition_date >= {{ incremental_start_date(this, 'snapshot_at') }}
  {{ incremental_end_filter('partition_date') }}
{% endif %}
