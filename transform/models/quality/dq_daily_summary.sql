-- One row per UTC day: how complete and how clean that day's data is.
-- A full day at one snapshot every 15 minutes has 96 snapshots.

with snapshots as (

    select
        snapshot_at,
        count(*) as station_rows,
        sum(unreadable_value_count) as unreadable_values,
        sum(duplicates_removed) as duplicates_removed,
        -- Stations that say they are open but have not reported in a while.
        count(*) filter (
            where is_installed and is_renting
              and (last_reported_at is null
                   or last_reported_at < snapshot_at - to_minutes({{ var('silent_station_minutes') }}))
        ) as silent_in_service_stations
    from {{ ref('fct_station_status') }}
    group by snapshot_at

),

snapshots_with_gaps as (

    select
        *,
        cast(snapshot_at as date) as day,
        date_diff('minute', lag(snapshot_at) over (order by snapshot_at), snapshot_at) as minutes_since_previous
    from snapshots

),

status_days as (

    select
        day,
        count(*) as status_snapshots,
        round(count(*) / 96.0, 3) as share_of_expected_snapshots,
        max(minutes_since_previous) as longest_gap_minutes,
        sum(station_rows) as station_rows,
        sum(unreadable_values) as unreadable_values,
        sum(duplicates_removed) as duplicates_removed,
        max(silent_in_service_stations) as max_silent_in_service_stations
    from snapshots_with_gaps
    group by day

),

quarantine_days as (

    select cast(snapshot_at as date) as day, count(*) as quarantined_rows
    from {{ ref('quarantine_station_status') }}
    where snapshot_at is not null
    group by 1

),

weather_days as (

    select observed_date as day, count(*) as weather_readings
    from {{ ref('fct_weather') }}
    group by 1

),

run_days as (

    select
        cast(started_at as date) as day,
        count(distinct run_id) as collection_runs,
        count(*) filter (where status = 'failed') as failed_source_results
    from {{ ref('fct_collection_run') }}
    group by 1

),

days as (

    select day from status_days
    union select day from weather_days
    union select day from run_days

)

select
    days.day,
    coalesce(status_days.status_snapshots, 0) as status_snapshots,
    coalesce(status_days.share_of_expected_snapshots, 0) as share_of_expected_snapshots,
    status_days.longest_gap_minutes,
    coalesce(weather_days.weather_readings, 0) as weather_readings,
    coalesce(run_days.collection_runs, 0) as collection_runs,
    coalesce(run_days.failed_source_results, 0) as failed_source_results,
    coalesce(status_days.station_rows, 0) as station_rows,
    coalesce(quarantine_days.quarantined_rows, 0) as quarantined_rows,
    coalesce(status_days.unreadable_values, 0) as unreadable_values,
    coalesce(status_days.duplicates_removed, 0) as duplicates_removed,
    status_days.max_silent_in_service_stations
from days
left join status_days using (day)
left join quarantine_days using (day)
left join weather_days using (day)
left join run_days using (day)
