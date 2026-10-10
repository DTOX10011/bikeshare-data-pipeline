-- One row per station per snapshot: how many bikes and docks each station had at that moment.
-- Reads the raw station_status files, gives every field a type, and removes duplicates.

with snapshots as (

    select
        dt as partition_date,
        regexp_extract(filename, '[^/\\]+$') as source_file,
        {{ unix_seconds_to_timestamp('last_updated') }} as snapshot_at,
        unnest(data.stations) as station
    from read_json(
        {{ raw_files('station_status') }},
        columns = {'last_updated': 'BIGINT', 'data': 'STRUCT(stations JSON[])'},
        hive_partitioning = true,
        filename = true
    )

),

typed as (

    select
        station ->> 'station_id' as station_id,
        snapshot_at,
        partition_date,
        -- A value that is not a whole number becomes null instead of failing the load.
        try_cast(station ->> 'num_bikes_available' as integer) as num_bikes_available,
        try_cast(station ->> 'num_ebikes_available' as integer) as num_ebikes_available,
        try_cast(station ->> 'num_bikes_disabled' as integer) as num_bikes_disabled,
        try_cast(station ->> 'num_docks_available' as integer) as num_docks_available,
        try_cast(station ->> 'num_docks_disabled' as integer) as num_docks_disabled,
        {{ gbfs_boolean("station ->> 'is_installed'") }} as is_installed,
        {{ gbfs_boolean("station ->> 'is_renting'") }} as is_renting,
        {{ gbfs_boolean("station ->> 'is_returning'") }} as is_returning,
        {{ unix_seconds_to_timestamp("station ->> 'last_reported'") }} as last_reported_at,
        source_file
    from snapshots

)

select *
from typed
where station_id is not null
  and snapshot_at is not null
-- Keep one row if a station is listed twice in the same snapshot. partition_date is part
-- of the grouping only so that a date filter can skip whole folders of old files.
qualify row_number() over (
    partition by station_id, snapshot_at, partition_date
    order by last_reported_at desc nulls last
) = 1
