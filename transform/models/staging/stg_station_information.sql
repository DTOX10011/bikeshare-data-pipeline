-- One row per station per daily snapshot of the reference data: name, location, capacity.

with snapshots as (

    select
        regexp_extract(filename, '[^/\\]+$') as source_file,
        {{ unix_seconds_to_timestamp('last_updated') }} as snapshot_at,
        unnest(data.stations) as station
    from read_json(
        {{ raw_files('station_information') }},
        columns = {'last_updated': 'BIGINT', 'data': 'STRUCT(stations JSON[])'},
        hive_partitioning = true,
        filename = true
    )

),

typed as (

    select
        station ->> 'station_id' as station_id,
        snapshot_at,
        nullif(trim(station ->> 'name'), '') as station_name,
        nullif(trim(station ->> 'short_name'), '') as short_name,
        try_cast(station ->> 'lat' as double) as latitude,
        try_cast(station ->> 'lon' as double) as longitude,
        try_cast(station ->> 'capacity' as integer) as capacity,
        station ->> 'region_id' as region_id,
        station ->> 'station_type' as station_type,
        source_file
    from snapshots

)

select *
from typed
where station_id is not null
  and snapshot_at is not null
qualify row_number() over (partition by station_id, snapshot_at order by station_name nulls last) = 1
