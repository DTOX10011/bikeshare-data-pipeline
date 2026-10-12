-- One row per station per snapshot: how many bikes and docks each station had at that moment.
-- Gives every field a type and removes duplicates. Rows with no station id or no snapshot
-- time are left out here and kept in stg_station_status_rejected instead.

with usable_rows as (

    select *
    from {{ ref('base_station_status_rows') }}
    where nullif(station ->> 'station_id', '') is not null
      and snapshot_at is not null

),

typed as (

    select
        station ->> 'station_id' as station_id,
        snapshot_at,
        partition_date,
        -- A value that cannot be read becomes null instead of failing the load.
        try_cast(station ->> 'num_bikes_available' as integer) as num_bikes_available,
        try_cast(station ->> 'num_ebikes_available' as integer) as num_ebikes_available,
        try_cast(station ->> 'num_bikes_disabled' as integer) as num_bikes_disabled,
        try_cast(station ->> 'num_docks_available' as integer) as num_docks_available,
        try_cast(station ->> 'num_docks_disabled' as integer) as num_docks_disabled,
        {{ gbfs_boolean("station ->> 'is_installed'") }} as is_installed,
        {{ gbfs_boolean("station ->> 'is_renting'") }} as is_renting,
        {{ gbfs_boolean("station ->> 'is_returning'") }} as is_returning,
        {{ unix_seconds_to_timestamp("station ->> 'last_reported'") }} as last_reported_at,
        source_file,
        station
    from usable_rows

),

checked as (

    select
        * exclude (station),
        -- How many fields held a value that could not be read (and so became null above).
        -- A missing field is not counted: the feed is allowed to leave fields out.
          {{ was_unreadable("station ->> 'num_bikes_available'", 'num_bikes_available') }}
        + {{ was_unreadable("station ->> 'num_ebikes_available'", 'num_ebikes_available') }}
        + {{ was_unreadable("station ->> 'num_bikes_disabled'", 'num_bikes_disabled') }}
        + {{ was_unreadable("station ->> 'num_docks_available'", 'num_docks_available') }}
        + {{ was_unreadable("station ->> 'num_docks_disabled'", 'num_docks_disabled') }}
        + {{ was_unreadable("station ->> 'is_installed'", 'is_installed') }}
        + {{ was_unreadable("station ->> 'is_renting'", 'is_renting') }}
        + {{ was_unreadable("station ->> 'is_returning'", 'is_returning') }}
        as unreadable_value_count,
        -- How many extra copies of this station were in the same snapshot.
        count(*) over (partition by station_id, snapshot_at, partition_date) - 1 as duplicates_removed
    from typed

)

select *
from checked
-- Keep one row if a station is listed twice in the same snapshot. partition_date is part
-- of the grouping only so that a date filter can skip whole folders of old files.
qualify row_number() over (
    partition by station_id, snapshot_at, partition_date
    order by last_reported_at desc nulls last
) = 1
