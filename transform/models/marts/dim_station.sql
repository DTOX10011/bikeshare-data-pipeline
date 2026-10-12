-- Dimension table. One row per station, with its most recent name, location and capacity.
--
-- Reference data is collected once a day, so a new station can report status before its
-- details arrive. Those stations still get a row here, flagged is_inferred, so every fact
-- row has a station to join to. The details fill in on a later run.

with information as (

    select * from {{ ref('stg_station_information') }}

),

latest_per_station as (

    select
        *,
        min(snapshot_at) over (partition by station_id) as first_seen_at,
        max(snapshot_at) over (partition by station_id) as last_seen_at
    from information
    qualify row_number() over (partition by station_id order by snapshot_at desc) = 1

),

described as (

    select
        station_id,
        station_name,
        short_name,
        latitude,
        longitude,
        capacity,
        region_id,
        station_type,
        first_seen_at,
        last_seen_at,
        -- False once a station drops out of the reference data, for example when it is removed.
        last_seen_at = (select max(snapshot_at) from information) as is_in_latest_reference_data,
        false as is_inferred
    from latest_per_station

),

inferred as (

    select distinct station_id
    from {{ ref('fct_station_status') }}
    where station_id not in (select station_id from described)

)

select * from described

union all

select
    station_id,
    null as station_name,
    null as short_name,
    null as latitude,
    null as longitude,
    null as capacity,
    null as region_id,
    null as station_type,
    null as first_seen_at,
    null as last_seen_at,
    false as is_in_latest_reference_data,
    true as is_inferred
from inferred
