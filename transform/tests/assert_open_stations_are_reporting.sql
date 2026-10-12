-- Warns when, in the newest snapshot, too many stations say they are open (installed and
-- renting) but have not reported for a long time. Closed stations are left out: many of them
-- have not reported for months, which is expected.
{{ config(severity = 'warn') }}

with newest as (

    select *
    from {{ ref('fct_station_status') }}
    where snapshot_at = (select max(snapshot_at) from {{ ref('fct_station_status') }})
      and is_installed
      and is_renting

)

select
    count(*) as open_stations,
    count(*) filter (
        where last_reported_at is null
           or last_reported_at < snapshot_at - to_minutes({{ var('silent_station_minutes') }})
    ) as silent_open_stations
from newest
having count(*) filter (
        where last_reported_at is null
           or last_reported_at < snapshot_at - to_minutes({{ var('silent_station_minutes') }})
    ) > {{ var('max_share_silent_in_service_stations') }} * count(*)
