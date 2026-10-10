-- Aggregate table. One row per station per hour, with the weather for that hour.
-- This is the table to chart from: which stations run empty, when, and in what weather.

with status as (

    select * from {{ ref('fct_station_status') }}

),

station_hours as (

    select
        station_id,
        date_trunc('hour', snapshot_at) as hour_utc,
        count(*) as snapshot_count,
        round(avg(num_bikes_available), 2) as avg_bikes_available,
        min(num_bikes_available) as min_bikes_available,
        max(num_bikes_available) as max_bikes_available,
        round(avg(num_ebikes_available), 2) as avg_ebikes_available,
        round(avg(num_docks_available), 2) as avg_docks_available,
        -- Share of this hour's snapshots in which the station was empty or full.
        round(avg(cast(is_empty as integer)), 4) as share_of_snapshots_empty,
        round(avg(cast(is_full as integer)), 4) as share_of_snapshots_full,
        round(avg(cast(coalesce(is_renting, false) as integer)), 4) as share_of_snapshots_renting
    from status
    group by station_id, date_trunc('hour', snapshot_at)

),

weather_hours as (

    select
        date_trunc('hour', observed_at) as hour_utc,
        count(*) as weather_reading_count,
        round(avg(temperature_c), 1) as avg_temperature_c,
        round(avg(apparent_temperature_c), 1) as avg_apparent_temperature_c,
        round(max(wind_gusts_kmh), 1) as max_wind_gusts_kmh,
        coalesce(bool_or(precipitation_mm > 0), false) as had_precipitation,
        coalesce(bool_or(snowfall_cm > 0), false) as had_snowfall
    from {{ ref('fct_weather') }}
    group by date_trunc('hour', observed_at)

)

select
    md5(station_hours.station_id || '|' || strftime(station_hours.hour_utc, '%Y-%m-%dT%H')) as station_hour_key,
    station_hours.station_id,
    station_hours.hour_utc,
    {{ to_local_time('station_hours.hour_utc') }} as hour_local,
    hour({{ to_local_time('station_hours.hour_utc') }}) as local_hour_of_day,
    isodow({{ to_local_time('station_hours.hour_utc') }}) as local_day_of_week,
    isodow({{ to_local_time('station_hours.hour_utc') }}) in (6, 7) as is_weekend,
    station_hours.snapshot_count,
    station_hours.avg_bikes_available,
    station_hours.min_bikes_available,
    station_hours.max_bikes_available,
    station_hours.avg_ebikes_available,
    station_hours.avg_docks_available,
    station_hours.share_of_snapshots_empty,
    station_hours.share_of_snapshots_full,
    station_hours.share_of_snapshots_renting,
    -- Weather columns are null for an hour with no weather readings.
    weather_hours.weather_reading_count,
    weather_hours.avg_temperature_c,
    weather_hours.avg_apparent_temperature_c,
    weather_hours.max_wind_gusts_kmh,
    weather_hours.had_precipitation,
    weather_hours.had_snowfall
from station_hours
left join weather_hours
    on station_hours.hour_utc = weather_hours.hour_utc
