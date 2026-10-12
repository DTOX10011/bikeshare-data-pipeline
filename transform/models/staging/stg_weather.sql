-- One row per weather reading. The API reports current conditions in 15-minute steps.

with readings as (

    select
        dt as partition_date,
        regexp_extract(filename, '[^/\\]+$') as source_file,
        "current" as reading
    from read_json(
        {{ raw_files('weather') }},
        columns = {'current': 'JSON'},
        hive_partitioning = true,
        filename = true
    )

),

typed as (

    select
        {{ unix_seconds_to_timestamp("reading ->> 'time'") }} as observed_at,
        partition_date,
        try_cast(reading ->> 'temperature_2m' as double) as temperature_c,
        try_cast(reading ->> 'apparent_temperature' as double) as apparent_temperature_c,
        try_cast(reading ->> 'relative_humidity_2m' as double) as relative_humidity_pct,
        try_cast(reading ->> 'precipitation' as double) as precipitation_mm,
        try_cast(reading ->> 'rain' as double) as rain_mm,
        try_cast(reading ->> 'snowfall' as double) as snowfall_cm,
        try_cast(reading ->> 'weather_code' as integer) as weather_code,
        try_cast(reading ->> 'cloud_cover' as double) as cloud_cover_pct,
        try_cast(reading ->> 'wind_speed_10m' as double) as wind_speed_kmh,
        try_cast(reading ->> 'wind_gusts_10m' as double) as wind_gusts_kmh,
        {{ gbfs_boolean("reading ->> 'is_day'") }} as is_day,
        source_file
    from readings

)

select *
from typed
where observed_at is not null
qualify row_number() over (partition by observed_at, partition_date order by source_file) = 1
