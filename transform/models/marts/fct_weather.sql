-- Fact table. One row per weather reading (every 15 minutes) for the service area.
-- Loaded incrementally in the same way as fct_station_status.

{{
    config(
        materialized = 'incremental',
        unique_key = 'observed_at',
        incremental_strategy = 'delete+insert'
    )
}}

select
    observed_at,
    cast(observed_at as date) as observed_date,
    temperature_c,
    apparent_temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    rain_mm,
    snowfall_cm,
    weather_code,
    cloud_cover_pct,
    wind_speed_kmh,
    wind_gusts_kmh,
    is_day,
    source_file
from {{ ref('stg_weather') }}
{% if is_incremental() %}
where partition_date >= {{ incremental_start_date(this, 'observed_at') }}
{% endif %}
