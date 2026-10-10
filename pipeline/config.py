"""Settings for the pipeline. Environment variables override the defaults."""

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode


@dataclass(frozen=True)
class Source:
    """One thing the pipeline collects.

    format        which parser reads the snapshot's own timestamp ("gbfs" or "open_meteo")
    once_per_day  True for slow-changing reference data: one snapshot per UTC day is enough
    """

    name: str
    url: str
    format: str
    once_per_day: bool = False


# Citi Bike publishes its live data in the General Bikeshare Feed Specification (GBFS).
GBFS_BASE = "https://gbfs.citibikenyc.com/gbfs/en"

# Open-Meteo: free weather API, no key needed. One point (Union Square, Manhattan) stands in
# for the whole service area. Times are requested as Unix seconds in UTC.
WEATHER_LATITUDE = 40.7359
WEATHER_LONGITUDE = -73.9911
WEATHER_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_gusts_10m",
    "is_day",
]
WEATHER_URL = "https://api.open-meteo.com/v1/forecast?" + urlencode(
    {
        "latitude": WEATHER_LATITUDE,
        "longitude": WEATHER_LONGITUDE,
        "current": ",".join(WEATHER_VARIABLES),
        "timezone": "GMT",
        "timeformat": "unixtime",
    },
    safe=",",
)

SOURCES = {
    source.name: source
    for source in [
        # Bikes and docks available at every station. Refreshes about once a minute.
        Source("station_status", f"{GBFS_BASE}/station_status.json", "gbfs"),
        # Station names, locations and capacity. Changes rarely, so once a day is enough.
        Source("station_information", f"{GBFS_BASE}/station_information.json", "gbfs", once_per_day=True),
        # Current weather conditions. Refreshes every 15 minutes.
        Source("weather", WEATHER_URL, "open_meteo"),
    ]
}

RAW_DIR = Path(os.environ.get("RAW_DIR", "data/raw"))
REQUEST_TIMEOUT_SECONDS = 30
