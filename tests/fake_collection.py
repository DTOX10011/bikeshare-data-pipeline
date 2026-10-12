"""A fake day of collection, produced by the real ingest code with canned responses.

Used by the data quality and orchestration tests.
"""

import json
from datetime import datetime, timedelta, timezone

from pipeline import config, ingest

STATUS = config.SOURCES["station_status"]
INFORMATION = config.SOURCES["station_information"]
WEATHER = config.SOURCES["weather"]

STATIONS = [f"S{n}" for n in range(1, 11)]
CAPACITY = 20
DAY = datetime(2026, 10, 5, tzinfo=timezone.utc)  # a Monday


def at(hour, minute=0):
    return DAY.replace(hour=hour, minute=minute)


def unix(moment):
    return int(moment.timestamp())


def healthy_row(station_id, moment):
    return {
        "station_id": station_id,
        "num_bikes_available": 8, "num_ebikes_available": 2, "num_bikes_disabled": 0,
        "num_docks_available": CAPACITY - 8, "num_docks_disabled": 0,
        "is_installed": 1, "is_renting": 1, "is_returning": 1,
        "last_reported": unix(moment - timedelta(minutes=3)),
    }


class FeedSession:
    """Stands in for the network: answers each source's URL with a prepared payload."""

    def __init__(self, payloads):
        self.payloads = payloads

    def get(self, url, timeout=None):
        content = self.payloads[url]

        class Response:
            def __init__(self):
                self.content = content

            def raise_for_status(self):
                pass

        return Response()


def collect(root, moment, rows=None, weather_payload=None):
    """One collection run at `moment`, done by the real ingest code with canned responses."""
    rows = rows if rows is not None else [healthy_row(s, moment) for s in STATIONS]
    stations_info = [
        {"station_id": s, "name": f"Station {s}", "lat": 40.74, "lon": -73.99, "capacity": CAPACITY}
        for s in STATIONS
    ]
    weather = {"utc_offset_seconds": 0, "current": {
        "time": unix(moment) - unix(moment) % 900, "interval": 900, "temperature_2m": 15.0,
        "precipitation": 0.0, "snowfall": 0.0, "wind_gusts_10m": 10.0,
    }}
    session = FeedSession({
        STATUS.url: json.dumps({"last_updated": unix(moment), "data": {"stations": rows}}).encode(),
        INFORMATION.url: json.dumps({"last_updated": unix(moment), "data": {"stations": stations_info}}).encode(),
        WEATHER.url: weather_payload if weather_payload is not None else json.dumps(weather).encode(),
    })
    sources = [config.SOURCES[name] for name in sorted(config.SOURCES)]
    ingest.run_once(session, sources, root / "raw", root / "run_log", now=moment + timedelta(seconds=20))


def every_15_minutes(start, end):
    moment = start
    while moment <= end:
        yield moment
        moment += timedelta(minutes=15)
