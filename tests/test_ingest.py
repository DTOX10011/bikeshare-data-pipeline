import gzip
import json
from datetime import datetime, timezone

import pytest
import requests

from pipeline import config, ingest

STATUS = config.SOURCES["station_status"]
INFORMATION = config.SOURCES["station_information"]
WEATHER = config.SOURCES["weather"]

NOW = datetime(2026, 10, 3, 21, 5, 0, tzinfo=timezone.utc)
FEED_TIME = int(datetime(2026, 10, 3, 21, 4, 5, tzinfo=timezone.utc).timestamp())


def feed_payload(last_updated=FEED_TIME, stations=None) -> bytes:
    """A small payload in the same shape as a real GBFS station feed."""
    if stations is None:
        stations = [
            {"station_id": "a1", "num_bikes_available": 7, "num_docks_available": 12, "is_renting": 1},
            {"station_id": "b2", "num_bikes_available": 0, "num_docks_available": 30, "is_renting": 1},
        ]
    return json.dumps({"last_updated": last_updated, "ttl": 60, "data": {"stations": stations}}).encode()


def weather_payload(time=FEED_TIME, **overrides) -> bytes:
    """A small payload in the same shape as an Open-Meteo current-conditions response."""
    doc = {
        "latitude": 40.74,
        "longitude": -73.99,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "current_units": {"time": "unixtime", "interval": "seconds", "temperature_2m": "°C"},
        "current": {"time": time, "interval": 900, "temperature_2m": 17.3, "precipitation": 0.0},
    }
    doc.update(overrides)
    return json.dumps(doc).encode()


class FakeResponse:
    def __init__(self, content=b"", error=None):
        self.content = content
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise self._error


class FakeSession:
    """Stands in for requests.Session so tests never touch the network."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        return self.responses.pop(0)


def stored_files(root):
    return sorted(root.rglob("*.json.gz"))


# --- Storing snapshots ---------------------------------------------------------------


def test_snapshot_lands_in_date_and_hour_partition(tmp_path):
    path, ts, written = ingest.land_snapshot(tmp_path, STATUS, feed_payload())

    assert written
    assert ts == datetime(2026, 10, 3, 21, 4, 5, tzinfo=timezone.utc)
    assert path == (
        tmp_path / "station_status" / "dt=2026-10-03" / "hour=21"
        / "station_status_20261003T210405Z.json.gz"
    )
    assert path.exists()


def test_raw_bytes_are_stored_unchanged(tmp_path):
    payload = feed_payload()
    path, _, _ = ingest.land_snapshot(tmp_path, STATUS, payload)
    assert gzip.decompress(path.read_bytes()) == payload


def test_same_snapshot_twice_writes_only_once(tmp_path):
    payload = feed_payload()
    first_path, _, first_written = ingest.land_snapshot(tmp_path, STATUS, payload)
    second_path, _, second_written = ingest.land_snapshot(tmp_path, STATUS, payload)

    assert first_written and not second_written
    assert first_path == second_path
    assert len(stored_files(tmp_path)) == 1


def test_newer_snapshot_creates_a_second_file(tmp_path):
    ingest.land_snapshot(tmp_path, STATUS, feed_payload(FEED_TIME))
    ingest.land_snapshot(tmp_path, STATUS, feed_payload(FEED_TIME + 60))
    assert len(stored_files(tmp_path)) == 2


def test_same_payload_always_produces_identical_file_bytes(tmp_path):
    """Stored files are byte-for-byte reproducible, so storing one twice changes nothing."""
    first, _, _ = ingest.land_snapshot(tmp_path / "a", STATUS, feed_payload())
    second, _, _ = ingest.land_snapshot(tmp_path / "b", STATUS, feed_payload())
    assert first.read_bytes() == second.read_bytes()


def test_no_temporary_files_are_left_behind(tmp_path):
    ingest.land_snapshot(tmp_path, STATUS, feed_payload())
    assert list(tmp_path.rglob("*.tmp")) == []


# --- Station feeds (GBFS) ------------------------------------------------------------


def test_gbfs_text_timestamps_are_understood():
    payload = json.dumps({"last_updated": "2026-10-03T21:04:05Z", "data": {"stations": []}}).encode()
    assert ingest.gbfs_snapshot_time(payload) == datetime(2026, 10, 3, 21, 4, 5, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "payload",
    [
        b"<html>Service unavailable</html>",
        b"[]",
        json.dumps({"last_updated": FEED_TIME, "data": {}}).encode(),
        json.dumps({"last_updated": FEED_TIME, "data": None}).encode(),
        json.dumps({"data": {"stations": []}}).encode(),
        json.dumps({"last_updated": "yesterday", "data": {"stations": []}}).encode(),
        json.dumps({"last_updated": True, "data": {"stations": []}}).encode(),
    ],
)
def test_invalid_station_payload_is_rejected_and_nothing_is_stored(tmp_path, payload):
    with pytest.raises(ValueError):
        ingest.land_snapshot(tmp_path, STATUS, payload)
    assert list(tmp_path.rglob("*")) == []


# --- Weather (Open-Meteo) ------------------------------------------------------------


def test_weather_url_asks_for_utc_unix_times_at_the_configured_point():
    assert WEATHER.url.startswith("https://api.open-meteo.com/v1/forecast?")
    assert f"latitude={config.WEATHER_LATITUDE}" in WEATHER.url
    assert f"longitude={config.WEATHER_LONGITUDE}" in WEATHER.url
    assert "timezone=GMT" in WEATHER.url
    assert "timeformat=unixtime" in WEATHER.url
    assert "current=temperature_2m," in WEATHER.url


def test_weather_snapshot_lands_under_its_observation_time(tmp_path):
    path, ts, written = ingest.land_snapshot(tmp_path, WEATHER, weather_payload())

    assert written
    assert ts == datetime(2026, 10, 3, 21, 4, 5, tzinfo=timezone.utc)
    assert path == tmp_path / "weather" / "dt=2026-10-03" / "hour=21" / "weather_20261003T210405Z.json.gz"


def test_weather_text_time_is_converted_to_utc_using_the_reported_offset():
    # 17:00 at four hours behind UTC is 21:00 UTC.
    payload = weather_payload(time="2026-10-03T17:00", utc_offset_seconds=-14400)
    assert ingest.open_meteo_snapshot_time(payload) == datetime(2026, 10, 3, 21, 0, tzinfo=timezone.utc)


def test_weather_text_time_without_an_offset_is_read_as_utc():
    payload = weather_payload(time="2026-10-03T21:00", utc_offset_seconds=None)
    assert ingest.open_meteo_snapshot_time(payload) == datetime(2026, 10, 3, 21, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"[]",
        json.dumps({"error": True, "reason": "Cannot initialize WeatherVariable"}).encode(),
        json.dumps({"latitude": 40.74}).encode(),
        json.dumps({"current": {"temperature_2m": 17.3}}).encode(),
        json.dumps({"current": {"time": "noon"}}).encode(),
    ],
)
def test_invalid_weather_payload_is_rejected_and_nothing_is_stored(tmp_path, payload):
    with pytest.raises(ValueError):
        ingest.land_snapshot(tmp_path, WEATHER, payload)
    assert list(tmp_path.rglob("*")) == []


def test_a_station_payload_is_not_accepted_as_weather(tmp_path):
    with pytest.raises(ValueError):
        ingest.land_snapshot(tmp_path, WEATHER, feed_payload())


# --- One source in one run -----------------------------------------------------------


def test_ingest_source_reports_written_then_duplicate(tmp_path):
    session = FakeSession([FakeResponse(feed_payload()), FakeResponse(feed_payload())])

    first = ingest.ingest_source(session, STATUS, tmp_path, NOW)
    second = ingest.ingest_source(session, STATUS, tmp_path, NOW)

    assert first.status == ingest.WRITTEN
    assert first.bytes == len(feed_payload())
    assert first.snapshot_time == "2026-10-03T21:04:05+00:00"
    assert first.path == "station_status/dt=2026-10-03/hour=21/station_status_20261003T210405Z.json.gz"
    assert second.status == ingest.SKIPPED_DUPLICATE
    assert len(stored_files(tmp_path)) == 1
    assert session.calls == [STATUS.url, STATUS.url]


def test_ingest_source_reports_failure_without_raising(tmp_path):
    session = FakeSession([FakeResponse(error=requests.HTTPError("503 Server Error"))])

    result = ingest.ingest_source(session, STATUS, tmp_path, NOW)

    assert result.status == ingest.FAILED
    assert "503" in result.error
    assert list(tmp_path.rglob("*")) == []


def test_once_per_day_source_is_not_downloaded_again_the_same_day(tmp_path):
    session = FakeSession([FakeResponse(feed_payload())])

    first = ingest.ingest_source(session, INFORMATION, tmp_path, NOW)
    second = ingest.ingest_source(session, INFORMATION, tmp_path, NOW.replace(hour=23, minute=50))

    assert first.status == ingest.WRITTEN
    assert second.status == ingest.SKIPPED_NOT_DUE
    assert len(session.calls) == 1  # the second run made no request at all


def test_once_per_day_source_is_collected_again_the_next_day(tmp_path):
    next_day = NOW.replace(day=4, hour=0, minute=5)
    session = FakeSession([FakeResponse(feed_payload()), FakeResponse(feed_payload(FEED_TIME + 3 * 3600))])

    ingest.ingest_source(session, INFORMATION, tmp_path, NOW)
    result = ingest.ingest_source(session, INFORMATION, tmp_path, next_day)

    assert result.status == ingest.WRITTEN
    assert len(stored_files(tmp_path)) == 2


def test_frequent_sources_are_collected_on_every_run(tmp_path):
    session = FakeSession([FakeResponse(feed_payload(FEED_TIME)), FakeResponse(feed_payload(FEED_TIME + 60))])

    ingest.ingest_source(session, STATUS, tmp_path, NOW)
    result = ingest.ingest_source(session, STATUS, tmp_path, NOW)

    assert result.status == ingest.WRITTEN
    assert len(stored_files(tmp_path)) == 2


# --- Whole runs and the run log ------------------------------------------------------


def read_run_log(run_log_dir):
    lines = (run_log_dir / "dt=2026-10-03" / "runs.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def test_run_once_collects_every_source_and_logs_the_run(tmp_path):
    raw, run_log = tmp_path / "raw", tmp_path / "run_log"
    session = FakeSession([
        FakeResponse(feed_payload()),
        FakeResponse(feed_payload()),
        FakeResponse(weather_payload()),
    ])

    results = ingest.run_once(session, [INFORMATION, STATUS, WEATHER], raw, run_log, now=NOW)

    assert [r.status for r in results] == [ingest.WRITTEN] * 3
    assert len(stored_files(raw)) == 3

    (record,) = read_run_log(run_log)
    assert record["run_id"] == "20261003T210500Z"
    assert record["started_at"] == "2026-10-03T21:05:00+00:00"
    assert [s["source"] for s in record["sources"]] == ["station_information", "station_status", "weather"]
    assert all(s["status"] == "written" and s["bytes"] > 0 for s in record["sources"])


def test_each_run_appends_one_line_to_the_run_log(tmp_path):
    raw, run_log = tmp_path / "raw", tmp_path / "run_log"
    session = FakeSession([FakeResponse(weather_payload()), FakeResponse(b"not json")])

    ingest.run_once(session, [WEATHER], raw, run_log, now=NOW)
    ingest.run_once(session, [WEATHER], raw, run_log, now=NOW.replace(minute=20))

    first, second = read_run_log(run_log)
    assert first["sources"][0]["status"] == "written"
    assert second["sources"][0]["status"] == "failed"
    assert second["sources"][0]["error"].startswith("payload is not valid JSON")
    assert second["sources"][0]["path"] is None


def test_a_failing_source_does_not_stop_the_others(tmp_path):
    raw = tmp_path / "raw"
    session = FakeSession([
        FakeResponse(error=requests.ConnectionError("network unreachable")),
        FakeResponse(weather_payload()),
    ])

    results = ingest.run_once(session, [STATUS, WEATHER], raw, tmp_path / "run_log", now=NOW)

    assert [r.status for r in results] == [ingest.FAILED, ingest.WRITTEN]
    assert len(stored_files(raw)) == 1


def test_run_forever_waits_between_runs_and_not_after_the_last():
    events = []

    ingest.run_forever(lambda: events.append("run"), 900, sleep=lambda s: events.append(f"sleep {s}"), max_runs=3)

    assert events == ["run", "sleep 900", "run", "sleep 900", "run"]


# --- Command line --------------------------------------------------------------------


def test_main_exit_code_reflects_failures(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    ok = [FakeResponse(feed_payload()), FakeResponse(feed_payload()), FakeResponse(weather_payload())]
    monkeypatch.setattr(ingest, "build_session", lambda: FakeSession(ok))
    assert ingest.main(["--raw-dir", str(raw)]) == 0

    monkeypatch.setattr(ingest, "build_session", lambda: FakeSession([FakeResponse(b"not json")]))
    assert ingest.main(["--raw-dir", str(raw), "--sources", "weather"]) == 1


def test_main_puts_the_run_log_next_to_the_raw_folder_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest, "build_session", lambda: FakeSession([FakeResponse(weather_payload())]))

    ingest.main(["--raw-dir", str(tmp_path / "raw"), "--sources", "weather"])

    assert len(list((tmp_path / "run_log").glob("dt=*/runs.jsonl"))) == 1


def test_main_rejects_a_non_positive_interval(tmp_path):
    with pytest.raises(SystemExit):
        ingest.main(["--raw-dir", str(tmp_path), "--every", "0"])
