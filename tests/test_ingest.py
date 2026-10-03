import gzip
import json
from datetime import datetime, timezone

import pytest
import requests

from pipeline import ingest


def feed_payload(last_updated=1791061445, stations=None) -> bytes:
    """A small payload in the same shape as a real station_status feed."""
    if stations is None:
        stations = [
            {"station_id": "a1", "num_bikes_available": 7, "num_docks_available": 12, "is_renting": 1},
            {"station_id": "b2", "num_bikes_available": 0, "num_docks_available": 30, "is_renting": 1},
        ]
    return json.dumps({"last_updated": last_updated, "ttl": 60, "data": {"stations": stations}}).encode()


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


def test_snapshot_lands_in_date_and_hour_partition(tmp_path):
    ts = datetime(2026, 10, 3, 21, 4, 5, tzinfo=timezone.utc)
    path, written = ingest.land_snapshot(tmp_path, "station_status", feed_payload(int(ts.timestamp())))

    assert written
    assert path == (
        tmp_path / "station_status" / "dt=2026-10-03" / "hour=21"
        / "station_status_20261003T210405Z.json.gz"
    )
    assert path.exists()


def test_raw_bytes_are_stored_unchanged(tmp_path):
    payload = feed_payload()
    path, _ = ingest.land_snapshot(tmp_path, "station_status", payload)
    assert gzip.decompress(path.read_bytes()) == payload


def test_same_snapshot_twice_writes_only_once(tmp_path):
    payload = feed_payload()
    first_path, first_written = ingest.land_snapshot(tmp_path, "station_status", payload)
    second_path, second_written = ingest.land_snapshot(tmp_path, "station_status", payload)

    assert first_written and not second_written
    assert first_path == second_path
    assert len(list(tmp_path.rglob("*.json.gz"))) == 1


def test_newer_snapshot_creates_a_second_file(tmp_path):
    ingest.land_snapshot(tmp_path, "station_status", feed_payload(1791061445))
    ingest.land_snapshot(tmp_path, "station_status", feed_payload(1791061505))
    assert len(list(tmp_path.rglob("*.json.gz"))) == 2


def test_text_timestamps_are_understood():
    payload = json.dumps({"last_updated": "2026-10-03T21:04:05Z", "data": {"stations": []}}).encode()
    assert ingest.snapshot_time(payload) == datetime(2026, 10, 3, 21, 4, 5, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "payload",
    [
        b"<html>Service unavailable</html>",
        b"[]",
        json.dumps({"last_updated": 1791061445, "data": {}}).encode(),
        json.dumps({"last_updated": 1791061445, "data": None}).encode(),
        json.dumps({"data": {"stations": []}}).encode(),
        json.dumps({"last_updated": "yesterday", "data": {"stations": []}}).encode(),
    ],
)
def test_invalid_payload_is_rejected_and_nothing_is_stored(tmp_path, payload):
    with pytest.raises(ValueError):
        ingest.land_snapshot(tmp_path, "station_status", payload)
    assert list(tmp_path.rglob("*")) == []


def test_no_temporary_files_are_left_behind(tmp_path):
    ingest.land_snapshot(tmp_path, "station_status", feed_payload())
    assert list(tmp_path.rglob("*.tmp")) == []


def test_ingest_feed_reports_success_and_duplicates(tmp_path):
    session = FakeSession([FakeResponse(feed_payload()), FakeResponse(feed_payload())])

    assert ingest.ingest_feed(session, "station_status", "https://example.test/feed", tmp_path)
    assert ingest.ingest_feed(session, "station_status", "https://example.test/feed", tmp_path)
    assert len(list(tmp_path.rglob("*.json.gz"))) == 1
    assert session.calls == ["https://example.test/feed"] * 2


def test_ingest_feed_reports_failure_without_raising(tmp_path):
    session = FakeSession([FakeResponse(error=requests.HTTPError("503 Server Error"))])

    assert not ingest.ingest_feed(session, "station_status", "https://example.test/feed", tmp_path)
    assert list(tmp_path.rglob("*")) == []


def test_main_exit_code_reflects_failures(tmp_path, monkeypatch):
    good = FakeSession([FakeResponse(feed_payload()), FakeResponse(feed_payload())])
    monkeypatch.setattr(ingest, "build_session", lambda: good)
    assert ingest.main(["--raw-dir", str(tmp_path)]) == 0

    bad = FakeSession([FakeResponse(feed_payload(1791061999)), FakeResponse(b"not json")])
    monkeypatch.setattr(ingest, "build_session", lambda: bad)
    assert ingest.main(["--raw-dir", str(tmp_path)]) == 1
