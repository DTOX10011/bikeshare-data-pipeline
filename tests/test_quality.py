"""Tests for the data quality checks.

A clean raw zone must pass every check. A messy one, with one of each problem the checks
look for, must raise exactly the expected warnings and still build. A pipeline bug must
fail the build.
"""

import json
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from pipeline import config, ingest, transform

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


def build(root, as_of):
    target = root / "target"
    succeeded = transform.run_dbt(
        root / "raw", root / "warehouse.duckdb", quiet=True,
        dbt_vars={"quality_as_of": as_of.strftime("%Y-%m-%d %H:%M:%S")}, target_path=target,
    )
    results = {r.name: r.status for r in transform.check_results(target)}
    return succeeded, results


def query(root, sql):
    with duckdb.connect(str(root / "warehouse.duckdb"), read_only=True) as con:
        return con.execute(sql).fetchall()


def warned(results):
    return {name for name, status in results.items() if status == "warn"}


# --- A clean day ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def clean(tmp_path_factory):
    root = tmp_path_factory.mktemp("clean")
    for moment in every_15_minutes(at(8), at(10)):
        collect(root, moment)
    return root, build(root, as_of=at(10, 5))


def test_clean_data_passes_every_check(clean):
    _, (succeeded, results) = clean
    assert succeeded
    assert len(results) > 30
    assert {status for status in results.values()} == {"pass"}


def test_daily_summary_describes_a_clean_day(clean):
    root, _ = clean
    assert query(root, """
        select status_snapshots, longest_gap_minutes, weather_readings, collection_runs,
               failed_source_results, quarantined_rows, unreadable_values, duplicates_removed
        from quality.dq_daily_summary
    """) == [(9, 15, 9, 9, 0, 0, 0, 0)]


def test_collection_runs_record_the_time_between_runs(clean):
    root, _ = clean
    assert query(root, """
        select count(*), min(minutes_since_previous_run), max(minutes_since_previous_run)
        from quality.fct_collection_run where source = 'station_status'
    """) == [(9, 15, 15)]


# --- A messy day ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def messy(tmp_path_factory):
    root = tmp_path_factory.mktemp("messy")
    # Runs every 15 minutes until 08:45, then nothing until 09:45: a 60-minute gap.
    moments = list(every_15_minutes(at(8), at(8, 45))) + [at(9, 45), at(10)]
    for moment in moments:
        rows = [healthy_row(s, moment) for s in STATIONS]
        # S1 always reports five more bikes and docks than it has room for.
        rows[0]["num_docks_available"] += 5
        if moment == at(8, 30):
            rows[1]["num_bikes_available"] = "n/a"       # a value that cannot be read
            rows[2]["num_ebikes_available"] = 9          # more e-bikes than bikes
            rows[3]["num_docks_available"] = -1          # a negative count
            rows.append({"num_bikes_available": 4})     # a row with no station id
        if moment == at(10):
            # S2 says it is open but has not reported for five hours.
            rows[1]["last_reported"] = unix(moment - timedelta(hours=5))
        weather = b"<html>Bad gateway</html>" if moment == at(8, 15) else None
        collect(root, moment, rows=rows, weather_payload=weather)
    # The checks run at 11:30, ninety minutes after the newest snapshot.
    return root, build(root, as_of=at(11, 30))


def test_messy_data_still_builds(messy):
    _, (succeeded, results) = messy
    assert succeeded
    assert "fail" not in results.values() and "error" not in results.values()


def test_messy_data_raises_one_warning_per_problem(messy):
    _, (_, results) = messy
    assert warned(results) == {
        "assert_station_status_is_fresh",
        "assert_no_long_gaps_between_snapshots",
        "assert_no_failed_collections",
        "assert_no_recent_quarantined_rows",
        "assert_unreadable_values_are_rare",
        "assert_ebikes_within_total_bikes",
        "assert_capacity_is_mostly_respected",
        "assert_open_stations_are_reporting",
        "non_negative_fct_station_status_num_docks_available",
    }


def test_row_without_station_id_is_quarantined_with_its_reason(messy):
    root, _ = messy
    assert query(root, """
        select source_file, rejection_reason, raw_row from quality.quarantine_station_status
    """) == [("station_status_20261005T083000Z.json.gz", "row has no station_id", '{"num_bikes_available":4}')]


def test_unreadable_value_is_counted_on_its_row(messy):
    root, _ = messy
    assert query(root, """
        select station_id, num_bikes_available, unreadable_value_count
        from marts.fct_station_status where unreadable_value_count > 0
    """) == [("S2", None, 1)]


def test_daily_summary_shows_the_problems(messy):
    root, _ = messy
    assert query(root, """
        select status_snapshots, longest_gap_minutes, weather_readings, collection_runs,
               failed_source_results, quarantined_rows, unreadable_values, max_silent_in_service_stations
        from quality.dq_daily_summary
    """) == [(6, 60, 5, 6, 1, 1, 1, 1)]


def test_failed_collection_records_the_error(messy):
    root, _ = messy
    ((source, error),) = query(root, "select source, error from quality.fct_collection_run where status = 'failed'")
    assert source == "weather"
    assert error.startswith("payload is not valid JSON")


# --- A pipeline bug ------------------------------------------------------------------


def test_unknown_run_status_fails_the_build(tmp_path):
    for moment in every_15_minutes(at(8), at(8, 30)):
        collect(tmp_path, moment)
    started = at(8, 45)
    ingest.append_run_log(tmp_path / "run_log", started, started, [ingest.Result("weather", "exploded")])

    succeeded, results = build(tmp_path, as_of=at(8, 50))

    assert not succeeded
    assert results["accepted_values_fct_collection_run_status__written__skipped_duplicate__skipped_not_due__failed"] == "fail"


# --- Command-line summary ------------------------------------------------------------


def test_warnings_are_listed_after_the_build(capsys):
    transform.print_warnings([
        transform.CheckResult("assert_station_status_is_fresh", "warn", 1, ""),
        transform.CheckResult("unique_dim_station_station_id", "pass", 0, ""),
    ])
    out = capsys.readouterr().out
    assert "1 warning(s)" in out
    assert "assert_station_status_is_fresh: 1 row(s) returned" in out
    assert "unique_dim_station_station_id" not in out


def test_clean_run_says_so(capsys):
    transform.print_warnings([transform.CheckResult("unique_dim_station_station_id", "pass", 0, "")])
    assert "all checks passed" in capsys.readouterr().out
