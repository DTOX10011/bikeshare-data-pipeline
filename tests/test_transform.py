"""End-to-end tests for the SQL models.

Each test builds a small raw zone with the same code the ingest job uses, runs the dbt
project against it, and checks the tables that come out.
"""

import json
import shutil
from datetime import datetime, timezone

import duckdb
import pytest

from pipeline import config, ingest, transform

STATUS = config.SOURCES["station_status"]
INFORMATION = config.SOURCES["station_information"]
WEATHER = config.SOURCES["weather"]


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def unix(text: str) -> int:
    return int(utc(text).timestamp())


def at(text: str) -> datetime:
    """A warehouse timestamp. The tables store UTC times without a time zone attached."""
    return datetime.fromisoformat(text)


def status_row(station_id, bikes, docks, ebikes=0, renting=1, returning=1, reported="2026-10-03T21:00:00"):
    return {
        "station_id": station_id,
        "num_bikes_available": bikes,
        "num_ebikes_available": ebikes,
        "num_bikes_disabled": 0,
        "num_docks_available": docks,
        "num_docks_disabled": 0,
        "is_installed": 1,
        "is_renting": renting,
        "is_returning": returning,
        "last_reported": unix(reported),
    }


def info_row(station_id, name, capacity):
    return {
        "station_id": station_id,
        "name": name,
        "short_name": f"{station_id}.01",
        "lat": 40.74,
        "lon": -73.99,
        "capacity": capacity,
        "region_id": "71",
        "station_type": "classic",
    }


def land_stations(raw, source, at, stations):
    payload = json.dumps({"last_updated": unix(at), "ttl": 60, "data": {"stations": stations}}).encode()
    ingest.land_snapshot(raw, source, payload)


def land_weather(raw, at, temperature, precipitation=0.0):
    current = {
        "time": unix(at), "interval": 900, "temperature_2m": temperature,
        "apparent_temperature": temperature - 1, "relative_humidity_2m": 50,
        "precipitation": precipitation, "rain": precipitation, "snowfall": 0.0,
        "weather_code": 3, "cloud_cover": 80, "wind_speed_10m": 9.0, "wind_gusts_10m": 20.0, "is_day": 1,
    }
    ingest.land_snapshot(raw, WEATHER, json.dumps({"utc_offset_seconds": 0, "current": current}).encode())


def make_raw_zone(raw):
    """Two days of data. 2026-10-03 is a Saturday; New York is four hours behind UTC."""
    # Day 1 reference data: stations A, B and C.
    land_stations(raw, INFORMATION, "2026-10-03T21:00:00", [
        info_row("A", "Alpha St", 20), info_row("B", "Bravo Ave", 10), info_row("C", "Charlie Pl", 15),
    ])
    # 21:04 - B is empty, C is full.
    land_stations(raw, STATUS, "2026-10-03T21:04:05", [
        status_row("A", bikes=7, ebikes=2, docks=13), status_row("B", bikes=0, docks=10),
        status_row("C", bikes=15, docks=0),
    ])
    # 21:19 - A is listed twice; the row reported most recently should win.
    land_stations(raw, STATUS, "2026-10-03T21:19:05", [
        status_row("A", bikes=99, docks=0, reported="2026-10-03T20:00:00"),
        status_row("A", bikes=5, docks=15, reported="2026-10-03T21:18:00"),
        status_row("B", bikes=0, docks=10), status_row("C", bikes=14, docks=1),
    ])
    # 21:34 - A has an unreadable bike count, B is switched off, E is not in the reference data.
    land_stations(raw, STATUS, "2026-10-03T21:34:05", [
        status_row("A", bikes="n/a", docks=15), status_row("B", bikes=0, docks=10, renting=0),
        status_row("C", bikes=14, docks=1), status_row("E", bikes=3, docks=4),
    ])
    land_stations(raw, STATUS, "2026-10-03T22:04:05", [
        status_row("A", bikes=6, docks=14), status_row("B", bikes=2, docks=8), status_row("C", bikes=12, docks=3),
    ])
    # 23:04 - an hour with no weather readings.
    land_stations(raw, STATUS, "2026-10-03T23:04:05", [
        status_row("A", bikes=6, docks=14), status_row("B", bikes=2, docks=8), status_row("C", bikes=12, docks=3),
    ])
    # Day 2 reference data: A is renamed and resized, C is gone, D is new.
    land_stations(raw, INFORMATION, "2026-10-04T00:05:00", [
        info_row("A", "Alpha St & 1 Ave", 22), info_row("B", "Bravo Ave", 10), info_row("D", "Delta Sq", 30),
    ])
    land_stations(raw, STATUS, "2026-10-04T00:10:05", [
        status_row("A", bikes=8, docks=14), status_row("B", bikes=1, docks=9), status_row("D", bikes=30, docks=0),
    ])
    land_weather(raw, "2026-10-03T21:00:00", 17.0)
    land_weather(raw, "2026-10-03T21:15:00", 18.0, precipitation=0.4)
    land_weather(raw, "2026-10-03T21:30:00", 19.0)
    land_weather(raw, "2026-10-03T22:00:00", 16.0)
    land_weather(raw, "2026-10-04T00:00:00", 12.0)


class Warehouse:
    """A raw zone and the warehouse built from it."""

    def __init__(self, folder):
        self.raw = folder / "raw"
        self.path = folder / "warehouse.duckdb"

    def build(self, full_refresh=False) -> bool:
        return transform.run_dbt(self.raw, self.path, full_refresh=full_refresh, quiet=True)

    def query(self, sql, *params):
        with duckdb.connect(str(self.path), read_only=True) as con:
            return con.execute(sql, params).fetchall()

    def one(self, sql, *params):
        (row,) = self.query(sql, *params)
        return row

    def copy_to(self, folder) -> "Warehouse":
        shutil.copytree(self.raw, folder / "raw")
        shutil.copy(self.path, folder / "warehouse.duckdb")
        return Warehouse(folder)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """The standard raw zone, built once and shared by the read-only tests."""
    warehouse = Warehouse(tmp_path_factory.mktemp("built"))
    make_raw_zone(warehouse.raw)
    assert warehouse.build(), "dbt build failed on the standard raw zone"
    return warehouse


@pytest.fixture
def fresh(built, tmp_path):
    """A private copy of the built warehouse, for tests that add data and rebuild."""
    return built.copy_to(tmp_path)


STATUS_AT = "select {} from marts.fct_station_status where station_id = ? and snapshot_at = ?"


# --- fct_station_status --------------------------------------------------------------


def test_fact_has_one_row_per_station_per_snapshot(built):
    assert built.one("select count(*) from marts.fct_station_status") == (19,)
    assert built.one(
        "select count(*) from (select distinct station_id, snapshot_at from marts.fct_station_status)"
    ) == (19,)


def test_station_listed_twice_in_a_snapshot_keeps_the_most_recent_report(built):
    rows = built.query(STATUS_AT.format("num_bikes_available, num_docks_available"), "A", at("2026-10-03T21:19:05"))
    assert rows == [(5, 15)]


def test_unreadable_number_becomes_null_and_the_row_is_kept(built):
    assert built.one(STATUS_AT.format("num_bikes_available, num_docks_available"), "A", at("2026-10-03T21:34:05")) == (None, 15)


def test_classic_bikes_are_total_bikes_minus_ebikes(built):
    assert built.one(
        STATUS_AT.format("num_bikes_available, num_ebikes_available, num_classic_bikes_available"),
        "A", at("2026-10-03T21:04:05"),
    ) == (7, 2, 5)


def test_empty_and_full_only_count_while_the_station_is_in_service(built):
    flags = "is_empty, is_full, is_renting"
    assert built.one(STATUS_AT.format(flags), "B", at("2026-10-03T21:04:05")) == (True, False, True)
    assert built.one(STATUS_AT.format(flags), "C", at("2026-10-03T21:04:05")) == (False, True, True)
    # B has no bikes at 21:34 but is switched off, so it is not counted as empty.
    assert built.one(STATUS_AT.format(flags), "B", at("2026-10-03T21:34:05")) == (False, False, False)


def test_timestamps_are_stored_in_utc(built):
    assert built.one("select min(snapshot_at), max(snapshot_at) from marts.fct_station_status") == (
        datetime(2026, 10, 3, 21, 4, 5), datetime(2026, 10, 4, 0, 10, 5),
    )


def test_each_row_records_the_file_it_came_from(built):
    assert built.one(STATUS_AT.format("source_file"), "A", at("2026-10-03T21:04:05")) == (
        "station_status_20261003T210405Z.json.gz",
    )


# --- dim_station ---------------------------------------------------------------------


def test_dimension_has_one_row_for_every_station_ever_seen(built):
    assert built.query("select station_id from marts.dim_station order by 1") == [("A",), ("B",), ("C",), ("D",), ("E",)]


def test_dimension_holds_the_latest_details_and_when_the_station_was_seen(built):
    assert built.one(
        "select station_name, capacity, first_seen_at, last_seen_at from marts.dim_station where station_id = 'A'"
    ) == ("Alpha St & 1 Ave", 22, datetime(2026, 10, 3, 21, 0), datetime(2026, 10, 4, 0, 5))


def test_station_removed_from_reference_data_is_kept_and_flagged(built):
    assert built.one(
        "select station_name, is_in_latest_reference_data, is_inferred from marts.dim_station where station_id = 'C'"
    ) == ("Charlie Pl", False, False)


def test_station_with_status_but_no_reference_data_is_inferred(built):
    assert built.one(
        "select station_name, capacity, is_inferred from marts.dim_station where station_id = 'E'"
    ) == (None, None, True)


def test_every_fact_row_joins_to_a_station(built):
    assert built.one(
        "select count(*) from marts.fct_station_status f left join marts.dim_station d using (station_id) "
        "where d.station_id is null"
    ) == (0,)


# --- fct_weather and agg_station_hourly ----------------------------------------------


def test_weather_has_one_row_per_reading(built):
    assert built.one("select count(*), min(temperature_c), max(precipitation_mm) from marts.fct_weather") == (5, 12.0, 0.4)


HOURLY = "select {} from marts.agg_station_hourly where station_id = ? and hour_utc = ?"


def test_hourly_table_averages_availability_within_the_hour(built):
    # A at 21:04, 21:19 and 21:34 had 7, 5 and an unreadable count: the average uses the two known values.
    assert built.one(
        HOURLY.format("snapshot_count, avg_bikes_available, min_bikes_available, max_bikes_available"),
        "A", at("2026-10-03T21:00:00"),
    ) == (3, 6.0, 5, 7)


def test_hourly_table_reports_the_share_of_snapshots_empty_and_full(built):
    # B was empty in two of its three snapshots that hour; in the third it was switched off.
    assert built.one(
        HOURLY.format("share_of_snapshots_empty, share_of_snapshots_renting"), "B", at("2026-10-03T21:00:00")
    ) == (0.6667, 0.6667)
    assert built.one(HOURLY.format("share_of_snapshots_full"), "C", at("2026-10-03T21:00:00")) == (0.3333,)


def test_hourly_table_carries_that_hours_weather(built):
    assert built.one(
        HOURLY.format("weather_reading_count, avg_temperature_c, had_precipitation"), "A", at("2026-10-03T21:00:00")
    ) == (3, 18.0, True)
    assert built.one(HOURLY.format("avg_temperature_c, had_precipitation"), "A", at("2026-10-03T22:00:00")) == (16.0, False)


def test_hour_without_weather_readings_keeps_its_rows_with_empty_weather(built):
    assert built.one(
        HOURLY.format("snapshot_count, weather_reading_count, avg_temperature_c, had_precipitation"),
        "A", at("2026-10-03T23:00:00"),
    ) == (1, None, None, None)


def test_hourly_table_adds_local_time(built):
    # 21:00 UTC on Saturday 3 October is 17:00 in New York. Midnight UTC is still Saturday evening there.
    columns = "hour_local, local_hour_of_day, local_day_of_week, is_weekend"
    assert built.one(HOURLY.format(columns), "A", at("2026-10-03T21:00:00")) == (datetime(2026, 10, 3, 17), 17, 6, True)
    assert built.one(HOURLY.format(columns), "A", at("2026-10-04T00:00:00")) == (datetime(2026, 10, 3, 20), 20, 6, True)


# --- Reruns and incremental loads ----------------------------------------------------


def table_fingerprints(warehouse):
    tables = ["fct_station_status", "fct_weather", "dim_station", "agg_station_hourly"]
    return {
        table: warehouse.one(f"select count(*), bit_xor(hash(columns(*))) from marts.{table}")
        for table in tables
    }


def test_running_again_with_no_new_data_changes_nothing(fresh):
    before = table_fingerprints(fresh)
    assert fresh.build()
    assert table_fingerprints(fresh) == before


def test_new_snapshot_is_added_by_the_next_run(fresh):
    land_stations(fresh.raw, STATUS, "2026-10-04T00:25:05", [
        status_row("A", bikes=9, docks=13), status_row("B", bikes=1, docks=9), status_row("D", bikes=29, docks=1),
    ])
    assert fresh.build()

    assert fresh.one("select count(*) from marts.fct_station_status") == (22,)
    assert fresh.one(HOURLY.format("snapshot_count, avg_bikes_available"), "A", at("2026-10-04T00:00:00")) == (2, 8.5)


def test_late_snapshot_inside_the_lookback_window_is_loaded(fresh):
    # The newest loaded day is 4 October; a file arriving late for 3 October is still picked up.
    land_stations(fresh.raw, STATUS, "2026-10-03T22:34:05", [status_row("A", bikes=4, docks=16)])
    assert fresh.build()

    assert fresh.one(STATUS_AT.format("num_bikes_available"), "A", at("2026-10-03T22:34:05")) == (4,)


def test_backfill_older_than_the_lookback_window_needs_a_full_refresh(fresh):
    land_stations(fresh.raw, STATUS, "2026-09-20T12:00:05", [status_row("A", bikes=11, docks=9)])

    assert fresh.build()
    assert fresh.query(STATUS_AT.format("num_bikes_available"), "A", at("2026-09-20T12:00:05")) == []

    assert fresh.build(full_refresh=True)
    assert fresh.one(STATUS_AT.format("num_bikes_available"), "A", at("2026-09-20T12:00:05")) == (11,)
    assert fresh.one("select count(*) from marts.fct_station_status") == (20,)


def test_incremental_run_does_not_open_old_date_folders(fresh):
    """A file that cannot be read sits in an old folder. Only a run that opens it would fail."""
    old = fresh.raw / "station_status" / "dt=2026-09-01" / "hour=10" / "station_status_20260901T100000Z.json.gz"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"this is not a gzip file")

    assert fresh.build()
    assert not fresh.build(full_refresh=True)


def test_inferred_station_is_filled_in_once_its_reference_data_arrives(fresh):
    land_stations(fresh.raw, INFORMATION, "2026-10-05T00:05:00", [
        info_row("A", "Alpha St & 1 Ave", 22), info_row("B", "Bravo Ave", 10),
        info_row("D", "Delta Sq", 30), info_row("E", "Echo Rd", 7),
    ])
    assert fresh.build()

    assert fresh.one("select station_name, capacity, is_inferred from marts.dim_station where station_id = 'E'") == (
        "Echo Rd", 7, False,
    )


# --- Command line --------------------------------------------------------------------


def test_main_stops_with_a_clear_message_when_a_source_has_no_data(tmp_path, capsys):
    land_weather(tmp_path / "raw", "2026-10-03T21:00:00", 17.0)

    code = transform.main(["--raw-dir", str(tmp_path / "raw"), "--warehouse", str(tmp_path / "w.duckdb")])

    assert code == 2
    assert "station_information, station_status" in capsys.readouterr().err
    assert not (tmp_path / "w.duckdb").exists()
