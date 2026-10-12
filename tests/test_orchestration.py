"""Tests for the Dagster definitions: what is scheduled, how the data branch is pulled, and
that the warehouse job and the backfill job really run."""

import json
import shutil
import subprocess
from datetime import timedelta

import duckdb
import pytest
from dagster import AssetKey, DefaultScheduleStatus, build_asset_context, materialize
from dagster_dbt import DbtCliResource

from fake_collection import STATUS, at, collect, every_15_minutes, healthy_row, STATIONS, unix
from orchestration.definitions import (
    DBT_PROJECT_DIR,
    PipelinePaths,
    WarehouseConfig,
    collected_data,
    dbt_build_args,
    dbt_project,
    defs,
    warehouse,
)
from pipeline import ingest


def git(*args, cwd=None):
    subprocess.run(
        ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
        cwd=cwd, check=True, capture_output=True,
    )


def resources(data_dir, warehouse_path):
    return {
        "paths": PipelinePaths(data_dir=str(data_dir), warehouse=str(warehouse_path)),
        "dbt": DbtCliResource(project_dir=dbt_project, profiles_dir=str(DBT_PROJECT_DIR)),
    }


def query(warehouse_path, sql):
    with duckdb.connect(str(warehouse_path), read_only=True) as con:
        return con.execute(sql).fetchall()


# --- What is defined -----------------------------------------------------------------


def test_jobs_and_schedule_are_defined():
    assert {job.name for job in defs.resolve_all_job_defs()} >= {"refresh_warehouse", "reload_days"}
    schedule = defs.resolve_schedule_def("refresh_warehouse_schedule")
    assert schedule.cron_schedule == "10 * * * *"
    assert schedule.execution_timezone == "America/New_York"
    assert schedule.default_status == DefaultScheduleStatus.RUNNING


def test_lineage_runs_from_raw_sources_through_staging_to_the_marts():
    graph = defs.resolve_asset_graph()
    parents = lambda key: {p.to_user_string() for p in graph.get(AssetKey(key.split("/"))).parent_keys}
    assert parents("staging/base_station_status_rows") == {"raw/station_status"}
    assert parents("staging/stg_weather") == {"raw/weather"}
    assert parents("staging/stg_run_log") == {"raw/run_log"}
    assert "marts/fct_station_status" in parents("marts/agg_station_hourly")


def test_dbt_tests_become_dagster_checks():
    checks = {key.name for key in defs.resolve_asset_graph().asset_check_keys}
    assert "unique_fct_station_status_station_snapshot_key" in checks
    assert "assert_station_status_is_fresh" in checks


# --- Building dbt's command line -----------------------------------------------------


def test_normal_run_is_a_plain_build():
    assert dbt_build_args(WarehouseConfig()) == ["build"]


def test_backfill_passes_both_dates_to_dbt():
    args = dbt_build_args(WarehouseConfig(backfill_start_date="2026-10-01", backfill_end_date="2026-10-03"))
    assert args[:2] == ["build", "--vars"]
    assert json.loads(args[2]) == {"backfill_start_date": "2026-10-01", "backfill_end_date": "2026-10-03"}


@pytest.mark.parametrize("start, end", [("2026-10-01", None), (None, "2026-10-01"), ("2026-10-05", "2026-10-01")])
def test_backfill_needs_a_valid_range(start, end):
    with pytest.raises(ValueError):
        dbt_build_args(WarehouseConfig(backfill_start_date=start, backfill_end_date=end))


# --- Pulling the data branch ---------------------------------------------------------


def results_by_asset(results):
    return {tuple(r.asset_key.path): r.metadata for r in results}


@pytest.fixture
def data_branch(tmp_path):
    """A stand-in for GitHub: a remote with a data branch, and a local checkout of it."""
    remote, writer, local = tmp_path / "remote.git", tmp_path / "writer", tmp_path / "local"
    git("init", "--bare", "-b", "data", str(remote))
    git("clone", str(remote), str(writer))
    collect(writer, at(8))
    git("add", "-A", cwd=writer)
    git("commit", "-m", "first run", cwd=writer)
    git("push", "origin", "HEAD:data", cwd=writer)
    git("clone", "--branch", "data", str(remote), str(local))
    return writer, local


def test_new_snapshots_are_pulled_from_the_data_branch(data_branch):
    writer, local = data_branch
    collect(writer, at(8, 15))  # the scheduled job collects again on GitHub...
    git("add", "-A", cwd=writer)
    git("commit", "-m", "second run", cwd=writer)
    git("push", "origin", "HEAD:data", cwd=writer)

    results = results_by_asset(collected_data(build_asset_context(), paths=PipelinePaths(data_dir=str(local))))

    assert results[("raw", "station_status")]["files"] == 2
    assert results[("raw", "station_status")]["new_files"] == 1
    assert results[("raw", "station_status")]["pulled_from_git"] is True
    assert results[("raw", "run_log")]["days"] == 1


def test_a_folder_that_is_not_a_checkout_is_used_as_it_is(tmp_path):
    collect(tmp_path, at(8))

    results = results_by_asset(collected_data(build_asset_context(), paths=PipelinePaths(data_dir=str(tmp_path))))

    assert results[("raw", "weather")]["files"] == 1
    assert results[("raw", "weather")]["pulled_from_git"] is False


def test_a_failed_pull_is_reported(data_branch):
    _, local = data_branch
    git("remote", "set-url", "origin", "/nowhere/remote.git", cwd=local)

    with pytest.raises(RuntimeError, match="git pull failed"):
        list(collected_data(build_asset_context(), paths=PipelinePaths(data_dir=str(local))))


def test_an_empty_data_folder_is_reported(tmp_path):
    with pytest.raises(RuntimeError, match="No snapshots"):
        list(collected_data(build_asset_context(), paths=PipelinePaths(data_dir=str(tmp_path))))


# --- Running the jobs end to end -----------------------------------------------------


@pytest.fixture(scope="module")
def refreshed(tmp_path_factory):
    """What the hourly job does: pull, then build the warehouse and run every check."""
    root = tmp_path_factory.mktemp("refresh")
    for moment in every_15_minutes(at(8), at(10)):
        collect(root, moment)
    result = materialize([collected_data, warehouse], resources=resources(root, root / "warehouse.duckdb"))
    return root, result


def test_hourly_job_builds_every_table(refreshed):
    root, result = refreshed
    assert result.success
    assert query(root / "warehouse.duckdb", "select count(*) from marts.fct_station_status") == [(90,)]
    assert query(root / "warehouse.duckdb", "select status_snapshots from quality.dq_daily_summary") == [(9,)]


def test_hourly_job_reports_check_results(refreshed):
    _, result = refreshed
    evaluations = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evaluations["unique_fct_station_status_station_snapshot_key"].passed
    # The fake data is from 2026, so measured against today it is not fresh: a warning.
    freshness = evaluations["assert_station_status_is_fresh"]
    assert not freshness.passed
    assert freshness.severity.value == "WARN"


def test_backfill_job_reloads_a_day_outside_the_lookback_window(refreshed, tmp_path):
    root, _ = refreshed
    shutil.copytree(root / "raw", tmp_path / "raw")
    shutil.copytree(root / "run_log", tmp_path / "run_log")
    shutil.copy(root / "warehouse.duckdb", tmp_path / "warehouse.duckdb")
    # A day of data recovered from somewhere else, two weeks before everything loaded so far.
    old = at(9) - timedelta(days=14)
    rows = [healthy_row(s, old) for s in STATIONS]
    ingest.land_snapshot(tmp_path / "raw", STATUS, json.dumps({"last_updated": unix(old), "data": {"stations": rows}}).encode())

    result = materialize(
        [warehouse],
        resources=resources(tmp_path, tmp_path / "warehouse.duckdb"),
        run_config={"ops": {"warehouse": {"config": {
            "backfill_start_date": old.strftime("%Y-%m-%d"), "backfill_end_date": old.strftime("%Y-%m-%d"),
        }}}},
    )

    assert result.success
    assert query(tmp_path / "warehouse.duckdb", "select count(*) from marts.fct_station_status") == [(100,)]
