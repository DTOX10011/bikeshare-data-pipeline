"""What Dagster runs, and when.

Collection itself runs on GitHub every 15 minutes (see .github/workflows/collect.yml) and
saves snapshots to the data branch. Dagster takes it from there on this machine:

    raw/*            pulls the newest snapshots from the data branch into a local folder
                     (one asset per raw source, all produced by one step: collected_data)
    warehouse        runs dbt: staging, facts, dimensions, quality tables and every check

Two jobs use those assets:

    refresh_warehouse   both assets, every hour at ten past (a schedule that starts on its own)
    reload_days         the warehouse only, for a chosen range of days: a backfill

Paths come from environment variables, so nothing here is specific to one machine:

    BIKESHARE_DATA_DIR   folder holding raw/ and run_log/        (default: data)
    WAREHOUSE_PATH       the DuckDB file to build                (default: data/warehouse.duckdb)
"""

import os
import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    AssetSpec,
    Backoff,
    Config,
    ConfigurableResource,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    RetryPolicy,
    ScheduleDefinition,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = ROOT / "transform"


class PipelinePaths(ConfigurableResource):
    """Where the collected data is, and where the warehouse goes."""

    data_dir: str = os.environ.get("BIKESHARE_DATA_DIR", "data")
    warehouse: str = os.environ.get("WAREHOUSE_PATH", "data/warehouse.duckdb")

    @property
    def raw_dir(self) -> Path:
        return Path(self.data_dir).resolve() / "raw"

    @property
    def run_log_dir(self) -> Path:
        return Path(self.data_dir).resolve() / "run_log"

    def dbt_environment(self) -> dict[str, str]:
        """The settings the dbt project reads (see transform/profiles.yml and macros/)."""
        return {
            "RAW_DIR": self.raw_dir.as_posix(),
            "RUN_LOG_DIR": self.run_log_dir.as_posix(),
            "WAREHOUSE_PATH": Path(self.warehouse).resolve().as_posix(),
        }


# --- Collected data ------------------------------------------------------------------


def count_snapshots(raw_dir: Path) -> dict[str, int]:
    return {
        source.name: sum(1 for _ in source.glob("dt=*/hour=*/*.json.gz"))
        for source in sorted(Path(raw_dir).glob("*"))
        if source.is_dir()
    }


RAW_SOURCES = ["station_status", "station_information", "weather"]


@multi_asset(
    name="collected_data",
    specs=[
        AssetSpec(["raw", name], group_name="collection", description=f"Raw {name} snapshots.")
        for name in RAW_SOURCES
    ] + [AssetSpec(["raw", "run_log"], group_name="collection", description="The ingest job's run log.")],
    # Pulling from GitHub can fail for a moment (network, rate limits): try again, waiting
    # longer each time, before giving up.
    retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL),
)
def collected_data(context: AssetExecutionContext, paths: PipelinePaths):
    """Raw snapshots and the run log, pulled from the data branch when the data folder is a git checkout."""
    data_dir = Path(paths.data_dir).resolve()
    before = count_snapshots(paths.raw_dir)

    is_git_checkout = (data_dir / ".git").exists()
    if is_git_checkout:
        result = subprocess.run(
            ["git", "-C", str(data_dir), "pull", "--ff-only", "--quiet"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git pull failed in {data_dir}: {result.stderr.strip()}")
    else:
        context.log.info(f"{data_dir} is not a git checkout, so it is used as it is.")

    after = count_snapshots(paths.raw_dir)
    if not any(after.get(name) for name in RAW_SOURCES):
        raise RuntimeError(f"No snapshots under {paths.raw_dir}. Check BIKESHARE_DATA_DIR.")

    for name in RAW_SOURCES:
        yield MaterializeResult(
            asset_key=["raw", name],
            metadata={
                "files": after.get(name, 0),
                "new_files": after.get(name, 0) - before.get(name, 0),
                "pulled_from_git": is_git_checkout,
            },
        )
    run_logs = sorted(paths.run_log_dir.glob("dt=*/runs.jsonl"))
    yield MaterializeResult(asset_key=["raw", "run_log"], metadata={"days": len(run_logs)})


# --- Warehouse (the dbt project) -----------------------------------------------------

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
if not dbt_project.manifest_path.exists():
    # Dagster reads the dbt project's manifest to learn its models and tests. Build it once.
    subprocess.run(
        [sys.executable, "-c", "from dbt.cli.main import cli; cli()", "--quiet", "parse",
         "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        check=True,
    )


class WarehouseConfig(Config):
    """Leave both dates empty for a normal run. Set them to reload just those days."""

    backfill_start_date: str | None = None  # YYYY-MM-DD
    backfill_end_date: str | None = None    # YYYY-MM-DD, inclusive
    full_refresh: bool = False              # rebuild every table from all raw files


def dbt_build_args(config: WarehouseConfig) -> list[str]:
    args = ["build"]
    if config.full_refresh:
        args.append("--full-refresh")
    if config.backfill_start_date or config.backfill_end_date:
        if not (config.backfill_start_date and config.backfill_end_date):
            raise ValueError("A backfill needs both backfill_start_date and backfill_end_date.")
        if config.backfill_start_date > config.backfill_end_date:
            raise ValueError("backfill_start_date is after backfill_end_date.")
        args += ["--vars", (
            f'{{"backfill_start_date": "{config.backfill_start_date}", '
            f'"backfill_end_date": "{config.backfill_end_date}"}}'
        )]
    return args


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    # One retry, a minute later: covers the warehouse file being briefly locked by another
    # program. A failed check is not worth retrying, but it is cheap to rule out a lock.
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def warehouse(context: AssetExecutionContext, dbt: DbtCliResource, paths: PipelinePaths, config: WarehouseConfig):
    os.environ.update(paths.dbt_environment())
    yield from dbt.cli(dbt_build_args(config), context=context).stream()


# --- Jobs and schedule ---------------------------------------------------------------

refresh_warehouse = define_asset_job(
    "refresh_warehouse",
    selection=AssetSelection.assets(collected_data) | AssetSelection.assets(warehouse),
    description="Pull the newest snapshots, then rebuild the warehouse and run every check.",
)

reload_days = define_asset_job(
    "reload_days",
    selection=AssetSelection.assets(warehouse),
    description="Backfill: reload a chosen range of days from the raw files. Set the dates in the run config.",
    config={"ops": {"warehouse": {"config": {
        "backfill_start_date": "2026-10-01", "backfill_end_date": "2026-10-01", "full_refresh": False,
    }}}},
)

hourly = ScheduleDefinition(
    job=refresh_warehouse,
    cron_schedule="10 * * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Every hour at ten past, while Dagster is running.",
)

defs = Definitions(
    assets=[collected_data, warehouse],
    jobs=[refresh_warehouse, reload_days],
    schedules=[hourly],
    resources={
        "paths": PipelinePaths(),
        "dbt": DbtCliResource(project_dir=dbt_project, profiles_dir=str(DBT_PROJECT_DIR)),
    },
)
