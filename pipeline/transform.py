"""Build the warehouse from the raw zone, then check the data's quality.

Runs the dbt project in transform/ against a local DuckDB file: staging views over the raw
files, then the fact, dimension, hourly and quality tables, then every data test.

Tests come in two strengths. An error means the pipeline itself is wrong (a duplicate key,
a row that joins to nothing) and the command fails. A warning means the data has a problem
worth knowing about (a gap in collection, stations that stopped reporting); the command
still succeeds and lists the warnings at the end.

Usage:
    python -m pipeline.transform                          # raw data in data/raw
    python -m pipeline.transform --raw-dir ../bikeshare-data/raw
    python -m pipeline.transform --full-refresh           # rebuild every table from all raw files
    python -m pipeline.transform --backfill 2026-10-01 2026-10-03   # reload just those days
"""

import argparse
import json
from datetime import date
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from pipeline import config

PROJECT_DIR = Path(__file__).resolve().parent.parent / "transform"
DEFAULT_WAREHOUSE = Path("data/warehouse.duckdb")
DEFAULT_TARGET = PROJECT_DIR / "target"


@dataclass
class CheckResult:
    """The outcome of one dbt data test."""

    name: str
    status: str        # pass, warn, fail or error
    failures: int      # rows the test returned
    message: str


def default_run_log_dir(raw_dir: Path) -> Path:
    """The ingest job writes its run log next to the raw folder."""
    return Path(raw_dir).parent / "run_log"


def missing_inputs(raw_dir: Path, run_log_dir: Path) -> list[str]:
    """Names of the inputs with no files at all: sources under raw_dir, and the run log."""
    missing = [
        name
        for name in sorted(config.SOURCES)
        if not any((Path(raw_dir) / name).glob("dt=*/hour=*/*.json.gz"))
    ]
    if not any(Path(run_log_dir).glob("dt=*/runs.jsonl")):
        missing.append("run log")
    return missing


def run_dbt(
    raw_dir: Path,
    warehouse: Path,
    full_refresh: bool = False,
    quiet: bool = False,
    run_log_dir: Path | None = None,
    dbt_vars: dict | None = None,
    target_path: Path | None = None,
) -> bool:
    """Run dbt build against the given raw zone and warehouse file.

    Returns True unless something failed at error level. Warnings do not count as failure.
    """
    warehouse = Path(warehouse).resolve()
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    run_log_dir = Path(run_log_dir or default_run_log_dir(raw_dir))
    # dbt reads these when it compiles the models. Forward slashes work on every system.
    env = dict(
        os.environ,
        RAW_DIR=Path(raw_dir).resolve().as_posix(),
        RUN_LOG_DIR=run_log_dir.resolve().as_posix(),
        WAREHOUSE_PATH=warehouse.as_posix(),
    )

    # dbt runs as its own process, so the warehouse file is closed again when it finishes.
    command = [sys.executable, "-c", "from dbt.cli.main import cli; cli()"]
    if quiet:
        command.append("--quiet")
    # "build" creates every model and then runs its data tests.
    command += ["build", "--project-dir", str(PROJECT_DIR), "--profiles-dir", str(PROJECT_DIR)]
    if target_path:
        command += ["--target-path", str(Path(target_path).resolve())]
    if full_refresh:
        command.append("--full-refresh")
    if dbt_vars:
        command += ["--vars", json.dumps(dbt_vars)]
    return subprocess.run(command, env=env).returncode == 0


def check_results(target_path: Path = DEFAULT_TARGET) -> list[CheckResult]:
    """The data test outcomes from the most recent dbt run."""
    results_file = Path(target_path) / "run_results.json"
    if not results_file.exists():
        return []
    results = json.loads(results_file.read_text(encoding="utf-8"))["results"]
    return [
        CheckResult(
            name=result["unique_id"].split(".")[2],
            status=result["status"],
            failures=result.get("failures") or 0,
            message=result.get("message") or "",
        )
        for result in results
        if result["unique_id"].startswith("test.")
    ]


def print_warnings(results: list[CheckResult]) -> None:
    warnings = [r for r in results if r.status == "warn"]
    if not warnings:
        print("\nData quality: all checks passed.")
        return
    print(f"\nData quality: {len(warnings)} warning(s). The tables were still built.")
    for result in warnings:
        print(f"  - {result.name}: {result.failures} row(s) returned")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build the warehouse tables from raw snapshots.")
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_DIR)
    parser.add_argument(
        "--run-log-dir", type=Path, default=None,
        help="where the ingest job's run log is (default: a run_log folder next to the raw folder)",
    )
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="rebuild every table from all raw files instead of loading only recent days",
    )
    parser.add_argument(
        "--backfill", nargs=2, metavar=("START", "END"), type=date.fromisoformat,
        help="reload exactly the days from START to END (YYYY-MM-DD, inclusive) and nothing else",
    )
    args = parser.parse_args(argv)
    if args.backfill and args.backfill[0] > args.backfill[1]:
        parser.error("--backfill START must not be after END")
    if args.backfill and args.full_refresh:
        parser.error("use either --backfill or --full-refresh, not both")
    dbt_vars = (
        {"backfill_start_date": args.backfill[0].isoformat(), "backfill_end_date": args.backfill[1].isoformat()}
        if args.backfill else None
    )
    run_log_dir = args.run_log_dir or default_run_log_dir(args.raw_dir)

    missing = missing_inputs(args.raw_dir, run_log_dir)
    if missing:
        print(
            f"No files found for: {', '.join(missing)}.\n"
            f"Looked under {args.raw_dir} and {run_log_dir}.\n"
            "Collect some first with: python -m pipeline.ingest",
            file=sys.stderr,
        )
        return 2

    succeeded = run_dbt(args.raw_dir, args.warehouse, args.full_refresh, run_log_dir=run_log_dir, dbt_vars=dbt_vars)
    print_warnings(check_results())
    return 0 if succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
