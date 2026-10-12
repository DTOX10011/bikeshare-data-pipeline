"""Build the warehouse from the raw zone.

Runs the dbt project in transform/ against a local DuckDB file: staging views over the raw
files, then the fact, dimension and hourly tables, then the data tests.

Usage:
    python -m pipeline.transform                          # raw data in data/raw
    python -m pipeline.transform --raw-dir ../bikeshare-data/raw
    python -m pipeline.transform --full-refresh           # rebuild every table from all raw files
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from pipeline import config

PROJECT_DIR = Path(__file__).resolve().parent.parent / "transform"
DEFAULT_WAREHOUSE = Path("data/warehouse.duckdb")


def missing_sources(raw_dir: Path) -> list[str]:
    """Names of sources with no snapshots at all under raw_dir."""
    return [
        name
        for name in sorted(config.SOURCES)
        if not any((Path(raw_dir) / name).glob("dt=*/hour=*/*.json.gz"))
    ]


def run_dbt(raw_dir: Path, warehouse: Path, full_refresh: bool = False, quiet: bool = False) -> bool:
    """Run dbt against the given raw zone and warehouse file. Returns True on success."""
    warehouse = Path(warehouse).resolve()
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    # dbt reads these when it compiles the models. Forward slashes work on every system.
    env = dict(
        os.environ,
        RAW_DIR=Path(raw_dir).resolve().as_posix(),
        WAREHOUSE_PATH=warehouse.as_posix(),
    )

    # dbt runs as its own process, so the warehouse file is closed again when it finishes.
    command = [sys.executable, "-c", "from dbt.cli.main import cli; cli()"]
    if quiet:
        command.append("--quiet")
    # "build" creates every model and then runs its data tests.
    command += ["build", "--project-dir", str(PROJECT_DIR), "--profiles-dir", str(PROJECT_DIR)]
    if full_refresh:
        command.append("--full-refresh")
    return subprocess.run(command, env=env).returncode == 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build the warehouse tables from raw snapshots.")
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_DIR)
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="rebuild every table from all raw files instead of loading only recent days",
    )
    args = parser.parse_args(argv)

    missing = missing_sources(args.raw_dir)
    if missing:
        print(
            f"No snapshots found under {args.raw_dir} for: {', '.join(missing)}.\n"
            "Collect some first with: python -m pipeline.ingest",
            file=sys.stderr,
        )
        return 2

    return 0 if run_dbt(args.raw_dir, args.warehouse, args.full_refresh) else 1


if __name__ == "__main__":
    sys.exit(main())
