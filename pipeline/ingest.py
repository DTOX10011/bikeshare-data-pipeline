"""Land raw snapshots of every source in date-partitioned storage.

Each run downloads the current snapshot of every source and stores it exactly as
received (gzipped). Files are named after the timestamp the source itself reports,
so running the job again before the source refreshes writes nothing new.

Every run also appends one record to a run log, so later stages can see what was
collected, what was skipped, and what failed.

Usage:
    python -m pipeline.ingest                      # collect once
    python -m pipeline.ingest --every 900          # collect every 15 minutes until stopped
    python -m pipeline.ingest --sources weather --raw-dir data/raw
"""

import argparse
import gzip
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline import config

log = logging.getLogger("ingest")

# Statuses a source can end a run with.
WRITTEN = "written"                      # a new snapshot was stored
SKIPPED_DUPLICATE = "skipped_duplicate"  # the source had not refreshed since the last run
SKIPPED_NOT_DUE = "skipped_not_due"      # once-per-day source already collected today
FAILED = "failed"


@dataclass
class Result:
    """What happened to one source in one run. One of these per source goes in the run log."""

    source: str
    status: str
    bytes: int | None = None
    snapshot_time: str | None = None
    path: str | None = None
    error: str | None = None


def build_session() -> requests.Session:
    """HTTP session that retries temporary failures with increasing waits."""
    retry = Retry(
        total=4,
        backoff_factor=1,  # waits get longer after each failed attempt
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "bikeshare-data-pipeline (portfolio project)"
    return session


def fetch(session, url: str) -> bytes:
    response = session.get(url, timeout=config.REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content


# --- Reading the snapshot time out of each kind of payload -------------------------------
#
# Each parser returns the time the source says the snapshot was produced, in UTC. It also
# acts as a basic validity check: a payload that does not look like the expected format
# raises ValueError and is never stored.


def _load_json(payload: bytes):
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"payload is not valid JSON: {exc}") from exc


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def gbfs_snapshot_time(payload: bytes) -> datetime:
    """Snapshot time of a GBFS station feed (its last_updated field)."""
    doc = _load_json(payload)
    data = doc.get("data") if isinstance(doc, dict) else None
    if not isinstance(data, dict) or not isinstance(data.get("stations"), list):
        raise ValueError("payload has no data.stations list")

    last_updated = doc.get("last_updated")
    if _is_number(last_updated):
        return datetime.fromtimestamp(last_updated, tz=timezone.utc)  # GBFS v1 and v2
    if isinstance(last_updated, str):
        try:  # GBFS v3 uses text timestamps such as 2026-10-03T21:04:05Z
            parsed = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"unreadable last_updated: {last_updated!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise ValueError("payload has no usable last_updated timestamp")


def open_meteo_snapshot_time(payload: bytes) -> datetime:
    """Snapshot time of an Open-Meteo current-conditions response (its current.time field)."""
    doc = _load_json(payload)
    if not isinstance(doc, dict):
        raise ValueError("payload is not a JSON object")
    if doc.get("error"):
        raise ValueError(f"weather API reported an error: {doc.get('reason')}")
    current = doc.get("current")
    if not isinstance(current, dict):
        raise ValueError("payload has no current conditions")

    observed = current.get("time")
    if _is_number(observed):  # what we ask for: Unix seconds
        return datetime.fromtimestamp(observed, tz=timezone.utc)
    if isinstance(observed, str):  # the API's default: local text time plus a UTC offset
        try:
            parsed = datetime.fromisoformat(observed.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"unreadable current.time: {observed!r}") from exc
        if parsed.tzinfo is None:
            offset = doc.get("utc_offset_seconds")
            offset = offset if _is_number(offset) else 0
            parsed = parsed.replace(tzinfo=timezone(timedelta(seconds=offset)))
        return parsed.astimezone(timezone.utc)
    raise ValueError("payload has no usable current.time timestamp")


SNAPSHOT_TIME_PARSERS = {
    "gbfs": gbfs_snapshot_time,
    "open_meteo": open_meteo_snapshot_time,
}


# --- Storing snapshots -------------------------------------------------------------------


def raw_path(raw_dir: Path, name: str, ts: datetime) -> Path:
    """data/raw/<source>/dt=YYYY-MM-DD/hour=HH/<source>_<timestamp>.json.gz"""
    return (
        Path(raw_dir)
        / name
        / f"dt={ts:%Y-%m-%d}"
        / f"hour={ts:%H}"
        / f"{name}_{ts:%Y%m%dT%H%M%SZ}.json.gz"
    )


def land_snapshot(raw_dir: Path, source: config.Source, payload: bytes) -> tuple[Path, datetime, bool]:
    """Store one snapshot.

    Returns (path, snapshot_time, written); written is False if it was already there.
    """
    ts = SNAPSHOT_TIME_PARSERS[source.format](payload)
    path = raw_path(raw_dir, source.name, ts)
    if path.exists():
        return path, ts, False

    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temporary file, then rename: a crash mid-write never leaves a half file
    # under the final name.
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
            gz.write(payload)
        os.replace(tmp_name, path)
    except BaseException:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise
    return path, ts, True


def collected_on(raw_dir: Path, name: str, day: datetime) -> bool:
    """True if at least one snapshot of this source is already stored for that UTC day."""
    partition = Path(raw_dir) / name / f"dt={day:%Y-%m-%d}"
    return any(partition.glob("hour=*/*.json.gz"))


def ingest_source(session, source: config.Source, raw_dir: Path, now: datetime) -> Result:
    """Fetch and land one source. Never raises for expected problems; failures are reported."""
    if source.once_per_day and collected_on(raw_dir, source.name, now):
        log.info("source=%s status=%s", source.name, SKIPPED_NOT_DUE)
        return Result(source.name, SKIPPED_NOT_DUE)

    try:
        payload = fetch(session, source.url)
        path, ts, written = land_snapshot(raw_dir, source, payload)
    except (requests.RequestException, ValueError, OSError) as exc:
        log.error("source=%s status=%s error=%s", source.name, FAILED, exc)
        return Result(source.name, FAILED, error=str(exc))

    status = WRITTEN if written else SKIPPED_DUPLICATE
    log.info("source=%s status=%s bytes=%d path=%s", source.name, status, len(payload), path)
    return Result(
        source.name,
        status,
        bytes=len(payload),
        snapshot_time=ts.isoformat(),
        path=path.relative_to(raw_dir).as_posix(),
    )


# --- Runs --------------------------------------------------------------------------------


def run_once(session, sources: list[config.Source], raw_dir: Path, run_log_dir: Path, now=None) -> list[Result]:
    """Collect every source once and append a record of the run to the run log."""
    started = now or datetime.now(timezone.utc)
    results = [ingest_source(session, source, raw_dir, started) for source in sources]
    finished = now or datetime.now(timezone.utc)
    append_run_log(run_log_dir, started, finished, results)
    return results


def append_run_log(run_log_dir: Path, started: datetime, finished: datetime, results: list[Result]) -> Path:
    """Append one JSON line describing the run to run_log/dt=YYYY-MM-DD/runs.jsonl."""
    record = {
        "run_id": f"{started:%Y%m%dT%H%M%SZ}",
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "sources": [asdict(result) for result in results],
    }
    path = Path(run_log_dir) / f"dt={started:%Y-%m-%d}" / "runs.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    return path


def run_forever(collect, every_seconds: float, sleep=time.sleep, max_runs=None) -> None:
    """Call collect() repeatedly, waiting between runs. Stops after max_runs if given."""
    runs = 0
    while True:
        collect()
        runs += 1
        if max_runs is not None and runs >= max_runs:
            return
        sleep(every_seconds)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Land raw snapshots of bike share and weather data.")
    parser.add_argument("--sources", nargs="+", choices=sorted(config.SOURCES), default=sorted(config.SOURCES))
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_DIR)
    parser.add_argument(
        "--run-log-dir", type=Path, default=None,
        help="where the run log goes (default: a run_log folder next to the raw folder)",
    )
    parser.add_argument(
        "--every", type=float, default=None, metavar="SECONDS",
        help="keep collecting, waiting this many seconds between runs, until stopped",
    )
    args = parser.parse_args(argv)
    if args.every is not None and args.every <= 0:
        parser.error("--every must be greater than 0")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    session = build_session()
    sources = [config.SOURCES[name] for name in args.sources]
    run_log_dir = args.run_log_dir or args.raw_dir.parent / "run_log"

    def collect() -> list[Result]:
        return run_once(session, sources, args.raw_dir, run_log_dir)

    if args.every is None:
        return 1 if any(result.status == FAILED for result in collect()) else 0

    def collect_and_keep_going() -> None:
        try:  # one bad run must not end a collection that is meant to run for days
            collect()
        except Exception:
            log.exception("run failed unexpectedly; will try again on the next run")

    log.info("collecting every %s seconds; press Ctrl+C to stop", args.every)
    try:
        run_forever(collect_and_keep_going, args.every)
    except KeyboardInterrupt:
        log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
