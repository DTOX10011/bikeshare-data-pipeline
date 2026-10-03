"""Land raw bike share feed snapshots in date-partitioned storage.

Each run downloads the current snapshot of every feed and stores it exactly as
received (gzipped). Files are named after the timestamp the feed itself reports,
so running the job again before the feed refreshes writes nothing new.

Usage:
    python -m pipeline.ingest
    python -m pipeline.ingest --feeds station_status --raw-dir data/raw
"""

import argparse
import gzip
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline import config

log = logging.getLogger("ingest")


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


def fetch_feed(session, url: str) -> bytes:
    response = session.get(url, timeout=config.REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content


def snapshot_time(payload: bytes) -> datetime:
    """Return the time the feed says this snapshot was produced, in UTC.

    Also acts as a basic validity check: a payload that is not a GBFS feed
    with a list of stations raises ValueError and is never stored.
    """
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"payload is not valid JSON: {exc}") from exc

    data = doc.get("data") if isinstance(doc, dict) else None
    if not isinstance(data, dict) or not isinstance(data.get("stations"), list):
        raise ValueError("payload has no data.stations list")

    last_updated = doc.get("last_updated")
    if isinstance(last_updated, (int, float)) and not isinstance(last_updated, bool):
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


def raw_path(raw_dir: Path, feed: str, ts: datetime) -> Path:
    """data/raw/<feed>/dt=YYYY-MM-DD/hour=HH/<feed>_<timestamp>.json.gz"""
    return (
        Path(raw_dir)
        / feed
        / f"dt={ts:%Y-%m-%d}"
        / f"hour={ts:%H}"
        / f"{feed}_{ts:%Y%m%dT%H%M%SZ}.json.gz"
    )


def land_snapshot(raw_dir: Path, feed: str, payload: bytes) -> tuple[Path, bool]:
    """Store one snapshot. Returns (path, written); written is False if it was already there."""
    path = raw_path(raw_dir, feed, snapshot_time(payload))
    if path.exists():
        return path, False

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
    return path, True


def ingest_feed(session, feed: str, url: str, raw_dir: Path) -> bool:
    """Fetch and land one feed. Returns True on success (including 'already had it')."""
    try:
        payload = fetch_feed(session, url)
        path, written = land_snapshot(raw_dir, feed, payload)
    except (requests.RequestException, ValueError, OSError) as exc:
        log.error("feed=%s status=failed error=%s", feed, exc)
        return False
    log.info(
        "feed=%s status=%s bytes=%d path=%s",
        feed, "written" if written else "skipped_duplicate", len(payload), path,
    )
    return True


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Land raw bike share feed snapshots.")
    parser.add_argument("--feeds", nargs="+", choices=sorted(config.FEEDS), default=sorted(config.FEEDS))
    parser.add_argument("--raw-dir", type=Path, default=config.RAW_DIR)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    session = build_session()
    results = [ingest_feed(session, feed, config.FEEDS[feed], args.raw_dir) for feed in args.feeds]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
