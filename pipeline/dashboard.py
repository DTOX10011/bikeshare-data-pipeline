"""Export what the dashboard shows from the warehouse, and assemble the site.

Reads the warehouse built by pipeline.transform, writes one data.json file next to a copy
of dashboard/index.html, and that folder is the whole site: it can be opened locally or
published as it is (the dashboard workflow publishes it to GitHub Pages).

Usage:
    python -m pipeline.dashboard                           # data/warehouse.duckdb -> site/
    python -m pipeline.dashboard --warehouse w.duckdb --out site --checks transform/target
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from pipeline import transform

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "dashboard"
PLACEHOLDER = "/*DATA*/"

TIMELINE_HOURS = 72          # the availability and weather charts cover the last three days
RANKING_DAYS = 7             # the empty-station ranking and the map cover the last week
TOP_STATIONS = 10
HEALTH_DAYS = 14


def iso(moment) -> str | None:
    """A warehouse timestamp (UTC without a zone) as ISO 8601 text with the zone stated."""
    if moment is None:
        return None
    return moment.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def rows(con, sql: str, params=()) -> list[dict]:
    cursor = con.execute(sql, params)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def build_data(con, checks: list[transform.CheckResult] | None = None) -> dict:
    """Everything the page shows, as plain JSON-ready values."""
    (latest,) = con.execute("select max(snapshot_at) from marts.fct_station_status").fetchone()
    if latest is None:
        raise ValueError("The warehouse has no station snapshots yet.")

    (first,) = con.execute("select min(snapshot_at) from marts.fct_station_status").fetchone()
    (snapshot_count,) = con.execute("select count(distinct snapshot_at) from marts.fct_station_status").fetchone()

    now = rows(con, """
        select
            count(*) filter (where is_installed and is_renting)         as open_stations,
            count(*) filter (where is_empty)                            as empty_stations,
            count(*) filter (where is_full)                             as full_stations,
            coalesce(sum(num_bikes_available) filter (where is_installed), 0)   as bikes_available,
            coalesce(sum(num_ebikes_available) filter (where is_installed), 0)  as ebikes_available
        from marts.fct_station_status
        where snapshot_at = ?
    """, [latest])[0]

    timeline = rows(con, """
        select
            snapshot_at,
            count(*) filter (where is_installed and is_renting) as open_stations,
            count(*) filter (where is_empty)                    as empty_stations,
            coalesce(sum(num_bikes_available) filter (where is_installed), 0) as bikes_available
        from marts.fct_station_status
        where snapshot_at > ? - to_hours(?)
        group by snapshot_at
        order by snapshot_at
    """, [latest, TIMELINE_HOURS])

    weather = rows(con, """
        select observed_at, temperature_c, precipitation_mm
        from marts.fct_weather
        where observed_at > ? - to_hours(?)
        order by observed_at
    """, [latest, TIMELINE_HOURS])

    # Share of the time each station was empty while it was open. Stations that were open
    # for less than half of the window are left out of the ranking: a handful of snapshots
    # is not enough to call a station "often empty".
    stations = rows(con, """
        with window_status as (
            select
                station_id,
                count(*) filter (where is_installed and is_renting) as open_snapshots,
                count(*) filter (where is_empty)                    as empty_snapshots
            from marts.fct_station_status
            where snapshot_at > ? - to_days(?)
            group by station_id
        )
        select
            s.station_id,
            s.station_name,
            s.latitude,
            s.longitude,
            s.capacity,
            w.open_snapshots,
            w.empty_snapshots,
            w.empty_snapshots / nullif(w.open_snapshots, 0) as share_empty
        from window_status w
        join marts.dim_station s using (station_id)
        where not s.is_inferred
          and s.latitude is not null
          and s.longitude is not null
          and w.open_snapshots > 0
    """, [latest, RANKING_DAYS])

    window_snapshots = max((s["open_snapshots"] for s in stations), default=0)
    enough = [
        s for s in stations
        if s["open_snapshots"] >= max(1, window_snapshots / 2) and s["share_empty"] > 0
    ]
    top = sorted(enough, key=lambda s: (-s["share_empty"], s["station_name"] or ""))[:TOP_STATIONS]

    health = rows(con, """
        select day, status_snapshots, share_of_expected_snapshots, longest_gap_minutes,
               weather_readings, collection_runs, failed_source_results,
               quarantined_rows, unreadable_values
        from quality.dq_daily_summary
        order by day desc
        limit ?
    """, [HEALTH_DAYS])

    check_rows = [
        {"name": c.name, "status": c.status, "rows": c.failures}
        for c in (checks or [])
    ]

    return {
        "generated_at": iso(datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)),
        "coverage": {
            "first_snapshot": iso(first),
            "latest_snapshot": iso(latest),
            "snapshots": snapshot_count,
            "stations": len(stations),
        },
        "now": now,
        "timeline": [
            {
                "t": iso(r["snapshot_at"]),
                "open": r["open_stations"],
                "empty": r["empty_stations"],
                "share_empty": round(r["empty_stations"] / r["open_stations"], 4) if r["open_stations"] else None,
                "bikes": r["bikes_available"],
            }
            for r in timeline
        ],
        "weather": [
            {"t": iso(r["observed_at"]), "temperature_c": r["temperature_c"], "precipitation_mm": r["precipitation_mm"]}
            for r in weather
        ],
        "ranking": {
            "days": RANKING_DAYS,
            "min_open_snapshots": int(max(1, window_snapshots / 2)),
            "stations": [
                {
                    "name": s["station_name"] or s["station_id"],
                    "share_empty": round(s["share_empty"], 4),
                    "open_snapshots": s["open_snapshots"],
                    "capacity": s["capacity"],
                }
                for s in top
            ],
        },
        "map": [
            [round(s["longitude"], 5), round(s["latitude"], 5), round(s["share_empty"], 3), s["station_name"] or s["station_id"]]
            for s in sorted(stations, key=lambda s: s["share_empty"])
        ],
        "health": {
            "days": [
                {**r, "day": r["day"].isoformat()}
                for r in health
            ],
            "checks": check_rows,
        },
    }


def write_site(warehouse: Path, out: Path, checks_dir: Path | None = None) -> Path:
    """Write data.json and copy the page into `out`. Returns the data file's path."""
    checks = transform.check_results(checks_dir) if checks_dir else []
    with duckdb.connect(str(warehouse), read_only=True) as con:
        data = build_data(con, checks)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, separators=(",", ":"), default=str)
    data_file = out / "data.json"
    data_file.write_text(text, encoding="utf-8")
    # The page also carries the data itself, so it opens straight from disk with no server.
    # "</" is escaped so a station name can never close the script tag early.
    page = (TEMPLATE_DIR / "index.html").read_text(encoding="utf-8")
    if PLACEHOLDER not in page:
        raise ValueError(f"{TEMPLATE_DIR / 'index.html'} has no {PLACEHOLDER} placeholder for the data.")
    (out / "index.html").write_text(page.replace(PLACEHOLDER, text.replace("</", "<\\/")), encoding="utf-8")
    return data_file


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Export the dashboard's data and assemble the site.")
    parser.add_argument("--warehouse", type=Path, default=transform.DEFAULT_WAREHOUSE)
    parser.add_argument("--out", type=Path, default=Path("site"))
    parser.add_argument(
        "--checks", type=Path, default=transform.DEFAULT_TARGET,
        help="dbt target folder whose run_results.json holds the latest check results",
    )
    args = parser.parse_args(argv)
    if not args.warehouse.exists():
        print(f"No warehouse at {args.warehouse}. Build it first: python -m pipeline.transform", file=sys.stderr)
        return 2
    data_file = write_site(args.warehouse, args.out, args.checks)
    print(f"Wrote {data_file} ({data_file.stat().st_size:,} bytes). Open {args.out / 'index.html'} to view.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
