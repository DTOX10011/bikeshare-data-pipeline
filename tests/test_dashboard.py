"""Tests for the dashboard export: what data the page gets, and that the page works."""

import json

import duckdb
import pytest

from fake_collection import STATIONS, at, collect, every_15_minutes, healthy_row
from pipeline import dashboard, transform


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    """Two hours of collection where S1 is always empty and S2 is empty half the time."""
    root = tmp_path_factory.mktemp("dashboard")
    for i, moment in enumerate(every_15_minutes(at(8), at(10))):
        rows = [healthy_row(s, moment) for s in STATIONS]
        rows[0]["num_bikes_available"] = rows[0]["num_ebikes_available"] = 0
        if i % 2:
            rows[1]["num_bikes_available"] = rows[1]["num_ebikes_available"] = 0
        rows[2]["is_renting"] = 0  # S3 is closed: it never counts as empty
        rows[2]["num_bikes_available"] = rows[2]["num_ebikes_available"] = 0
        collect(root, moment, rows=rows)
    target = root / "target"
    assert transform.run_dbt(root / "raw", root / "warehouse.duckdb", quiet=True, target_path=target)
    return root


@pytest.fixture(scope="module")
def data(warehouse):
    with duckdb.connect(str(warehouse / "warehouse.duckdb"), read_only=True) as con:
        return dashboard.build_data(con, transform.check_results(warehouse / "target"))


def test_right_now_figures_come_from_the_newest_snapshot(data):
    assert data["coverage"]["latest_snapshot"] == "2026-10-05T10:00:00Z"
    assert data["coverage"]["snapshots"] == 9
    # S3 is closed; S1 is empty; S2 is empty at even-numbered snapshots, and 10:00 is the 9th (index 8).
    assert data["now"]["open_stations"] == 9
    assert data["now"]["empty_stations"] == 1


def test_timeline_has_one_point_per_snapshot_with_the_share_empty(data):
    assert len(data["timeline"]) == 9
    assert [p["empty"] for p in data["timeline"][:2]] == [1, 2]
    assert data["timeline"][1]["share_empty"] == round(2 / 9, 4)


def test_ranking_puts_the_always_empty_station_first_and_skips_closed_ones(data):
    ranking = data["ranking"]["stations"]
    assert ranking[0] == {"name": "Station S1", "share_empty": 1.0, "open_snapshots": 9, "capacity": 20}
    assert ranking[1]["name"] == "Station S2"
    assert ranking[1]["share_empty"] == round(4 / 9, 4)
    assert "Station S3" not in [s["name"] for s in ranking]


def test_map_lists_every_open_station_from_never_to_most_often_empty(data):
    shares = [point[2] for point in data["map"]]
    assert len(shares) == 9
    assert shares == sorted(shares)
    assert data["map"][-1][3] == "Station S1"


def test_health_reports_each_day_and_the_check_results(data):
    (day,) = data["health"]["days"]
    assert day["day"] == "2026-10-05"
    assert day["status_snapshots"] == 9
    statuses = {c["name"]: c["status"] for c in data["health"]["checks"]}
    assert statuses["unique_fct_station_status_station_snapshot_key"] == "pass"
    assert statuses["assert_station_status_is_fresh"] == "warn"  # the fake day is long past


def test_site_is_one_page_with_the_data_inside_and_a_copy_beside_it(warehouse, tmp_path):
    data_file = dashboard.write_site(warehouse / "warehouse.duckdb", tmp_path, warehouse / "target")

    page = (tmp_path / "index.html").read_text(encoding="utf-8")
    assert "/*DATA*/" not in page
    assert json.loads(data_file.read_text(encoding="utf-8"))["coverage"]["snapshots"] == 9


def test_text_in_the_data_cannot_close_the_script_tag(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "build_data", lambda con, checks: {"name": "</script><b>x</b>"})
    duckdb.connect(str(tmp_path / "w.duckdb")).close()

    dashboard.write_site(tmp_path / "w.duckdb", tmp_path / "site")

    page = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    script = page.split('<script id="data" type="application/json">', 1)[1].split("</script>", 1)[0]
    assert json.loads(script) == {"name": "</script><b>x</b>"}


def test_an_empty_warehouse_is_reported(tmp_path):
    with duckdb.connect(str(tmp_path / "w.duckdb")) as con:
        con.execute("create schema marts; create table marts.fct_station_status (snapshot_at timestamp)")
        with pytest.raises(ValueError, match="no station snapshots"):
            dashboard.build_data(con)


def test_command_needs_a_built_warehouse(tmp_path, capsys):
    assert dashboard.main(["--warehouse", str(tmp_path / "missing.duckdb"), "--out", str(tmp_path / "site")]) == 2
    assert "Build it first" in capsys.readouterr().err


def test_page_renders_and_shows_the_figures(warehouse, tmp_path):
    """Opens the built page in a real browser. Skipped where no browser is installed."""
    playwright = pytest.importorskip("playwright.sync_api")
    dashboard.write_site(warehouse / "warehouse.duckdb", tmp_path, warehouse / "target")
    try:
        with playwright.sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1200, "height": 900})
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto((tmp_path / "index.html").as_uri())
            page.wait_for_selector(".hero-figure")
            hero = page.inner_text(".hero-figure")
            bars = page.locator("svg [role=listitem]").count()
            dots = page.locator(".chart svg circle").count()
            page.locator(".chart svg rect[tabindex]").first.focus()
            tooltip = page.inner_text(".tooltip.on")
            browser.close()
    except Exception as exc:  # no browser binary in this environment
        if "Executable doesn't exist" in str(exc):
            pytest.skip("no browser installed")
        raise
    assert errors == []
    assert hero == "1"
    assert bars == 2           # S1 and S2; the stations never empty are left out of the bars
    assert dots >= 9           # every open station on the map
    assert "of open stations empty" in tooltip
