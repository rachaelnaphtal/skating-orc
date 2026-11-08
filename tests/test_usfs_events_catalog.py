"""Results-site catalog -> IJS Live leaderboard links."""

from datetime import date

from ijs_results_urls import results_url_for_storage
from usfs_events_catalog import (
    catalog_row,
    current_season_label,
    ijs_leaderboard_entry,
    normalize_season_label,
    season_code_from_label,
    select_catalog_rows,
)


def test_current_season_turns_over_in_july():
    assert current_season_label(date(2026, 11, 7)) == "2026-27"
    assert current_season_label(date(2026, 3, 1)) == "2025-26"


def test_season_label_accepts_catalog_dash_and_code():
    assert normalize_season_label("2026–27") == "2026-27"
    assert normalize_season_label("2627") == "2026-27"
    assert season_code_from_label("2026-27") == "2627"


def test_view_results_id_maps_to_published_ijs_url():
    row = catalog_row(
        {
            "id": 37256,
            "name": "2026 Fall Madison Open",
            "start": "2026-10-23",
            "end": "2026-10-25",
            "city": "Madison",
            "region": "WI",
            "status": "final",
            "season": "2026–27",
            "path": "2026/37256",
            "url": "https://ijs.usfigureskating.org/leaderboard/results/2026/37256/index.asp",
        }
    )
    assert row is not None
    assert row["url"] == (
        "https://ijs.usfigureskating.org/leaderboard/results/2026/37256/index.asp"
    )
    assert row["year"] == "2026"
    assert row["competition_id"] == "37256"
    assert row["location"] == "Madison, WI"
    assert row["season_year"] == "2627"
    assert row["http_status"] == "200"
    assert row["probe_error"] == ""


def test_directory_url_is_stored_as_index_htm():
    """FSM internationals 404 on index.asp. The loader must keep index.htm."""
    row = catalog_row(
        {
            "id": 37263,
            "name": "2026 Cranberry Cup International",
            "start": "2026-08-06",
            "end": "2026-08-09",
            "city": "Norwood",
            "region": "MA",
            "status": "final",
            "season": "2026-27",
            "url": "https://ijs.usfigureskating.org/leaderboard/results/2026/37263/",
        }
    )
    assert row is not None
    assert row["url"].endswith("/2026/37263/index.htm")
    assert results_url_for_storage(row["url"]) == row["url"]


def test_ijs_year_wins_when_results_site_path_year_differs():
    parsed = ijs_leaderboard_entry(
        "https://ijs.usfigureskating.org/leaderboard/results/2011/68087/index.asp"
    )
    assert parsed is not None
    year, competition_id, url = parsed
    assert (year, competition_id) == ("2011", "68087")
    assert url.endswith("/2011/68087/index.asp")


def test_slug_leaderboard_url_is_kept():
    parsed = ijs_leaderboard_entry(
        "https://ijs.usfigureskating.org/leaderboard/results/2014/2014_sw_regionals/index.asp"
    )
    assert parsed is not None
    assert parsed[1] == "2014_sw_regionals"
    assert parsed[2].endswith("/2014/2014_sw_regionals/index.asp")


def test_select_keeps_final_current_season_and_skips_events_without_ijs_url():
    events = [
        {
            "id": 37256,
            "name": "2026 Fall Madison Open",
            "start": "2026-10-23",
            "end": "2026-10-25",
            "city": "Madison",
            "region": "WI",
            "status": "final",
            "season": "2026–27",
            "url": "https://ijs.usfigureskating.org/leaderboard/results/2026/37256/index.asp",
        },
        {
            "id": 36414,
            "name": "2027 Eastern Sectional Singles & U.S. Pairs Final",
            "start": "2026-11-03",
            "end": "2026-11-07",
            "city": "Norwood",
            "region": "MA",
            "status": "live",
            "season": "2026–27",
            "path": "2026/36414",
        },
        {
            "id": 1,
            "name": "Older",
            "start": "2025-10-01",
            "end": "2025-10-01",
            "status": "final",
            "season": "2025–26",
            "url": "https://ijs.usfigureskating.org/leaderboard/results/2025/1/index.asp",
        },
        {
            "id": 2,
            "name": "Still upcoming",
            "start": "2026-12-01",
            "end": "2026-12-01",
            "status": "upcoming",
            "season": "2026–27",
            "url": "https://ijs.usfigureskating.org/leaderboard/results/2026/2/index.asp",
        },
        {
            "id": 3,
            "name": "Final on the new site only",
            "start": "2026-09-01",
            "end": "2026-09-01",
            "status": "final",
            "season": "2026–27",
            "path": "2026/3",
        },
    ]
    rows, skipped = select_catalog_rows(
        events,
        seasons={"2026-27"},
        statuses={"final"},
    )
    assert skipped == 1
    assert [row["competition_id"] for row in rows] == ["37256"]
