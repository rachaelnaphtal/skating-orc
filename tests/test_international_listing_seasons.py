import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "sqlite:////tmp/international_listing_seasons_tests.db")

from activityAnalysis.international_listing_seasons import (
    REPORT_LISTING_SEASON_DEFAULT,
    competition_year_matches_seasons,
    default_listing_season_code,
    filter_panel_to_season_codes,
    format_listing_reference_july1,
    format_promote_first_eligible_display,
    format_usfs_season_code,
    listing_calendar_year,
    listing_calendar_year_from_season_code,
    listing_reference_july1,
    listing_season_code_from_calendar_year,
    listing_season_codes_for_projection,
    season_codes_preceding_listing,
)


def test_season_codes_preceding_listing():
    assert season_codes_preceding_listing(2627, 3) == [2324, 2425, 2526]
    assert season_codes_preceding_listing(2728, 3) == [2425, 2526, 2627]
    assert season_codes_preceding_listing(2627, 4) == [2223, 2324, 2425, 2526]
    assert season_codes_preceding_listing(2627, 2) == [2425, 2526]


def test_listing_reference_july1():
    assert listing_reference_july1(2627) == date(2026, 7, 1)
    assert listing_reference_july1(2728) == date(2027, 7, 1)
    assert format_listing_reference_july1(2627) == "as of July 1, 2026"


def test_listing_season_codes_for_projection():
    codes = listing_season_codes_for_projection(years_ahead=2)
    assert 2627 in codes
    assert 2728 in codes
    assert 2829 in codes


def test_format_promote_first_eligible_display():
    assert format_promote_first_eligible_display(None, current_listing_season_code=2627) == "—"
    assert format_promote_first_eligible_display(2627, current_listing_season_code=2627) == "2026"
    assert format_promote_first_eligible_display(2728, current_listing_season_code=2627) == "2027"


def test_listing_season_code_mapping():
    assert listing_calendar_year_from_season_code(2627) == 2027
    assert listing_calendar_year_from_season_code(2728) == 2028
    assert listing_season_code_from_calendar_year(2027) == 2627
    assert format_usfs_season_code(2627) == "26-27"
    assert REPORT_LISTING_SEASON_DEFAULT == 2627


def test_listing_calendar_year():
    assert listing_calendar_year(date(2026, 8, 1)) == 2026
    assert listing_calendar_year(date(2026, 3, 1)) == 2026
    assert default_listing_season_code(date(2027, 8, 1)) == 2627
    assert default_listing_season_code(date(2028, 8, 1)) == 2728


def test_filter_panel_to_season_codes():
    panel = pd.DataFrame(
        [
            {"competition_id": 1, "competition_year": 2526},
            {"competition_id": 2, "competition_year": 2627},
            {"competition_id": 3, "competition_year": 2025},
        ]
    )
    filtered = filter_panel_to_season_codes(panel, [2425, 2526])
    assert len(filtered) == 1
    assert set(filtered["competition_id"]) == {1}
    assert competition_year_matches_seasons(2526, [2425, 2526])
    assert not competition_year_matches_seasons(2025, [2425, 2526])
