import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "sqlite:////tmp/international_listing_seasons_tests.db")

from activityAnalysis.international_listing_seasons import (
    REPORT_LISTING_SEASON_DEFAULT,
    calendar_year_for_listing_milestone,
    competition_year_matches_seasons,
    current_age_bin_label,
    default_listing_season_code,
    filter_panel_to_season_codes,
    format_age_out_display,
    format_listing_reference_july1,
    format_promote_first_eligible_display,
    format_usfs_season_code,
    histogram_counts_by_current_age_bins,
    histogram_counts_by_year_bins,
    isu_season_codes_preceding_july1,
    listing_calendar_year,
    listing_calendar_year_from_season_code,
    listing_reference_july1,
    listing_season_code_from_calendar_year,
    listing_season_codes_for_projection,
    season_codes_preceding_listing,
    usfs_season_code_ending_in_calendar_year,
    usfs_season_code_for_date,
)


def test_season_codes_preceding_listing():
    assert season_codes_preceding_listing(2627, 3) == [2324, 2425, 2526]
    assert season_codes_preceding_listing(2728, 3) == [2425, 2526, 2627]
    assert season_codes_preceding_listing(2627, 4) == [2223, 2324, 2425, 2526]
    assert season_codes_preceding_listing(2627, 2) == [2425, 2526]
    assert 2627 not in season_codes_preceding_listing(2627, 4)


def test_isu_season_codes_preceding_july1():
    assert isu_season_codes_preceding_july1(2026, 3) == [2324, 2425, 2526]
    assert isu_season_codes_preceding_july1(2026, 4) == [2223, 2324, 2425, 2526]
    assert isu_season_codes_preceding_july1(2026, 2) == [2425, 2526]
    assert 2627 not in isu_season_codes_preceding_july1(2026, 4)


def test_usfs_season_code_ending_in_calendar_year():
    assert usfs_season_code_ending_in_calendar_year(2026) == 2526
    assert usfs_season_code_ending_in_calendar_year(2027) == 2627


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


def test_calendar_year_for_listing_milestone():
    assert calendar_year_for_listing_milestone(None, current_listing_season_code=2627) is None
    assert calendar_year_for_listing_milestone(2728, current_listing_season_code=2627) == 2027
    assert calendar_year_for_listing_milestone(2627, current_listing_season_code=2627) == 2026


def test_format_age_out_display():
    assert format_age_out_display(2728, current_listing_season_code=2627) == "2027"


def test_current_age_bin_label_aligned_to_age_out():
    assert current_age_bin_label(72, age_out_at=70) == "70+"
    assert current_age_bin_label(70, age_out_at=70) == "70+"
    assert current_age_bin_label(67, age_out_at=70) == "65–69"
    assert current_age_bin_label(65, age_out_at=70) == "65–69"
    assert current_age_bin_label(62, age_out_at=70) == "60–64"
    assert current_age_bin_label(60, age_out_at=70) == "60–64"


def test_histogram_counts_by_current_age_bins():
    ages = pd.Series([67, 68, 62, 71, None])
    hist = histogram_counts_by_current_age_bins(ages, bin_width=5, age_out_at=70)
    assert list(hist["bin_label"]) == ["70+", "65–69", "60–64"]
    assert list(hist["count"]) == [1, 2, 1]


def test_histogram_counts_by_year_bins():
    years = pd.Series([2024, 2026, 2027, 2028, None])
    hist = histogram_counts_by_year_bins(years, bin_width=5)
    assert list(hist["bin_label"]) == ["2020–2024", "2025–2029"]
    assert list(hist["count"]) == [1, 3]
    assert histogram_counts_by_year_bins(pd.Series([None, None])).empty

    single = histogram_counts_by_year_bins(pd.Series([2026, 2026, 2027]), bin_width=1)
    assert list(single["bin_label"]) == ["2026", "2027"]
    assert list(single["count"]) == [2, 1]


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


def test_usfs_season_code_for_date():
    assert usfs_season_code_for_date(date(2025, 9, 1)) == 2526
    assert usfs_season_code_for_date(date(2026, 6, 15)) == 2526
    assert usfs_season_code_for_date(date(2026, 7, 1)) == 2627


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
