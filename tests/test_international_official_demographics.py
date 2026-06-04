"""Tests for listing age and years-in-grade calculations."""

from datetime import date

from activityAnalysis.international_listing_seasons import (
    format_listing_reference_july1,
    listing_reference_july1,
)
from activityAnalysis.international_official_demographics import (
    OFFICIAL_AGE_OUT_ON_JULY1,
    age_as_of_listing,
    first_listing_season_at_least_age,
    first_year_credit_july1,
    grade_date_for_tc_prerequisite_tenure,
    grade_date_from_appointment_contexts,
    max_years_for_tc_prerequisite_role,
    years_in_grade_at_listing,
)


def test_listing_reference_july1():
    assert listing_reference_july1(2627) == date(2026, 7, 1)
    assert listing_reference_july1(2728) == date(2027, 7, 1)
    assert format_listing_reference_july1(2627) == "as of July 1, 2026"


def test_age_as_of_listing():
    dob = date(1960, 8, 15)
    assert age_as_of_listing(dob, listing_season_code=2627) == 65
    assert age_as_of_listing(dob, listing_season_code=2728) == 66
    assert age_as_of_listing(None, listing_season_code=2627) is None


def test_first_listing_season_at_least_age_70():
    assert OFFICIAL_AGE_OUT_ON_JULY1 == 70
    # Born June 1956 → age 70 on July 1, 2026 (listing 2627).
    assert first_listing_season_at_least_age(date(1956, 6, 1), 70) == 2627
    # Born August 1956 → age 70 on July 1, 2027 (listing 2728).
    assert first_listing_season_at_least_age(date(1956, 8, 1), 70) == 2728
    assert first_listing_season_at_least_age(None, 70) is None
    # Young official: age-out is far in the future, not limited by projection window.
    assert first_listing_season_at_least_age(date(1990, 3, 15), 70) == 6061


def test_years_in_grade_user_examples():
    achieved = date(2025, 10, 1)
    assert years_in_grade_at_listing(achieved, listing_season_code=2627) == 1
    assert years_in_grade_at_listing(achieved, listing_season_code=2728) == 2

    mid_season = date(2026, 3, 15)
    assert years_in_grade_at_listing(mid_season, listing_season_code=2627) == 1

    on_listing_day = date(2026, 7, 1)
    assert years_in_grade_at_listing(on_listing_day, listing_season_code=2627) == 1
    assert first_year_credit_july1(on_listing_day) == date(2027, 7, 1)


def test_first_year_credit_july1():
    assert first_year_credit_july1(date(2025, 7, 1)) == date(2026, 7, 1)
    assert first_year_credit_july1(date(2026, 6, 30)) == date(2026, 7, 1)


def test_grade_date_for_tc_prerequisite_isu_judge_uses_appointed():
    gd = grade_date_for_tc_prerequisite_tenure(
        date(2022, 7, 1),
        date(2018, 6, 1),
        level_id=16,
        international_level_id=17,
        isu_level_id=16,
    )
    assert gd == date(2018, 6, 1)


def test_max_years_for_tc_prerequisite_role_combines_intl_and_isu_judge():
    rows = [
        {
            "official_id": 1,
            "appointment_type_id": 12,
            "discipline_id": 9,
            "level_id": 17,
            "achieved_date": date(2018, 6, 1),
            "appointed_date": None,
            "active": True,
        },
        {
            "official_id": 1,
            "appointment_type_id": 12,
            "discipline_id": 9,
            "level_id": 16,
            "achieved_date": date(2022, 7, 1),
            "appointed_date": date(2018, 6, 1),
            "active": True,
        },
    ]
    years = max_years_for_tc_prerequisite_role(
        rows,
        listing_season_code=2627,
        appointment_type_id=12,
        discipline_ids=[1, 8, 9],
        international_level_id=17,
        isu_level_id=16,
    )
    assert years >= 4


def test_grade_date_for_collapsed_data_operator():
    """Collapsed IDVO summary uses discipline_id NULL; grade dates live per discipline."""
    contexts = {
        (1, 16, 3): {"achieved_date": date(2022, 5, 1), "appointed_date": None},
        (1, 16, 4): {"achieved_date": date(2024, 8, 1), "appointed_date": None},
    }
    gd = grade_date_from_appointment_contexts(
        contexts, official_id=1, appointment_type_id=16, discipline_id=None
    )
    assert gd == date(2022, 5, 1)
    assert years_in_grade_at_listing(gd, listing_season_code=2627) == 5
