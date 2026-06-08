import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["DATABASE_URL"] = "sqlite:////tmp/international_requirements_tests.db"

_mock_lad = MagicMock()
_mock_lad.activity_database_is_postgresql.return_value = False
_mock_lad.engine = MagicMock()
_mock_lad.calendar_years_for_usfs_season_codes.return_value = [2024, 2025]
sys.modules["activityAnalysis.load_activity_data"] = _mock_lad
sys.modules["load_activity_data"] = _mock_lad

_mock_iod = MagicMock()
_mock_iod.COUNTABLE_SEGMENT_LEVELS = frozenset({"Junior", "Senior"})
_mock_iod.DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL = "All"
_mock_iod.INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID = 16
_mock_iod._discipline_match_sql.return_value = ("", {})
def _nullable_int_for_sql_test(x):
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


_mock_iod._nullable_int_for_sql.side_effect = _nullable_int_for_sql_test
_mock_iod.national_segment_appointment_type_id.return_value = 4
sys.modules["activityAnalysis.international_officials_data"] = _mock_iod
sys.modules["international_officials_data"] = _mock_iod

import pandas as pd

from activityAnalysis import international_requirements as ir


def test_appointment_context_from_batch_collapsed_idvo():
    contexts = {
        (1, 16, 9): {"level_id": ir.DIRECTORY_LEVEL_ID_INTERNATIONAL, "level_name": "International"},
        (1, 16, 2): {"level_id": ir.DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP, "level_name": "ISU"},
    }
    ctx = ir._appointment_context_from_batch(contexts, 1, 16, None)
    assert ctx["level_id"] == ir.DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP

    ctx_sp = ir._appointment_context_from_batch(contexts, 1, 16, 9)
    assert ctx_sp["level_id"] == ir.DIRECTORY_LEVEL_ID_INTERNATIONAL


def test_isu_season_codes_preceding_july1():
    assert ir.isu_season_codes_preceding_july1(2026, 3) == [2324, 2425, 2526]
    assert ir.isu_season_codes_preceding_july1(2026, 4) == [2223, 2324, 2425, 2526]


def test_listing_calendar_year():
    assert ir.listing_calendar_year(date(2026, 8, 1)) == 2026
    assert ir.listing_calendar_year(date(2026, 3, 1)) == 2026


def test_championship_or_olympic_detection():
    assert ir._is_championship_or_olympic(15, "World Championships")
    assert ir._is_championship_or_olympic(16, "Olympic Winter Games Milano 2026")
    assert not ir._is_championship_or_olympic(17, "Lake Placid International")


def test_appointment_sport():
    assert ir._appointment_sport(2) == "synchronized"
    assert ir._appointment_sport(9) == "figure"
    assert ir._appointment_sport(None) == "figure"


def test_parse_int_list():
    assert ir._parse_int_list("1, 4, 11") == [1, 4, 11]
    assert ir._parse_int_list(None) is None
    assert ir._parse_int_list([1, 2]) == [1, 2]


def test_directory_level_listing_tier():
    assert ir.directory_listing_tier_for_level("International") == "international"
    assert ir.directory_listing_tier_for_level("ISU Championship") == "isu"
    assert ir.directory_listing_tier_for_level("International", level_id=17) == "international"
    assert ir.directory_listing_tier_for_level("ISU Championship", level_id=16) == "isu"
    assert ir.directory_listing_tier_for_level("International", level_id=16) == "isu"
    assert ir.listing_tier_display_label("isu") == "ISU Championship"
    assert ir.listing_tier_display_label("international") == "International"
    assert ir.is_isu_directory_level("ISU Championship")
    assert not ir.is_isu_directory_level("International")
    assert ir.is_isu_appointment_level("ISU Championship", level_id=16)
    assert not ir.is_isu_appointment_level("International", level_id=17)
    assert ir.is_isu_appointment_level("International", level_id=16)
    assert not ir.should_evaluate_promote_requirements(
        1, 12, 9, "ISU Championship", 16, isu_level_id=16, isu_listing_keys=set()
    )
    assert not ir.should_evaluate_promote_requirements(
        1,
        12,
        9,
        "International",
        17,
        isu_level_id=16,
        isu_listing_keys={(1, 12, 9)},
    )
    assert ir.should_evaluate_promote_requirements(
        1, 12, 9, "International", 17, isu_level_id=16, isu_listing_keys=set()
    )


def test_rule_set_applies_tc_promote_pairs_discipline():
    """TC promote (414.3.c) is keyed to Pairs directory id 8, not Singles/Pairs id 9."""
    rule_set = pd.Series(
        {
            "appointment_type_id": 15,
            "sport": "figure",
            "discipline_id": 8,
            "directory_level_id": pd.NA,
            "listing_tier": "international",
        }
    )
    applies, _ = ir._rule_set_applies(
        rule_set,
        appointment_type_id=15,
        directory_discipline_id=8,
        appointment_level_id=17,
        international_level_id=17,
        isu_level_id=16,
        purpose="promote",
        official_id=1,
    )
    assert applies

    applies_sp, reason = ir._rule_set_applies(
        rule_set,
        appointment_type_id=15,
        directory_discipline_id=9,
        appointment_level_id=17,
        international_level_id=17,
        isu_level_id=16,
        purpose="promote",
        official_id=1,
    )
    assert not applies_sp
    assert reason == "discipline mismatch"
    assert ir._competition_matches_scope(15, False, (15, 16, 17), include_qualifying_national=False)
    assert not ir._competition_matches_scope(4, False, (15, 16, 17), include_qualifying_national=False)
    assert ir._competition_matches_scope(4, True, (15, 16, 17), include_qualifying_national=True)
    assert not ir._competition_matches_scope(12, True, (15, 16, 17), include_qualifying_national=True)

    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "competition_name": "Worlds",
                "segment_level": "Senior",
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_type_id": 4,
                "competition_qualifying": True,
                "competition_name": "US Champs",
                "segment_level": "Senior",
            },
            {
                "competition_id": 3,
                "competition_year": 2526,
                "competition_type_id": 4,
                "competition_qualifying": True,
                "competition_name": "Sectionals",
                "segment_level": "Novice",
            },
        ]
    )
    assert (
        ir._competition_count_from_panel(
            panel,
            season_codes=[2526],
            competition_type_ids=(15, 16, 17),
            segment_levels=frozenset({"Junior", "Senior"}),
            include_qualifying_national=False,
        )
        == 1
    )
    assert (
        ir._competition_count_from_panel(
            panel,
            season_codes=[2526],
            competition_type_ids=(15, 16, 17),
            segment_levels=frozenset({"Junior", "Senior"}),
            include_qualifying_national=True,
        )
        == 2
    )


def test_competition_alternatives_metric():
    config = {
        "alternatives": [
            {"label": "2 International", "requirements": [{"scope": "international_all", "min": 2}]},
            {"label": "1 ISU Event", "requirements": [{"scope": "isu_event", "min": 1}]},
            {
                "label": "1 International + 1 National",
                "requirements": [
                    {"scope": "international_competition", "min": 1},
                    {"scope": "national_qualifying", "min": 1},
                ],
            },
        ]
    }
    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
            },
        ]
    )
    met, via, detail = ir._evaluate_competition_alternatives(
        panel, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met and via == "2 International"

    panel_isu = pd.DataFrame(
        [
            {
                "competition_id": 3,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
            },
        ]
    )
    met, via, _ = ir._evaluate_competition_alternatives(
        panel_isu, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met and via == "1 ISU Event"

    panel_mixed = pd.DataFrame(
        [
            {
                "competition_id": 4,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
            },
            {
                "competition_id": 5,
                "competition_year": 2526,
                "competition_type_id": 4,
                "competition_qualifying": True,
                "segment_level": "Senior",
            },
        ]
    )
    met, via, _ = ir._evaluate_competition_alternatives(
        panel_mixed, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met and via == "1 International + 1 National"


def test_competition_alternatives_isu_events_count_as_international():
    """Figure ISU TC/TS maintain: 2 International includes ISU Events (types 15–16)."""
    config = {
        "alternatives": [
            {
                "label": "2 International Competitions",
                "requirements": [{"scope": "international_all", "min": 2}],
            },
            {
                "label": "1 ISU Event + 1 National",
                "requirements": [
                    {"scope": "isu_event", "min": 1},
                    {"scope": "national_qualifying", "min": 1},
                ],
            },
        ]
    }
    panel = pd.DataFrame(
        [
            {
                "competition_id": i,
                "competition_year": 2526,
                "competition_type_id": 16,
                "competition_qualifying": False,
                "segment_level": "Senior",
            }
            for i in (1, 2, 3)
        ]
    )
    met, via, detail = ir._evaluate_competition_alternatives(
        panel, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met
    assert via == "2 International Competitions"
    assert "3/2 International" in detail


def test_format_competition_alternatives_detail():
    lines = ir.format_competition_alternatives_detail(
        "Need one of: 2 International: 0/2 International; 1 ISU Event + 1 National: 3/1 ISU Event, 0/1 National",
        met=False,
    )
    assert len(lines) == 2
    assert lines[0].startswith("2 International:")


def test_isu_championship_scope():
    assert ir._row_matches_competition_scope(15, False, "isu_championship")
    assert not ir._row_matches_competition_scope(16, False, "isu_championship")
    assert ir._row_matches_competition_scope(16, False, "international_all")


def test_role_ids_for_metric_competition_alternatives():
    assert ir._role_ids_for_metric("competition_alternatives", ()) is None
    assert ir._role_ids_for_metric("competition_alternatives", (1, 11)) == (1, 11)


def test_competition_alternatives_tc_branch_without_role_ids():
    """Unscoped TC branch must not count Judge panel work when Judge branch is role-scoped."""
    config = {
        "alternatives": [
            {
                "label": "Judge",
                "role_ids": [1],
                "requirements": [
                    {"scope": "international_all", "min": 3},
                    {"scope": "isu_championship", "min": 1},
                ],
            },
            {
                "label": "TC (Technical Committee members)",
                "requirements": [
                    {"scope": "international_all", "min": 3},
                    {"scope": "isu_championship", "min": 1},
                ],
            },
        ]
    }
    panel = pd.DataFrame(
        [
            {
                "competition_id": i,
                "competition_year": 2526,
                "competition_type_id": 15 if i == 1 else 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            }
            for i in (1, 2, 3, 4)
        ]
    )
    met, via, _ = ir._evaluate_competition_alternatives(
        panel, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met and via == "Judge"


def test_competition_alternatives_role_ids():
    config = {
        "alternatives": [
            {
                "label": "Judge",
                "role_ids": [1],
                "requirements": [
                    {"scope": "international_all", "min": 2},
                    {"scope": "isu_championship", "min": 1},
                ],
            },
            {
                "label": "TC",
                "role_ids": [11],
                "requirements": [
                    {"scope": "international_all", "min": 2},
                ],
            },
        ]
    }
    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            },
            {
                "competition_id": 3,
                "competition_year": 2526,
                "competition_type_id": 16,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            },
            {
                "competition_id": 4,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 11,
            },
        ]
    )
    met, via, _ = ir._evaluate_competition_alternatives(
        panel, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met and via == "Judge"


def test_international_including_championship_counts_same_events():
    """≥3 International incl. ≥1 ISU Championship — not four separate competitions."""
    config = {
        "alternatives": [
            {
                "label": "Judge",
                "role_ids": [1],
                "requirements": [
                    {"scope": "international_all", "min": 3},
                    {"scope": "isu_championship", "min": 1},
                ],
            }
        ]
    }
    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            },
            {
                "competition_id": 3,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            },
        ]
    )
    met, via, detail = ir._evaluate_competition_alternatives(
        panel, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met and via == "Judge"
    assert "among those" in detail
    assert "need distinct competitions" not in detail

    two_intl_one_champ = panel.iloc[:2]
    met2, _, detail2 = ir._evaluate_competition_alternatives(
        two_intl_one_champ,
        config,
        season_codes=[2526],
        segment_levels=frozenset({"Junior", "Senior"}),
    )
    assert not met2
    assert "2/3 International" in detail2


def test_competition_alternatives_requires_distinct_competitions():
    config = {
        "alternatives": [
            {
                "label": "1 ISU Event + 1 International",
                "requirements": [
                    {"scope": "isu_event", "min": 1},
                    {"scope": "international_all", "min": 1},
                ],
            }
        ]
    }
    one_isu = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 11,
            }
        ]
    )
    met, _, _ = ir._evaluate_competition_alternatives(
        one_isu, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert not met

    two_comps = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 11,
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 11,
            },
        ]
    )
    met, via, _ = ir._evaluate_competition_alternatives(
        two_comps, config, season_codes=[2526], segment_levels=frozenset({"Junior", "Senior"})
    )
    assert met and via == "1 ISU Event + 1 International"


def test_judge_promote_isu_singles_pairs():
    config = {
        "min_competitions": 4,
        "required": [
            {"kind": "segment_level", "level": "Senior", "min_competitions": 1},
            {"kind": "segment_level", "level": "Junior", "min_competitions": 1},
            {"kind": "segment_discipline_type_id", "discipline_type_id": 2, "min_competitions": 1},
            {"kind": "scope", "scope": "isu_event", "min_competitions": 1, "last_season_only": True},
        ],
    }
    season_codes = [2223, 2324, 2425, 2526]
    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2324,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "segment_discipline_type_id": 1,
            },
            {
                "competition_id": 2,
                "competition_year": 2425,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Junior",
                "segment_discipline_type_id": 1,
            },
            {
                "competition_id": 3,
                "competition_year": 2425,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "segment_discipline_type_id": 2,
            },
            {
                "competition_id": 4,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "segment_discipline_type_id": 1,
            },
        ]
    )
    met, detail = ir._evaluate_judge_promote_isu(
        panel,
        config,
        season_codes=season_codes,
        segment_levels=frozenset({"Junior", "Senior"}),
        competition_type_ids=(15, 16, 17),
        include_qualifying_national=False,
    )
    assert met, detail

    panel_no_isu_last = panel[panel["competition_id"] != 4]
    met, detail = ir._evaluate_judge_promote_isu(
        panel_no_isu_last,
        config,
        season_codes=season_codes,
        segment_levels=frozenset({"Junior", "Senior"}),
        competition_type_ids=(15, 16, 17),
        include_qualifying_national=False,
    )
    assert not met
    assert "ISU Event (last season)" in detail


def test_tc_ts_promote_isu():
    config = {"min_competitions": 3, "min_international_competition": 1}
    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 4,
                "competition_qualifying": True,
                "segment_level": "Senior",
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_type_id": 4,
                "competition_qualifying": True,
                "segment_level": "Senior",
            },
            {
                "competition_id": 3,
                "competition_year": 2526,
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
            },
        ]
    )
    met, detail = ir._evaluate_tc_ts_promote_isu(
        panel,
        config,
        season_codes=[2526],
        segment_levels=frozenset({"Junior", "Senior"}),
        competition_type_ids=(15, 16, 17),
        include_qualifying_national=True,
    )
    assert met, detail

    panel_no_intl = panel[panel["competition_type_id"] != 17]
    met, _ = ir._evaluate_tc_ts_promote_isu(
        panel_no_intl,
        config,
        season_codes=[2526],
        segment_levels=frozenset({"Junior", "Senior"}),
        competition_type_ids=(15, 16, 17),
        include_qualifying_national=True,
    )
    assert not met


def test_years_in_grade_promote_metrics():
    listing = 2627
    rows = [
        {
            "official_id": 1,
            "appointment_type_id": 12,
            "discipline_id": 9,
            "level_id": 17,
            "achieved_date": date(2021, 6, 1),
            "appointed_date": None,
            "active": True,
        },
        {
            "official_id": 1,
            "appointment_type_id": 12,
            "discipline_id": 9,
            "level_id": 16,
            "achieved_date": date(2020, 7, 1),
            "appointed_date": None,
            "active": True,
        },
        {
            "official_id": 1,
            "appointment_type_id": 13,
            "discipline_id": 9,
            "level_id": 17,
            "achieved_date": date(2022, 3, 1),
            "appointed_date": None,
            "active": True,
        },
        {
            "official_id": 1,
            "appointment_type_id": 15,
            "discipline_id": 8,
            "level_id": 17,
            "achieved_date": date(2021, 8, 1),
            "appointed_date": None,
            "active": True,
        },
        {
            "official_id": 1,
            "appointment_type_id": 14,
            "discipline_id": 8,
            "level_id": 16,
            "achieved_date": date(2022, 1, 1),
            "appointed_date": None,
            "active": True,
        },
    ]
    from activityAnalysis.international_listing_seasons import (
        format_promote_first_eligible_display,
    )
    from activityAnalysis.international_official_demographics import (
        first_listing_season_meeting_min_years_on_grade_date,
        max_years_for_appointment_criteria,
        related_discipline_ids_for_tc_prerequisite,
        years_in_grade_at_listing,
    )

    assert related_discipline_ids_for_tc_prerequisite(1) == [1, 8, 9]
    assert related_discipline_ids_for_tc_prerequisite(8) == [1, 8, 9]
    assert related_discipline_ids_for_tc_prerequisite(2) == [2]
    assert (
        max_years_for_appointment_criteria(
            rows,
            listing_season_code=listing,
            appointment_type_id=12,
            level_id=16,
        )
        >= 4
    )
    assert (
        max_years_for_appointment_criteria(
            rows,
            listing_season_code=listing,
            appointment_type_id=13,
            level_id=17,
            discipline_ids=[9],
        )
        >= 4
    )
    tc_ctx = {"achieved_date": date(2021, 8, 1), "appointed_date": None}
    assert ir._years_in_grade_for_current_appointment(tc_ctx, listing_season_code=listing) >= 4
    assert (
        first_listing_season_meeting_min_years_on_grade_date(date(2021, 6, 1), 4) == 2627
    )
    assert format_promote_first_eligible_display(2627, current_listing_season_code=2627) == "2026"
    assert format_promote_first_eligible_display(2728, current_listing_season_code=2627) == "2027"
    actual, _ = ir._evaluate_years_tc_prerequisite(
        rows,
        None,
        directory_discipline_id=8,
        listing_season_code=listing,
        international_level_id=17,
        isu_level_id=16,
    )
    assert actual >= 4


def test_tc_prerequisite_counts_isu_judge_from_appointed_date():
    """ISU Judge: appointed = first international; achieved = ISU only — count from appointed."""
    listing = 2627
    isu_only = [
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
    actual, detail = ir._evaluate_years_tc_prerequisite(
        isu_only,
        '{"related_discipline_ids": [1, 8, 9]}',
        directory_discipline_id=8,
        listing_season_code=listing,
        international_level_id=17,
        isu_level_id=16,
    )
    assert actual >= 4
    assert "Judge" in detail
    achieved_only, _ = ir._evaluate_years_tc_prerequisite(
        [
            {
                "official_id": 1,
                "appointment_type_id": 12,
                "discipline_id": 9,
                "level_id": 16,
                "achieved_date": date(2022, 7, 1),
                "appointed_date": None,
                "active": True,
            },
        ],
        '{"related_discipline_ids": [1, 8, 9]}',
        directory_discipline_id=8,
        listing_season_code=listing,
        international_level_id=17,
        isu_level_id=16,
    )
    assert achieved_only < actual


def test_singles_tc_prerequisite_judge_singles_pairs_ts_singles_only():
    """Singles TC: Judge/Referee credit Singles/Pairs; ISU TS credits Singles discipline only."""
    listing = 2627
    rows = [
        {
            "official_id": 1,
            "appointment_type_id": 12,
            "discipline_id": 9,
            "level_id": 17,
            "achieved_date": date(2020, 7, 1),
            "appointed_date": None,
            "active": True,
        },
        {
            "official_id": 1,
            "appointment_type_id": 14,
            "discipline_id": 8,
            "level_id": 16,
            "achieved_date": date(2020, 7, 1),
            "appointed_date": None,
            "active": True,
        },
    ]
    actual, _ = ir._evaluate_years_tc_prerequisite(
        rows,
        None,
        directory_discipline_id=1,
        listing_season_code=listing,
        international_level_id=17,
        isu_level_id=16,
    )
    assert actual >= 4

    ts_only_pairs, _ = ir._evaluate_years_tc_prerequisite(
        [
            {
                "official_id": 1,
                "appointment_type_id": 14,
                "discipline_id": 8,
                "level_id": 16,
                "achieved_date": date(2020, 7, 1),
                "appointed_date": None,
                "active": True,
            },
        ],
        None,
        directory_discipline_id=1,
        listing_season_code=listing,
        international_level_id=17,
        isu_level_id=16,
    )
    assert ts_only_pairs == 0


def test_rule_set_applies_singles_tc_promote():
    rule_set = pd.Series(
        {
            "appointment_type_id": 15,
            "sport": "figure",
            "discipline_id": 1,
            "directory_level_id": pd.NA,
            "listing_tier": "international",
        }
    )
    applies, _ = ir._rule_set_applies(
        rule_set,
        appointment_type_id=15,
        directory_discipline_id=1,
        appointment_level_id=17,
        international_level_id=17,
        isu_level_id=16,
        purpose="promote",
        official_id=1,
    )
    assert applies


def test_rule_set_applies_singles_ts_promote():
    rule_set = pd.Series(
        {
            "appointment_type_id": 14,
            "sport": "figure",
            "discipline_id": 1,
            "directory_level_id": pd.NA,
            "listing_tier": "international",
        }
    )
    applies, _ = ir._rule_set_applies(
        rule_set,
        appointment_type_id=14,
        directory_discipline_id=1,
        appointment_level_id=17,
        international_level_id=17,
        isu_level_id=16,
        purpose="promote",
        official_id=1,
    )
    assert applies


def test_tc_ts_promote_isu_isu_event_counts_as_international_competition():
    """ISU Championship (15) satisfies min_international_competition per Rule 411."""
    config = {"min_competitions": 3, "min_international_competition": 1}
    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_type_id": 4,
                "competition_qualifying": True,
                "segment_level": "Senior",
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_type_id": 4,
                "competition_qualifying": True,
                "segment_level": "Senior",
            },
            {
                "competition_id": 3,
                "competition_year": 2526,
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
            },
        ]
    )
    met, detail = ir._evaluate_tc_ts_promote_isu(
        panel,
        config,
        season_codes=[2526],
        segment_levels=frozenset({"Junior", "Senior"}),
        competition_type_ids=(15, 16, 17),
        include_qualifying_national=True,
    )
    assert met, detail
    assert "1/1 International Competition(s)" in detail


def test_qualifying_competitions_combined_roles():
    panel = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "competition_year": 2526,
                "competition_name": "Worlds",
                "competition_type_id": 15,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 4,
            },
            {
                "competition_id": 2,
                "competition_year": 2526,
                "competition_name": "GP USA",
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_level": "Senior",
                "national_appointment_type_id": 1,
            },
            {
                "competition_id": 3,
                "competition_year": 2526,
                "competition_name": "Sectionals",
                "competition_type_id": 4,
                "competition_qualifying": True,
                "segment_level": "Senior",
                "national_appointment_type_id": 4,
            },
        ]
    )
    qualifying = ir._qualifying_competitions_from_panel(
        panel,
        season_codes=[2526],
        competition_type_ids=(15, 16, 17),
        segment_levels=frozenset({"Junior", "Senior"}),
        include_qualifying_national=True,
    )
    assert len(qualifying) == 3
    assert set(qualifying["panel_roles"]) == {"Judge", "Referee"}

    merged = ir._union_qualifying_competitions(
        [
            ir.RuleCheckResult(
                "combined_roles_competitions",
                "Combined",
                1,
                3,
                True,
                "",
                qualifying_competitions=qualifying,
            )
        ]
    )
    assert merged is not None
    assert len(merged) == 3


def test_idvo_first_promote_year_uses_current_listing_without_year_rules(monkeypatch):
    """International IDVO has competition-only promote rules — no year gate."""
    from activityAnalysis.international_listing_seasons import (
        format_promote_first_eligible_display,
    )

    listing = 2627
    monkeypatch.setattr(
        ir,
        "_promote_year_rule_rows_for_appointment",
        lambda *args, **kwargs: [],
    )
    first = ir.first_listing_season_eligible_for_promote_years(
        official_id=1,
        appointment_type_id=16,
        directory_discipline_id=None,
        appointment_level="International",
        appointment_level_id=17,
        appointment_rows=[],
        appt_ctx={},
        listing_season_code=listing,
        rules_df=pd.DataFrame(),
        international_level_id=17,
        isu_level_id=16,
        isu_listing_keys=set(),
    )
    assert first == listing
    assert format_promote_first_eligible_display(
        first, current_listing_season_code=listing
    ) == "2026"

    assert (
        ir.first_listing_season_eligible_for_promote_years(
            official_id=1,
            appointment_type_id=12,
            directory_discipline_id=9,
            appointment_level="International",
            appointment_level_id=17,
            appointment_rows=[],
            appt_ctx={},
            listing_season_code=listing,
            rules_df=pd.DataFrame(),
            international_level_id=17,
            isu_level_id=16,
            isu_listing_keys=set(),
        )
        is None
    )


def test_user_facing_requirement_label_strips_type_ids():
    assert ir.user_facing_requirement_label(
        "Referee in ≥2 international competitions (4 seasons; types 15–17)"
    ) == "Referee in ≥2 international competitions (4 seasons)"
    assert ir.user_facing_requirement_label(
        "TC in ≥3 competitions incl. ≥1 International Competition (4 seasons; types 15-17)"
    ) == "TC in ≥3 competitions incl. ≥1 International Competition (4 seasons)"


def test_seminar_requirement_status_from_evaluation():
    no_rules = ir.RequirementEvaluation(
        rule_set_id=1,
        isu_rule_ref="412.2.b",
        purpose="maintain",
        label="Judge maintain",
        listing_tier="international",
        season_window=3,
        season_codes=[2425, 2526],
        meets=True,
        summary_note="Meets requirements",
        rule_results=[
            ir.RuleCheckResult(
                metric="competition_count",
                display_label="Competitions",
                required=2,
                actual=3,
                met=True,
            )
        ],
    )
    assert ir.seminar_requirement_status_from_evaluation(no_rules) == ""

    seminar_met = ir.RequirementEvaluation(
        rule_set_id=2,
        isu_rule_ref="412.2.b",
        purpose="maintain",
        label="Referee maintain",
        listing_tier="international",
        season_window=4,
        season_codes=[2223, 2324, 2425, 2526],
        meets=True,
        summary_note="Meets requirements",
        rule_results=[
            ir.RuleCheckResult(
                metric="seminar_alternatives",
                display_label="Seminar",
                required=1,
                actual=1,
                met=True,
            )
        ],
    )
    assert ir.seminar_requirement_status_from_evaluation(seminar_met) == "Yes"

    seminar_unmet = ir.RequirementEvaluation(
        rule_set_id=3,
        isu_rule_ref="412.4.b",
        purpose="promote",
        label="Referee promote",
        listing_tier="international",
        season_window=4,
        season_codes=[2223, 2324, 2425, 2526],
        meets=False,
        summary_note="Need seminar",
        rule_results=[
            ir.RuleCheckResult(
                metric="seminar_count",
                display_label="In-person seminar",
                required=1,
                actual=0,
                met=False,
            )
        ],
    )
    assert ir.seminar_requirement_status_from_evaluation(seminar_unmet) == "No"
    assert ir.seminar_requirement_status_from_evaluation(None) == ""


def test_evaluate_seminar_count_and_alternatives():
    seminars = pd.DataFrame(
        [
            {
                "official_id": 1,
                "appointment_type_id": 13,
                "discipline_id": 9,
                "season_code": 2526,
                "in_person": True,
            },
            {
                "official_id": 1,
                "appointment_type_id": 13,
                "discipline_id": 9,
                "season_code": 2425,
                "in_person": False,
            },
        ]
    )
    met, detail = ir._evaluate_seminar_count(
        seminars,
        {"in_person": True, "season_window": 4},
        listing_season_code=2627,
        min_value=1,
    )
    assert met
    assert "1/1 in person" in detail

    met_alt, _via, detail_alt = ir._evaluate_seminar_alternatives(
        seminars,
        {
            "alternatives": [
                {
                    "label": "In-person (4 seasons)",
                    "requirements": [{"in_person": True, "season_window": 4, "min": 1}],
                },
                {
                    "label": "Online (2 seasons)",
                    "requirements": [{"in_person": False, "season_window": 2, "min": 1}],
                },
            ]
        },
        listing_season_code=2627,
    )
    assert met_alt
    assert "In-person (4 seasons)" in detail_alt

    met_any, detail_any = ir._evaluate_seminar_count(
        seminars,
        {"season_window": 2},
        listing_season_code=2627,
        min_value=1,
    )
    assert met_any
    assert detail_any == "2/1 seminar (25-26, 24-25)"
    assert "seminar seminar" not in detail_any

    at_event_df = pd.DataFrame(
        [
            {
                "official_id": 1,
                "appointment_type_id": 15,
                "discipline_id": 1,
                "season_code": 2526,
                "in_person": True,
                "at_event": True,
            }
        ]
    )
    met_event, _via, detail_event = ir._evaluate_seminar_alternatives(
        at_event_df,
        {
            "alternatives": [
                {
                    "label": "At competition",
                    "requirements": [{"season_window": 2, "min": 1, "at_event": True}],
                }
            ]
        },
        listing_season_code=2627,
    )
    assert met_event
    assert "at event" in detail_event


if __name__ == "__main__":
    test_isu_season_codes_preceding_july1()
    test_listing_calendar_year()
    test_championship_or_olympic_detection()
    test_evaluate_seminar_count_and_alternatives()
    print("ok")
