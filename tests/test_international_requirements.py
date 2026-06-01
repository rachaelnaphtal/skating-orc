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
_mock_iod._nullable_int_for_sql.side_effect = lambda x: None if x is None else int(x)
_mock_iod.national_segment_appointment_type_id.return_value = 4
sys.modules["activityAnalysis.international_officials_data"] = _mock_iod
sys.modules["international_officials_data"] = _mock_iod

import pandas as pd

from activityAnalysis import international_requirements as ir


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


def test_competition_scope_and_qualifying_national():
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


if __name__ == "__main__":
    test_isu_season_codes_preceding_july1()
    test_listing_calendar_year()
    test_championship_or_olympic_detection()
    print("ok")
