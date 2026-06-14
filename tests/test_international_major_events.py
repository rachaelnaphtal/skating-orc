import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["DATABASE_URL"] = "sqlite:////tmp/international_major_events_tests.db"

_mock_lad = MagicMock()
_mock_lad.activity_database_is_postgresql.return_value = False
_mock_lad.engine = MagicMock()
_mock_lad.SINGLES_DISCIPLINE_ID = 1
_mock_lad.DISC_PAIRS_ID = 8
_mock_lad.DISC_DANCE_ID = 4
_mock_lad.DISC_SYNCHRO_ID = 2
_mock_lad.DISC_SINGLES_PAIRS_ID = 9
_mock_lad.segment_discipline_type_ids_for_directory.return_value = (1,)
sys.modules["activityAnalysis.load_activity_data"] = _mock_lad
sys.modules["load_activity_data"] = _mock_lad

import pandas as pd

from activityAnalysis import international_major_events as ime
from activityAnalysis.isu_major_event_classification import (
    MAJOR_ISU_EVENT_SYNCHRO_WORLDS,
    MAJOR_ISU_EVENT_WORLDS,
)


def test_competition_calendar_year_prefers_start_date():
    assert (
        ime.competition_calendar_year(
            start_date=pd.Timestamp("2025-03-25"),
            end_date=None,
            competition_year="2526",
        )
        == 2025
    )


def test_competition_calendar_year_from_season_code():
    assert (
        ime.competition_calendar_year(
            start_date=None,
            end_date=None,
            competition_year="2526",
        )
        == 2026
    )


def test_competition_calendar_year_from_title_for_worlds():
    assert (
        ime.competition_calendar_year(
            start_date=None,
            end_date=None,
            competition_year="2009",
            competition_name="2010 World Figure Skating Championships",
        )
        == 2010
    )


def test_major_event_assignment_label():
    assert ime.major_event_assignment_label(1, 3) == "J-D"
    assert ime.major_event_assignment_label(11, 1) == "TC-S"
    assert ime.major_event_assignment_label(8, 3) == "DO"
    assert ime.major_event_assignment_label(8, 1) == "DO"


def test_major_event_cell_labels_sorted():
    activity = pd.DataFrame(
        [
            {"national_appointment_type_id": 9, "segment_discipline_type_id": 1},
            {"national_appointment_type_id": 1, "segment_discipline_type_id": 3},
        ]
    )
    assert ime.major_event_cell_labels(activity) == "J-D, TS-S"


def test_years_eligible_for_appointment():
    assert ime.years_eligible_for_appointment(2010, current_year=2025) == 15
    assert ime.years_eligible_for_appointment(2025, current_year=2025) == 0


def test_enrich_major_event_matrix_eligibility_uses_earliest():
    matrix = pd.DataFrame(
        {
            "official_id": [1],
            "full_name": ["A. Official"],
            2020: ["J-S"],
        }
    )
    appts = pd.DataFrame(
        [
            {"official_id": 1, "achieved_date": "2010-01-01"},
            {"official_id": 1, "achieved_date": "2015-06-01"},
        ]
    )
    out = ime.enrich_major_event_matrix_eligibility(matrix, appts)
    assert int(out.loc[0, "eligible_years"]) == ime.years_eligible_for_appointment(
        2010, current_year=ime._CURRENT_YEAR
    )
    assert int(out.loc[0, "achieved_year"]) == 2010


def test_format_major_event_years_since_last_blank_when_never_served():
    matrix = pd.DataFrame(
        {
            "official_id": [1, 2],
            "full_name": ["Never", "Recent"],
            "years_since_last": [pd.NA, 0],
            "eligible_years": [10, 10],
        }
    )
    display, _ = ime.format_major_event_matrix_for_display(matrix)
    assert pd.isna(display.loc[0, "Years since last"])
    assert int(display.loc[1, "Years since last"]) == 0


def test_ensure_matrix_year_columns_adds_missing_years():
    matrix = pd.DataFrame({"official_id": [1], "full_name": ["A. Official"], 2026: [1]})
    out = ime._ensure_matrix_year_columns(matrix, [2010, 2015, 2026])
    assert set(ime._matrix_year_columns(out)) == {2010, 2015, 2026}


def test_referee_segment_discipline_filter_allows_event_level_rows():
    sql, params = ime._segment_discipline_filter_sql(
        appointment_type_id=13,
        discipline_id=None,
    )
    assert sql == ""
    assert params == {}


def test_idvo_segment_discipline_filter_uses_combined_disciplines():
    sql, params = ime._segment_discipline_filter_sql(
        appointment_type_id=16,
        discipline_id=None,
    )
    assert "idvo_segment_discipline_type_ids" in sql
    assert params["idvo_segment_discipline_type_ids"] == [1, 2, 3, 5]


def test_all_roles_segment_discipline_filter_includes_synchronized():
    sql, params = ime._segment_discipline_filter_sql(
        appointment_type_id=None,
        discipline_id=None,
    )
    assert params["spd_segment_discipline_type_ids"] == [1, 2, 3, 5]


def test_synchro_event_segment_discipline_filter_synchronized_only():
    sql, params = ime._segment_discipline_filter_sql(
        appointment_type_id=None,
        discipline_id=None,
        event_key=MAJOR_ISU_EVENT_SYNCHRO_WORLDS,
    )
    assert "synchro_segment_discipline_type_ids" in sql
    assert params["synchro_segment_discipline_type_ids"] == [5]


def test_competition_calendar_year_from_synchro_results_url():
    assert (
        ime.competition_calendar_year(
            start_date=None,
            end_date=None,
            competition_year="2425",
            competition_name="ISU World Synchronized Championships 2025",
            results_url="https://results.isu.org/results/season2425/wsysc2025/",
        )
        == 2025
    )


def test_major_event_assignment_label_synchronized():
    assert ime.major_event_assignment_label(1, 5) == "J-SYS"
    assert ime.major_event_assignment_label(11, 5) == "TC-SYS"


def test_major_event_pre_appointment_mask():
    display = pd.DataFrame({"achieved_year": [2010]}, index=[0])
    mask = ime.major_event_pre_appointment_mask(display, ["2009", "2010", "2011"])
    assert bool(mask.loc[0, "2009"])
    assert bool(mask.loc[0, "2010"])
    assert not bool(mask.loc[0, "2011"])


def test_format_major_event_matrix_preserves_labels():
    matrix = pd.DataFrame(
        {
            "official_id": [1],
            "full_name": ["A. Official"],
            "region": ["M"],
            "years_since_last": [1],
            "most_recent_year": [2025],
            "never_used": [False],
            "total_championships": [2],
            "eligible_years": [10.0],
            "achieved_year": [2010],
            2024: ["J-D"],
            2025: [""],
        }
    )
    display, year_cols = ime.format_major_event_matrix_for_display(matrix)
    assert year_cols == ["2025", "2024"]
    assert display.loc[0, "2024"] == "J-D"
    assert display.loc[0, "2025"] == ""
    assert display.loc[0, "Years eligible"] == 10.0
    assert display.loc[0, "achieved_year"] == 2010


def test_load_major_event_assignment_rows_empty_without_postgresql():
    assert ime.load_major_event_assignment_rows([], event_key=MAJOR_ISU_EVENT_WORLDS).empty


def test_collapse_qualified_officials_one_row_per_person():
    qualified = pd.DataFrame(
        [
            {"official_id": 1, "full_name": "A. Official", "achieved_date": "2020-01-01"},
            {"official_id": 1, "full_name": "A. Official", "achieved_date": "2018-06-01"},
            {"official_id": 2, "full_name": "B. Official", "achieved_date": "2019-01-01"},
        ]
    )
    out = ime._collapse_qualified_officials(qualified)
    assert len(out) == 2
    assert out.loc[out["official_id"] == 1, "achieved_date"].iloc[0] == pd.Timestamp("2018-06-01")
