import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["DATABASE_URL"] = "sqlite:////tmp/international_officials_tests.db"

_mock_lad = MagicMock()
_mock_lad.activity_database_is_postgresql.return_value = False
_mock_lad.engine = MagicMock()
_mock_lad.NO_DISCIPLINE_DIRECTORY_ID = 7
_mock_lad.segment_discipline_type_ids_for_directory.return_value = ()
_mock_lad.calendar_years_for_usfs_season_codes.return_value = [2025, 2026]
sys.modules["activityAnalysis.load_activity_data"] = _mock_lad
sys.modules["load_activity_data"] = _mock_lad

import pandas as pd

from activityAnalysis import international_officials_data as iod


def test_national_segment_appointment_type_mapping():
    assert iod.national_segment_appointment_type_id(12) == 1
    assert iod.national_segment_appointment_type_id(13) == 4
    assert iod.national_segment_appointment_type_id(14) == 9
    assert iod.national_segment_appointment_type_id(15) == 11
    assert iod.national_segment_appointment_type_id(16) == 8
    assert iod.national_segment_appointment_type_id(99) is None


def test_summarize_international_activity_counts():
    detail = pd.DataFrame(
        [
            {
                "official_id": 1,
                "official_name": "A. Official",
                "mbr_number": "123",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "discipline_id": 1,
                "discipline": "Singles",
                "competition_id": 10,
                "segment_id": 100,
            },
            {
                "official_id": 1,
                "official_name": "A. Official",
                "mbr_number": "123",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "discipline_id": 1,
                "discipline": "Singles",
                "competition_id": 10,
                "segment_id": 101,
            },
            {
                "official_id": 1,
                "official_name": "A. Official",
                "mbr_number": "123",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "discipline_id": 1,
                "discipline": "Singles",
                "competition_id": 11,
                "segment_id": 102,
            },
        ]
    )
    summary = iod.summarize_international_activity(detail)
    assert len(summary) == 1
    assert summary.iloc[0]["competition_count"] == 2
    assert summary.iloc[0]["segment_count"] == 3


def test_detail_empty_without_postgresql():
    assert iod.get_international_official_activity_detail().empty


def test_split_panel_detail_by_scope():
    detail = pd.DataFrame(
        [
            {"competition_scope": "International", "competition_name": "ISU Event"},
            {"competition_scope": "National", "competition_name": "Nationals"},
            {"competition_scope": "Other", "competition_name": "Local"},
        ]
    )
    international, national = iod.split_panel_detail_by_scope(detail)
    assert len(international) == 1
    assert len(national) == 1
    assert international.iloc[0]["competition_name"] == "ISU Event"


def test_sort_panel_activity_detail_most_recent_first():
    detail = pd.DataFrame(
        [
            {
                "competition_year": 2425,
                "competition_name": "Older Event",
                "start_date": "2024-10-01",
                "segment_name": "SP",
            },
            {
                "competition_year": 2526,
                "competition_name": "Newer Event",
                "start_date": "2025-11-15",
                "segment_name": "FS",
            },
            {
                "competition_year": 2526,
                "competition_name": "Newer Event",
                "start_date": "2025-11-15",
                "segment_name": "SP",
            },
        ]
    )
    sorted_detail = iod.sort_panel_activity_detail(detail)
    assert sorted_detail.iloc[0]["competition_name"] == "Newer Event"
    assert sorted_detail.iloc[-1]["competition_name"] == "Older Event"
    assert list(sorted_detail["competition_year"]) == [2526, 2526, 2425]


def test_filter_appointments_excluding_one():
    appointments = pd.DataFrame(
        [
            {"official_id": 1, "appointment_type_id": 12, "discipline_id": 1},
            {"official_id": 1, "appointment_type_id": 13, "discipline_id": 1},
            {"official_id": 1, "appointment_type_id": 12, "discipline_id": 2},
        ]
    )
    other = iod.filter_appointments_excluding_one(
        appointments, official_id=1, appointment_type_id=12, discipline_id=1
    )
    assert len(other) == 2


def test_collapse_data_operator_appointments():
    df = pd.DataFrame(
        [
            {
                "official_id": 1,
                "official_name": "D. Operator",
                "mbr_number": "99",
                "appointment_type_id": 16,
                "appointment_type": "International Data / Video Operator",
                "appointment_level": "International",
                "discipline_id": 9,
                "discipline": "Singles/Pairs",
            },
            {
                "official_id": 1,
                "official_name": "D. Operator",
                "mbr_number": "99",
                "appointment_type_id": 16,
                "appointment_type": "International Data / Video Operator",
                "appointment_level": "ISU",
                "discipline_id": 2,
                "discipline": "Synchronized",
            },
            {
                "official_id": 2,
                "official_name": "J. Judge",
                "mbr_number": "1",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "appointment_level": "International",
                "discipline_id": 1,
                "discipline": "Singles",
            },
        ]
    )
    out = iod._collapse_data_operator_appointments(df)
    assert len(out) == 2
    idvo = out[out["appointment_type_id"] == 16]
    assert len(idvo) == 1
    assert idvo.iloc[0]["discipline"] == iod.DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL
    assert idvo.iloc[0]["appointment_level"] == "ISU Championship"


def test_officials_filter_column_order():
    """SQL row tuple order must match DataFrame column names (level vs discipline)."""
    row = (1, "A. Official", "123", 12, "International Judge", "International", 1, "Singles")
    df = pd.DataFrame(
        [row],
        columns=[
            "official_id",
            "official_name",
            "mbr_number",
            "appointment_type_id",
            "appointment_type",
            "appointment_level",
            "discipline_id",
            "discipline",
        ],
    )
    assert df.iloc[0]["appointment_level"] == "International"
    assert df.iloc[0]["discipline"] == "Singles"
    assert df.iloc[0]["discipline_id"] == 1
    assert iod._normalize_appointment_level("international") == "International"
    assert iod._normalize_appointment_level("ISU Championship") == "ISU Championship"
    assert iod._normalize_appointment_level("ISU") == "ISU Championship"
    levels = pd.Series(["International", "ISU Championship"])
    assert iod._pick_collapsed_appointment_level(levels) == "ISU Championship"


def test_competition_scope_label():
    assert iod.competition_scope_label(15, False) == "International"
    assert iod.competition_scope_label(4, True) == "National"
    assert iod.competition_scope_label(11, False) == "Other"
    assert iod.competition_scope_label(12, True) == "Other"


def test_normalize_merge_key_dtypes():
    appt = pd.DataFrame(
        [{"official_id": 1, "appointment_type_id": 16, "discipline_id": pd.NA}]
    )
    counts = pd.DataFrame(
        [{"official_id": 1, "appointment_type_id": 16, "discipline_id": float("nan")}]
    )
    merged = iod._normalize_merge_key_dtypes(appt).merge(
        iod._normalize_merge_key_dtypes(counts),
        on=["official_id", "appointment_type_id", "discipline_id"],
        how="left",
    )
    assert len(merged) == 1


def test_idvo_discipline_match_sql():
    clause, params = iod._discipline_match_sql(
        intl_appointment_type_id=16,
        directory_discipline_id=None,
    )
    assert "discipline_type_id IN" in clause
    assert params["idvo_segment_discipline_type_ids"] == [1, 2, 3, 5]


def test_nullable_int_for_sql_handles_pd_na():
    assert iod._nullable_int_for_sql(pd.NA) is None
    assert iod._nullable_int_for_sql(9) == 9


def test_activity_detail_groups_panel_by_official(monkeypatch):
    monkeypatch.setattr(iod, "activity_database_is_postgresql", lambda: True)

    panel = pd.DataFrame(
        [
            {
                "official_id": 1,
                "national_appointment_type_id": 1,
                "segment_discipline_type_id": 1,
                "competition_id": 10,
                "competition_year": 2526,
                "competition_name": "GP",
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_id": 100,
                "segment_level": "Senior",
                "segment_name": "Men",
                "segment_discipline": "Men",
                "role": "Judge",
                "results_url": "",
                "start_date": None,
                "end_date": None,
            },
            {
                "official_id": 2,
                "national_appointment_type_id": 1,
                "segment_discipline_type_id": 1,
                "competition_id": 11,
                "competition_year": 2526,
                "competition_name": "Other",
                "competition_type_id": 17,
                "competition_qualifying": False,
                "segment_id": 101,
                "segment_level": "Senior",
                "segment_name": "Women",
                "segment_discipline": "Women",
                "role": "Judge",
                "results_url": "",
                "start_date": None,
                "end_date": None,
            },
        ]
    )
    appointments = pd.DataFrame(
        [
            {
                "official_id": 1,
                "official_name": "A. Official",
                "mbr_number": "1",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "discipline_id": 1,
                "discipline": "Singles",
            }
        ]
    )
    monkeypatch.setattr(
        iod,
        "allowed_segment_discipline_type_ids",
        lambda _at, _disc: frozenset({1}),
    )
    detail = iod.get_international_official_activity_detail(
        appointments=appointments,
        panel_bulk=panel,
    )
    assert len(detail) == 1
    assert detail.iloc[0]["official_id"] == 1
    assert detail.iloc[0]["competition_id"] == 10


if __name__ == "__main__":
    test_national_segment_appointment_type_mapping()
    test_summarize_international_activity_counts()
    test_detail_empty_without_postgresql()
    test_collapse_data_operator_appointments()
    test_idvo_discipline_match_sql()
    test_nullable_int_for_sql_handles_pd_na()
    print("ok")
