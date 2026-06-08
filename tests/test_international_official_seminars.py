import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "sqlite:////tmp/international_official_seminars_tests.db")

from activityAnalysis.international_listing_seasons import usfs_season_code_for_date
from activityAnalysis.international_official_seminars import (
    DISCIPLINE_ID_PAIRS,
    DISCIPLINE_ID_SINGLES,
    INTERNATIONAL_TECHNICAL_CONTROLLER_APPOINTMENT_TYPE_ID,
    batch_seminars_for_appointment_keys,
    filter_seminars_for_appointment,
    filter_seminars_to_season_codes,
    seminar_discipline_matches,
    seminars_display_for_appointment,
)


def test_usfs_season_code_for_date():
    assert usfs_season_code_for_date(date(2025, 8, 1)) == 2526
    assert usfs_season_code_for_date(date(2026, 6, 30)) == 2526
    assert usfs_season_code_for_date(date(2026, 7, 1)) == 2627


def test_seminar_discipline_matches_strict_for_judge_referee():
    assert seminar_discipline_matches(9, 9, appointment_type_id=12)
    assert seminar_discipline_matches(4, 4, appointment_type_id=13)
    assert not seminar_discipline_matches(8, 1, appointment_type_id=13)
    assert not seminar_discipline_matches(None, 4, appointment_type_id=12)
    assert not seminar_discipline_matches(4, 9, appointment_type_id=12)


def test_seminar_discipline_pairs_counts_for_singles_tc_ts():
    atid = INTERNATIONAL_TECHNICAL_CONTROLLER_APPOINTMENT_TYPE_ID
    assert seminar_discipline_matches(
        DISCIPLINE_ID_PAIRS, DISCIPLINE_ID_SINGLES, appointment_type_id=atid
    )
    assert not seminar_discipline_matches(
        DISCIPLINE_ID_SINGLES, DISCIPLINE_ID_PAIRS, appointment_type_id=atid
    )
    assert seminar_discipline_matches(4, 4, appointment_type_id=atid)


def test_filter_seminars_for_appointment_and_season_window():
    seminars = pd.DataFrame(
        [
            {
                "id": 1,
                "official_id": 10,
                "appointment_type_id": 13,
                "discipline_id": 9,
                "seminar_date": date(2025, 10, 1),
                "season_code": 2526,
                "in_person": True,
                "place": "Colorado Springs",
                "at_event": False,
                "notes": None,
            },
            {
                "id": 2,
                "official_id": 10,
                "appointment_type_id": 13,
                "discipline_id": 9,
                "seminar_date": date(2024, 3, 1),
                "season_code": 2324,
                "in_person": False,
                "place": "Online",
                "at_event": False,
                "notes": None,
            },
            {
                "id": 3,
                "official_id": 11,
                "appointment_type_id": 13,
                "discipline_id": 4,
                "seminar_date": date(2025, 10, 1),
                "season_code": 2526,
                "in_person": True,
                "place": "Montreal",
                "at_event": True,
                "notes": None,
            },
        ]
    )
    scoped = filter_seminars_for_appointment(
        seminars,
        official_id=10,
        appointment_type_id=13,
        directory_discipline_id=9,
    )
    assert len(scoped) == 2

    in_person = filter_seminars_to_season_codes(
        scoped, [2425, 2526], in_person=True
    )
    assert len(in_person) == 1
    assert int(in_person.iloc[0]["season_code"]) == 2526

    at_event = filter_seminars_to_season_codes(
        seminars, [2526], at_event=True
    )
    assert len(at_event) == 1
    assert int(at_event.iloc[0]["id"]) == 3


def test_batch_seminars_for_appointment_keys_reuses_filtered_rows():
    seminars = pd.DataFrame(
        [
            {
                "id": 1,
                "official_id": 10,
                "appointment_type_id": 13,
                "discipline_id": 9,
                "seminar_date": date(2025, 10, 1),
                "season_code": 2526,
                "in_person": True,
                "place": "Colorado Springs",
                "at_event": False,
                "notes": None,
            },
            {
                "id": 2,
                "official_id": 11,
                "appointment_type_id": 13,
                "discipline_id": 4,
                "seminar_date": date(2025, 10, 1),
                "season_code": 2526,
                "in_person": True,
                "place": "Montreal",
                "at_event": False,
                "notes": None,
            },
        ]
    )
    keys = [(10, 13, 9), (10, 13, 9), (11, 13, 4)]
    batched = batch_seminars_for_appointment_keys(seminars, keys)
    assert len(batched) == 2
    assert len(batched[(10, 13, 9)]) == 1
    assert len(batched[(11, 13, 4)]) == 1


def test_seminars_display_for_appointment():
    seminars = pd.DataFrame(
        [
            {
                "id": 1,
                "official_id": 10,
                "appointment_type_id": 15,
                "discipline_id": 1,
                "seminar_date": date(2025, 10, 1),
                "season_code": 2526,
                "in_person": True,
                "place": "Geneva",
                "at_event": False,
                "notes": "Referee track",
            }
        ]
    )
    display = seminars_display_for_appointment(
        10, 15, 1, seminars_bulk=seminars
    )
    assert len(display) == 1
    assert display.iloc[0]["Season"] == "25-26 (2526)"
    assert display.iloc[0]["In person"] == "Yes"
    assert display.iloc[0]["Notes"] == "Referee track"
