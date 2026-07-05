"""Tests for international judge availability ingest and reporting."""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import pytest

from activityAnalysis.international_availability import (
    DISCIPLINE_FILTER_DANCE,
    DISCIPLINE_FILTER_SINGLES_PAIRS,
    LEVEL_FILTER_ISU,
    availability_cell_display,
    build_form_response_index,
    build_international_availability_report,
    default_availability_workbook_path,
    event_column_short_label,
    load_international_availability_workbook,
    official_is_eligible_for_availability_report,
    parse_workbook_structure,
    supplemental_column_short_label,
)
from activityAnalysis.international_requirements import (
    DIRECTORY_LEVEL_ID_INTERNATIONAL,
    DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
)
from activityAnalysis.load_activity_data import DISC_DANCE_ID, DISC_SINGLES_PAIRS_ID


def test_default_workbook_exists():
    path = default_availability_workbook_path()
    assert os.path.isfile(path), f"Bundled workbook missing: {path}"


def test_load_bundled_workbook_structure():
    df = load_international_availability_workbook(default_availability_workbook_path())
    layout = parse_workbook_structure(df)
    assert layout["first_name_col"] == "First Name"
    assert layout["email_col"] == "Email Address"
    assert len(layout["event_cols"]) >= 40
    assert len(layout["supplemental_cols"]) == 5


def test_event_column_short_label():
    header = "JGP China - August 18-22 (Junior Grand Prix Events)"
    assert event_column_short_label(header) == "JGP China - August 18-22"


def test_supplemental_column_short_label():
    assert (
        supplemental_column_short_label("Do you have a valid passport?")
        == "Valid passport"
    )


@pytest.mark.parametrize(
    "raw,label",
    [
        ("YES", "Available"),
        ("NO", "Not available"),
        (None, "No response"),
    ],
)
def test_availability_cell_display(raw, label):
    assert availability_cell_display(raw) == label


def test_build_form_response_index_by_email():
    df = pd.DataFrame(
        [
            {
                "First Name": "Jane",
                "Last Name": "Doe",
                "Email Address": "Jane.Doe@Example.com",
                "Event A": "YES",
            }
        ]
    )
    layout = parse_workbook_structure(df)
    by_email, by_name = build_form_response_index(df, layout)
    assert "jane.doe@example.com" in by_email
    assert "jane doe" in by_name


def test_build_report_joins_directory_appointments():
    form_df = pd.DataFrame(
        [
            {
                "First Name": "Jane",
                "Last Name": "Doe",
                "Email Address": "jane@example.com",
                "JGP China - August 18-22 (Junior Grand Prix Events)": "YES",
                "Grand Prix France - October 22-25 (Grand Prix Events)": "NO",
                "Are you able to attend more than one competition in a season?": "Yes",
                "Do you have a valid passport?": "Yes",
                "If someone is needed for back-to-back events, is that an option for you?": "No",
                "Judges, do you need any specific activity of which the International Selections Subcommittee should be aware?": "",
                "Please provide us with any other information we might need to know regarding your availability.": "",
            }
        ]
    )
    layout = parse_workbook_structure(form_df)
    appointments = pd.DataFrame(
        [
            {
                "official_id": 42,
                "official_name": "Jane Doe",
                "mbr_number": "12345",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "appointment_level": "International",
                "appointment_level_id": DIRECTORY_LEVEL_ID_INTERNATIONAL,
                "discipline_id": DISC_SINGLES_PAIRS_ID,
                "discipline": "Singles/Pairs",
            }
        ]
    )

    report, meta = build_international_availability_report(
        form_df,
        discipline_filter=DISCIPLINE_FILTER_SINGLES_PAIRS,
        level_filter=LEVEL_FILTER_ISU,
        appointments=appointments,
        layout=layout,
        engine=None,
    )
    # Appointment is International level; ISU filter should exclude it.
    assert report.empty

    report, meta = build_international_availability_report(
        form_df,
        discipline_filter=DISCIPLINE_FILTER_SINGLES_PAIRS,
        level_filter="International",
        appointments=appointments,
        layout=layout,
        birthdates={42: date(1980, 1, 1)},
        engine=None,
    )
    assert len(report) == 1
    assert report.loc[0, "Official"] == "Jane Doe"
    assert meta["form_match_count"] == 1
    jgp_label = layout["event_short_labels"][layout["event_cols"][0]]
    assert report.loc[0, jgp_label] == "Available"


def test_build_report_no_form_match_shows_no_response():
    form_df = pd.DataFrame(
        columns=[
            "First Name",
            "Last Name",
            "Email Address",
            "JGP China - August 18-22 (Junior Grand Prix Events)",
        ]
    )
    layout = parse_workbook_structure(form_df)
    appointments = pd.DataFrame(
        [
            {
                "official_id": 7,
                "official_name": "Unlisted Official",
                "mbr_number": "999",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "appointment_level": "ISU Championship",
                "appointment_level_id": DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
                "discipline_id": DISC_DANCE_ID,
                "discipline": "Ice Dance",
            }
        ]
    )
    report, meta = build_international_availability_report(
        form_df,
        discipline_filter=DISCIPLINE_FILTER_DANCE,
        level_filter=LEVEL_FILTER_ISU,
        appointments=appointments,
        layout=layout,
        engine=None,
    )
    assert len(report) == 1
    assert meta["form_match_count"] == 0
    jgp_label = layout["event_short_labels"][layout["event_cols"][0]]
    assert report.loc[0, jgp_label] == "No response"


@pytest.mark.parametrize(
    "dob,eligible",
    [
        (date(1956, 6, 1), False),  # 70 on July 1, 2026
        (date(1956, 7, 2), True),  # 69 on July 1, 2026
        (None, True),
    ],
)
def test_official_is_eligible_for_availability_report(dob, eligible):
    assert official_is_eligible_for_availability_report(dob) is eligible


def test_build_report_excludes_officials_age_70_plus():
    form_df = pd.DataFrame(
        columns=[
            "First Name",
            "Last Name",
            "Email Address",
            "JGP China - August 18-22 (Junior Grand Prix Events)",
        ]
    )
    layout = parse_workbook_structure(form_df)
    appointments = pd.DataFrame(
        [
            {
                "official_id": 1,
                "official_name": "Young Judge",
                "mbr_number": "1",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "appointment_level": "ISU Championship",
                "appointment_level_id": DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
                "discipline_id": DISC_DANCE_ID,
                "discipline": "Ice Dance",
            },
            {
                "official_id": 2,
                "official_name": "Aged Out Judge",
                "mbr_number": "2",
                "appointment_type_id": 12,
                "appointment_type": "International Judge",
                "appointment_level": "ISU Championship",
                "appointment_level_id": DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
                "discipline_id": DISC_DANCE_ID,
                "discipline": "Ice Dance",
            },
        ]
    )
    birthdates = {
        1: date(1957, 1, 1),
        2: date(1956, 3, 15),
    }
    report, meta = build_international_availability_report(
        form_df,
        discipline_filter=DISCIPLINE_FILTER_DANCE,
        level_filter=LEVEL_FILTER_ISU,
        appointments=appointments,
        layout=layout,
        birthdates=birthdates,
        engine=None,
    )
    assert meta["excluded_age_out"] == 1
    assert len(report) == 1
    assert report.loc[0, "Official"] == "Young Judge"
