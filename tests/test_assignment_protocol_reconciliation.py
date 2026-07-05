import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["DATABASE_URL"] = "sqlite:////tmp/activity_tracker_tests.db"

from activityAnalysis.assignment_protocol_reconciliation import (
    PROTOCOL_PANEL_APPOINTMENT_TYPE_NAMES,
    _compare_panel_role_sets,
    _filter_retired_official_rows,
    compare_official_sets,
    panel_appointment_type_ids,
    competition_names_compatible,
    match_assignment_competitions_to_public,
    pub_year_to_season_code,
    season_code_for_calendar_year,
)
from activityAnalysis.load_activity_data import (
    is_retired_official_display_name,
    load_retired_official_name_keys,
)


def test_season_code_for_calendar_year_2022():
    assert season_code_for_calendar_year(2022) == 2122


def test_pub_year_to_season_code_accepts_season_and_calendar():
    assert pub_year_to_season_code("2122") == 2122
    assert pub_year_to_season_code(2022) == 2122
    assert pub_year_to_season_code("2526") == 2526


def test_competition_names_compatible_substring():
    assert competition_names_compatible(
        "2022 U.S. Figure Skating Championships",
        "U.S. Figure Skating Championships 2022",
    )


def test_match_assignment_competitions_exact_name():
    oa = pd.DataFrame(
        [
            {
                "oa_competition_id": 1,
                "oa_name": "2022 U.S. Figure Skating Championships",
                "oa_calendar_year": 2022,
                "competition_type_id": 4,
                "competition_type_name": "US Championships",
            }
        ]
    )
    pub = pd.DataFrame(
        [
            {
                "pub_competition_id": 10,
                "pub_name": "2022 U.S. Figure Skating Championships",
                "pub_year": "2122",
                "competition_type_id": 4,
            }
        ]
    )
    out = match_assignment_competitions_to_public(oa, pub)
    assert len(out) == 1
    assert int(out.iloc[0]["pub_competition_id"]) == 10
    assert out.iloc[0]["match_status"] == "matched"


def test_compare_official_sets_counts_diffs():
    matches = pd.DataFrame(
        [
            {
                "oa_competition_id": 1,
                "oa_name": "Test Event",
                "oa_calendar_year": 2022,
                "season_code": 2122,
                "competition_type_id": 4,
                "competition_type_name": "US Championships",
                "pub_competition_id": 10,
                "pub_name": "Test Event",
                "pub_year": "2122",
                "match_status": "matched",
            }
        ]
    )
    assignments = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "official_id": 100,
                "full_name": "Alex Example",
                "appt_type_name": "Judge",
                "discipline_name": "Singles",
                "chief": False,
                "lower_levels_only": False,
            },
            {
                "competition_id": 1,
                "official_id": 101,
                "full_name": "Only Assign",
                "appt_type_name": "Referee",
                "discipline_name": "Singles",
                "chief": False,
                "lower_levels_only": False,
            },
        ]
    )
    protocol = pd.DataFrame(
        [
            {
                "competition_id": 10,
                "official_id": 100,
                "official_name": "Alex Example",
                "directory_name": "Alex Example",
                "appointment_name": "Judge",
                "protocol_role": "Judge No.1",
                "discipline": "Singles",
            },
            {
                "competition_id": 10,
                "official_id": 102,
                "official_name": "Only Protocol",
                "directory_name": "Only Protocol",
                "appointment_name": "Technical Specialist",
                "protocol_role": "Technical Specialist",
                "discipline": "Singles",
            },
        ]
    )
    summary, detail = compare_official_sets(assignments, protocol, matches=matches)
    row = summary.iloc[0]
    assert int(row["assignment_officials"]) == 2
    assert int(row["protocol_officials"]) == 2
    assert int(row["in_both"]) == 1
    assert int(row["assignments_only"]) == 1
    assert int(row["protocol_only"]) == 1
    statuses = set(detail["status"])
    assert statuses == {"both", "assignments_only", "protocol_only"}


def test_compare_panel_role_sets_flags_extra_protocol_role():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Referee – Synchronized"},
        {"Referee – Synchronized", "Technical Controller – Synchronized"},
    )
    assert status == "role_mismatch"
    assert only_assign == set()
    assert only_proto == {("Technical Controller", "synchronized")}


def test_compare_panel_role_sets_matching_roles():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Referee – Synchronized"},
        {"Referee – synchronized"},
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_chief_referee_matches_referee():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Chief Referee – Synchronized"},
        {"Referee – Synchronized"},
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_chief_referee_ascii_hyphen():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Chief Referee - Synchronized"},
        {"Referee – Synchronized"},
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_singles_pairs_assignment_matches_protocol_singles():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Referee – Singles/Pairs"},
        {"Referee – Singles"},
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_singles_pairs_assignment_matches_protocol_pairs():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Competition Judge – Singles/Pairs"},
        {"Judge – Pairs"},
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_singles_pairs_covers_both_protocol_segments():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Referee – Singles/Pairs"},
        {"Referee – Pairs", "Referee – Singles"},
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_singles_plus_singles_pairs_with_protocol_singles_and_pairs():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {
            "Competition Judge – Singles",
            "Competition Judge – Singles/Pairs",
            "Technical Controller – Singles",
        },
        {
            "Competition Judge – Pairs",
            "Competition Judge – Singles",
            "Technical Controller – Singles",
        },
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_referee_singles_plus_singles_pairs_with_protocol_split():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {
            "Competition Judge – Singles/Pairs",
            "Referee – Singles",
            "Referee – Singles/Pairs",
            "Technical Controller – Singles",
        },
        {
            "Competition Judge – Singles",
            "Referee – Pairs",
            "Referee – Singles",
            "Technical Controller – Singles",
        },
    )
    assert status == "both"
    assert only_assign == set()
    assert only_proto == set()


def test_compare_panel_role_sets_tc_singles_pairs_does_not_match_protocol_singles():
    status, only_assign, only_proto = _compare_panel_role_sets(
        {"Technical Controller – Singles/Pairs"},
        {"Technical Controller – Singles"},
    )
    assert status == "role_mismatch"
    assert only_assign == {("Technical Controller", "singles/pairs")}
    assert only_proto == {("Technical Controller", "singles")}


def test_compare_official_sets_linked_judge_name_spelling():
    matches = pd.DataFrame(
        [
            {
                "oa_competition_id": 1,
                "oa_name": "Test Event",
                "oa_calendar_year": 2022,
                "season_code": 2122,
                "competition_type_id": 4,
                "competition_type_name": "US Championships",
                "pub_competition_id": 10,
                "pub_name": "Test Event",
                "pub_year": "2122",
                "match_status": "matched",
            }
        ]
    )
    assignments = pd.DataFrame(
        [
            {
                "competition_id": 1,
                "official_id": 55,
                "full_name": "Tamara Campbell",
                "appt_type_name": "Competition Judge",
                "discipline_name": "Singles",
                "chief": False,
                "lower_levels_only": False,
            },
        ]
    )
    protocol = pd.DataFrame(
        [
            {
                "competition_id": 10,
                "official_id": None,
                "official_name": "Tamie Campell",
                "directory_name": "",
                "appointment_name": "",
                "protocol_role": "Judge No.1",
                "discipline": "Singles",
            },
        ]
    )
    identity_ctx = {
        "judge_names_by_official": {55: {"Tamara Campbell", "Tamie Campell"}},
        "alias_keys_by_official": {},
    }
    summary, detail = compare_official_sets(
        assignments,
        protocol,
        matches=matches,
        identity_ctx=identity_ctx,
    )
    assert int(summary.iloc[0]["in_both"]) == 1
    assert int(summary.iloc[0]["assignments_only"]) == 0
    assert int(summary.iloc[0]["protocol_only"]) == 0
    assert detail.iloc[0]["status"] == "both"


def test_protocol_panel_appointment_type_names():
    assert "Competition Judge" in PROTOCOL_PANEL_APPOINTMENT_TYPE_NAMES
    assert "Data Operator" in PROTOCOL_PANEL_APPOINTMENT_TYPE_NAMES
    assert len(PROTOCOL_PANEL_APPOINTMENT_TYPE_NAMES) == 5
    without_do = panel_appointment_type_ids(include_data_operator=False)
    assert len(without_do) == 4
    assert 8 not in without_do


def test_load_retired_official_name_keys_reads_workbook():
    keys = load_retired_official_name_keys()
    assert len(keys) > 100
    assert is_retired_official_display_name("Charles Cyr", retired_name_keys=keys)


def test_filter_retired_official_rows_drops_retired_protocol_names():
    retired_name_keys = {"charles cyr"}
    frame = pd.DataFrame(
        [
            {"official_id": None, "official_name": "Charles Cyr", "directory_name": ""},
            {"official_id": 99, "official_name": "Active Person", "directory_name": ""},
        ]
    )
    out = _filter_retired_official_rows(
        frame,
        retired_name_keys=retired_name_keys,
        name_fields=("directory_name", "official_name"),
    )
    assert len(out) == 1
    assert out.iloc[0]["official_name"] == "Active Person"

