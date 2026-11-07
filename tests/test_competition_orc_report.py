"""ORC workbook built from stored scores, without scraping a results URL."""

import openpyxl
from io import BytesIO

from competition_orc_report import (
    build_orc_workbook,
    build_segment_protocol,
    panel_judge_slots,
    safe_report_filename,
    segment_matches_event_regex,
)


def test_panel_order_matches_judge_numbers_case_insensitively():
    slots, warnings = panel_judge_slots(
        [
            ("Referee", "Pat Official"),
            ("Judge No.2", "BOB SMITH"),
            ("Judge No.1", "Ms. Ann Smith"),
        ],
        {5: "Bob Smith", 9: "Ann Smith"},
    )
    assert warnings == []
    assert slots == [(9, "Ann Smith"), (5, "Bob Smith")]


def test_scoring_judge_missing_from_panel_is_appended():
    slots, warnings = panel_judge_slots(
        [("Judge No.1", "Ann Smith")],
        {1: "Ann Smith", 2: "Cara Jones"},
    )
    assert [name for _jid, name in slots] == ["Ann Smith", "Cara Jones"]
    assert warnings and "Cara Jones" in warnings[0]


def test_protocol_scores_follow_panel_order_and_leave_gaps():
    judges, elements, pcs, _warnings = build_segment_protocol(
        [("Judge No.1", "Ann Smith"), ("Judge No.2", "Bob Smith")],
        [
            _element_row(judge_id=5, judge_name="Bob Smith", score=-5, element_number=1),
            _element_row(judge_id=9, judge_name="Ann Smith", score=1, element_number=1),
        ],
        [
            _pcs_row(judge_id=9, judge_name="Ann Smith", score=7.5),
        ],
    )
    assert judges == ["Ann Smith", "Bob Smith"]
    assert elements["Skater One"][0]["Scores"] == [1.0, -5.0]
    assert elements["Skater One"][0]["Notes"] == "F"
    assert pcs["Skater One"][0]["Scores"] == [7.5, None]


def test_workbook_flags_rule_error_for_the_panel_judge():
    workbook = _workbook_for(
        [
            (
                "JUNIOR_MEN_SHORT_PROGRAM",
                ["Ann Smith", "Bob Smith"],
                {
                    "Skater One": [
                        {
                            "Element": "3F",
                            "Notes": "F",
                            "Number": 1,
                            "Scores": [1, -5],
                        }
                    ]
                },
                {},
            )
        ]
    )
    sheet = next(ws for ws in workbook.worksheets if ws.title != "Summary")
    assert sheet["A6"].value == "J1- Ann Smith"
    assert sheet["C6"].value == "Max with errors is -3"
    assert sheet["E6"].value == "3F"
    assert workbook["Summary"]["A1"].value == "Eastern Sectional"


def test_judge_filter_drops_other_panel_judges():
    workbook = _workbook_for(
        [
            (
                "JUNIOR_MEN_SHORT_PROGRAM",
                ["Ann Smith", "Bob Smith"],
                {
                    "Skater One": [
                        {
                            "Element": "3F",
                            "Notes": "F",
                            "Number": 1,
                            "Scores": [1, -5],
                        }
                    ]
                },
                {},
            )
        ],
        judge_filter="Bob Smith",
    )
    sheet = next(ws for ws in workbook.worksheets if ws.title != "Summary")
    assert sheet["A6"].value == "J2- Bob Smith"
    assert sheet["D6"].value == "Skater One"


def test_additional_analysis_workbook_is_written_without_pcs():
    _main, extra = build_orc_workbook(
        "Eastern Sectional",
        [
            (
                "JUNIOR_MEN_SHORT_PROGRAM",
                ["Ann Smith", "Bob Smith"],
                {
                    "Skater One": [
                        {
                            "Element": "3F",
                            "Notes": "F",
                            "Number": 1,
                            "Scores": [1, -5],
                        }
                    ]
                },
                {},
            )
        ],
        add_additional_analysis=True,
        competition_start_date="2026-11-03",
        competition_year="2627",
    )
    assert extra
    extra_book = openpyxl.load_workbook(BytesIO(extra))
    assert "All Elements" in extra_book.sheetnames
    assert "PCS Thrown out" in extra_book.sheetnames


def test_event_regex_blank_includes_every_name():
    assert segment_matches_event_regex("CHAMPIONSHIP_MEN_SHORT_PROGRAM", "")
    assert segment_matches_event_regex("CHAMPIONSHIP_MEN_SHORT_PROGRAM", "(?i).*MEN.*")
    assert not segment_matches_event_regex("JUVENILE_GIRLS_FREE_SKATING", "(?i).*MEN.*")


def test_report_filename_rejects_path_characters():
    assert safe_report_filename("  Eastern  Sectional ") == "Eastern Sectional"
    try:
        safe_report_filename("a/b")
    except ValueError as exc:
        assert "Report name" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def _workbook_for(segments, **kwargs):
    data, _extra = build_orc_workbook(
        "Eastern Sectional",
        segments,
        competition_start_date="2026-11-03",
        competition_year="2627",
        **kwargs,
    )
    return openpyxl.load_workbook(BytesIO(data))


def _element_row(**overrides):
    row = {
        "skater_segment_id": 1,
        "start_number": 1,
        "skater": "Skater One",
        "element_id": 10,
        "element_number": 1,
        "element": "3F",
        "notes": "F",
        "judge_id": 9,
        "judge_name": "Ann Smith",
        "score": 1,
    }
    row.update(overrides)
    return row


def _pcs_row(**overrides):
    row = {
        "skater_segment_id": 1,
        "start_number": 1,
        "skater": "Skater One",
        "pcs_type_id": 3,
        "component": "Skating Skills",
        "judge_id": 9,
        "judge_name": "Ann Smith",
        "score": 7.5,
    }
    row.update(overrides)
    return row
