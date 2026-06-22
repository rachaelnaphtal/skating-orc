"""Tests for National SP judge analysis Excel export."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import load_workbook

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts.national_sp_judge_analysis_xlsx import (  # noqa: E402
    ANALYSIS_FREEZE_PANES,
    LOOKUP_SHEET,
    RAW_SHEET,
    ANALYSIS_SHEET,
    _anomaly_rules,
    analysis_row_order,
    marking_score_rating,
    performance_overall,
    write_national_sp_judge_analysis_xlsx,
)
from scripts.report_national_singles_pairs_judge_errors import (  # noqa: E402
    _season_years_for_listing,
)


def test_season_years_for_listing_2627_default_window():
    assert _season_years_for_listing(2627, 3) == ["2526", "2425", "2324"]


def test_marking_score_rating_thresholds():
    assert marking_score_rating(0.05) == "N/A"
    assert marking_score_rating(0.5) == "Good"
    assert marking_score_rating(1.1) == "Fair"
    assert marking_score_rating(1.5) == "Poor"


def test_performance_overall_any_na():
    assert performance_overall("Poor", "N/A", "Good", "Fair") == "N/A"


def test_analysis_row_order_puts_poor_first():
    df = pd.DataFrame(
        [
            {
                "directory_name": "Good Judge",
                "total_comps_in_role_3yr": 20,
                "competition_count": 10,
                "segment_count": 50,
                "junior_senior_segment_count": 30,
                "total_rule_errors": 0,
                "anomaly_rate_pct": 0.5,
                "pcs_marking_score": 0.5,
                "element_marking_score": 0.5,
                "champs_competition_count": 0,
            },
            {
                "directory_name": "Poor Judge",
                "total_comps_in_role_3yr": 20,
                "competition_count": 10,
                "segment_count": 50,
                "junior_senior_segment_count": 30,
                "total_rule_errors": 5,
                "anomaly_rate_pct": 2.5,
                "pcs_marking_score": 1.5,
                "element_marking_score": 1.5,
                "champs_competition_count": 0,
            },
        ]
    )
    ordered = analysis_row_order(df)
    assert ordered.iloc[0]["directory_name"] == "Poor Judge"


def test_anomaly_rule_formulas_use_thresholds():
    formulas = [rule.formula[0] for rule in _anomaly_rules("N4")]
    assert any(">=2" in f for f in formulas)
    assert any(">=1" in f and "<2" in f for f in formulas)
    assert any("<1" in f for f in formulas)


def test_write_workbook_layout(tmp_path: Path):
    df = pd.DataFrame(
        [
            {
                "directory_name": "Example Judge",
                "international_judge": True,
                "mbr_number": "123456",
                "us_champs_senior_availability": "Available",
                "appointment_year": 2010,
                "last_champs_in_role": 2020,
                "total_comps_in_role_3yr": 12,
                "pcs_marking_score": 1.05,
                "element_marking_score": 0.95,
                "competition_count": 4,
                "segment_count": 25,
                "junior_senior_segment_count": 15,
                "total_rule_errors": 2,
                "anomaly_rate_pct": 1.2,
                "champs_pcs_marking_score": None,
                "champs_element_marking_score": None,
                "champs_competition_count": 0,
                "champs_segment_count": 0,
                "champs_junior_senior_segment_count": 0,
                "champs_total_rule_errors": 0,
                "champs_anomaly_rate_pct": None,
            }
        ]
    )
    out = tmp_path / "report.xlsx"
    write_national_sp_judge_analysis_xlsx(df, out)

    wb = load_workbook(out, data_only=False)
    assert wb.sheetnames == [ANALYSIS_SHEET, RAW_SHEET, LOOKUP_SHEET]

    ws = wb[ANALYSIS_SHEET]
    assert ws["A3"].value == "Name"
    assert ws["G2"].value == "Last 3 years"
    assert ws["A4"].value == "Example Judge"
    assert ws["B4"].value == "X"
    assert ws["C4"].value == "123456"
    assert ws["O4"].value == f"=VLOOKUP(I4,'{LOOKUP_SHEET}'!A$2:B$5,2,TRUE)"
    assert ws["AI4"].value.startswith("=IF(COUNTIF(AE4:AH4")
    assert ws["AD4"].value.startswith("=IF(COUNTIF(AA4:AC4")
    assert "@" not in str(ws["AD4"].value)
    assert ws.column_dimensions["H"].hidden is True
    assert ws.column_dimensions["I"].hidden is True
    assert ws.column_dimensions["Q"].hidden is True
    assert ws.column_dimensions["R"].hidden is True
    assert ws["AA3"].value == "Total Activity"
    assert ws.cell(3, 27).value == "Total Activity"
    assert ws.freeze_panes == ANALYSIS_FREEZE_PANES
    assert ws.row_dimensions[3].height == 51
    assert ws["A3"].alignment.wrap_text is True
    assert ws.column_dimensions["J"].width == 13.83
    assert ws["G4"].border.left.style == "thin"
    assert ws["AA4"].border.left.style == "thin"
    assert len(ws.conditional_formatting) > 0

    wb_values = load_workbook(out, data_only=False)
    raw_ws = wb_values[RAW_SHEET]
    assert raw_ws["A1"].value == "directory_name"
    assert raw_ws["A2"].value == "Example Judge"
