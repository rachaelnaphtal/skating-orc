"""
Build the formatted National Singles/Pairs judge analysis Excel workbook.

Mirrors the layout in ``National SP Judge Analysis.xlsx``: group headers, raw
metrics, lookup-based deviation ratings, activity flags, and rollup columns.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import Workbook
from openpyxl.formatting.rule import CellIsRule, FormulaRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

LOOKUP_SHEET = "Lookup table"
RAW_SHEET = "raw_data"
ANALYSIS_SHEET = "analysis"

ANALYSIS_FIRST_DATA_ROW = 4
ANALYSIS_LAST_COL = 40
HIDDEN_ANALYSIS_COLUMNS = ("H", "I", "Q", "R")
ANALYSIS_FREEZE_PANES = "B4"

_THIN = Side(style="thin")
_NO_SIDE = Side()
_ANALYSIS_COLUMN_WIDTHS: dict[str, float] = {
    "A": 21.83,
    "B": 9.33,
    "C": 10.0,
    "D": 13.16,
    "E": 12.83,
    "F": 11.0,
    "G": 17.16,
    "H": 12.83,
    "I": 13.0,
    "J": 13.83,
    "K": 10.83,
    "L": 14.33,
    "M": 10.83,
    "N": 10.83,
    "O": 10.83,
    "P": 10.83,
    "Q": 0.0,
    "R": 0.0,
    "S": 15.0,
    "T": 10.83,
    "U": 13.83,
    "V": 10.83,
    "W": 10.83,
    "X": 10.83,
    "Y": 10.83,
    "Z": 2.0,
    "AA": 10.83,
    "AB": 10.83,
    "AC": 10.83,
    "AD": 10.83,
    "AE": 10.83,
    "AF": 10.83,
    "AG": 10.83,
    "AH": 10.83,
    "AI": 10.83,
    "AJ": 10.83,
    "AK": 10.83,
    "AL": 10.83,
    "AM": 10.83,
    "AN": 10.83,
}
_SECTION_HEADER_LEFT_COLS = frozenset({"G", "H", "Q", "AA", "AE", "AJ"})
_SECTION_HEADER_RIGHT_COLS = frozenset({"G", "P", "Y", "AD", "AI", "AN"})
_DATA_LEFT_BORDER_COLS = frozenset({"G", "AA", "AE", "AJ"})
_DATA_RIGHT_BORDER_COLS = frozenset({"G", "P", "Y", "AD", "AI", "AN"})

_FILL_GREEN = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
_FILL_YELLOW = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
_FILL_RED = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

_OVERALL_SORT_ORDER = {
    "Poor": 0,
    "Fair": 1,
    "Fair/Good": 2,
    "Good": 3,
    "N/A": 4,
}


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _to_number(value: Any) -> float | None:
    if _is_blank(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    num = _to_number(value)
    if num is None:
        return None
    return int(num)


def marking_score_rating(score: Any) -> str | None:
    """VLOOKUP-style rating for PCS/element marking scores."""
    num = _to_number(score)
    if num is None:
        return None
    if num < 0.1:
        return "N/A"
    if num < 1.0:
        return "Good"
    if num < 1.3:
        return "Fair"
    return "Poor"


def _activity_flags(
    total_comps: Any,
    competition_count: Any,
    segment_count: Any,
    junior_senior_segments: Any,
    *,
    international_flag: Any = None,
) -> tuple[str, str, str]:
    is_int = str(international_flag or "").strip().upper() == "X"
    total = "Low" if (_to_int(total_comps) or 0) <= 5 else ""
    qualifying = ""
    if not is_int:
        comps = _to_int(competition_count)
        segments = _to_int(segment_count)
        if (comps is not None and comps < 3) or (segments is not None and segments < 20):
            qualifying = "Low"
    junior_senior = ""
    if not is_int:
        js = _to_int(junior_senior_segments)
        if js is not None and js < 10:
            junior_senior = "Low"
    return total, qualifying, junior_senior


def activity_overall(total: str, qualifying: str, junior_senior: str) -> str:
    count = sum(1 for value in (total, qualifying, junior_senior) if value)
    return {3: "Low", 2: "Fair", 1: "Fair", 0: "Good"}.get(count, "Good")


def qualifying_rule_errors_rating(rule_errors: Any, *, activity_is_low: bool) -> str | None:
    if activity_is_low:
        return "N/A"
    count = _to_int(rule_errors)
    if count is None:
        return None
    if count >= 4:
        return "Poor"
    if count == 3:
        return "Fair"
    if count == 0:
        return "Very Good"
    return "Good"


def qualifying_anomaly_rating(anomaly_pct: Any, *, activity_is_low: bool) -> str | None:
    if activity_is_low:
        return "N/A"
    rate = _to_number(anomaly_pct)
    if rate is None:
        return None
    if rate >= 2:
        return "Poor"
    if rate >= 1:
        return "Fair"
    return "Good"


def champs_rule_errors_rating(rule_errors: Any, *, has_champs: bool) -> str | None:
    if not has_champs:
        return "N/A"
    count = _to_int(rule_errors)
    if count is None:
        return None
    return {3: "Poor", 2: "Fair", 1: "Good", 0: "Very Good"}.get(count)


def champs_anomaly_rating(anomaly_pct: Any, *, has_champs: bool) -> str | None:
    if not has_champs:
        return "N/A"
    rate = _to_number(anomaly_pct)
    if rate is None:
        return None
    if rate >= 2:
        return "Poor"
    if rate >= 1:
        return "Fair"
    return "Good"


def performance_overall(
    *ratings: str | None,
    any_na_is_overall_na: bool = True,
    all_na_is_overall_na: bool = False,
) -> str | None:
    present = [value for value in ratings if value not in (None, "")]
    if not present:
        return None
    na_count = sum(1 for value in present if value == "N/A")
    if all_na_is_overall_na and na_count == len(present):
        return "N/A"
    if any_na_is_overall_na and na_count > 0:
        return "N/A"
    values = [value for value in present if value != "N/A"]
    if not values:
        return "N/A"
    poor = sum(1 for value in values if value == "Poor")
    good = sum(1 for value in values if value in {"Good", "Very Good"})
    fair = sum(1 for value in values if value == "Fair")
    if poor >= 2:
        return "Poor"
    if good == len(values):
        return "Good"
    if poor == 1:
        return "Fair"
    if fair > 0 and good > 0:
        return "Fair/Good"
    return "Fair"


def _analysis_sort_key(row: pd.Series) -> tuple[Any, ...]:
    total_comps = _to_int(row.get("total_comps_in_role_3yr"))
    comp_count = _to_int(row.get("competition_count"))
    seg_count = _to_int(row.get("segment_count"))
    js_count = _to_int(row.get("junior_senior_segment_count"))
    total_act, qual_act, js_act = _activity_flags(
        total_comps,
        comp_count,
        seg_count,
        js_count,
        international_flag="X" if row.get("international_judge") else None,
    )
    act_overall = activity_overall(total_act, qual_act, js_act)
    act_low = act_overall == "Low"

    elem_rating = marking_score_rating(row.get("element_marking_score"))
    pcs_rating = marking_score_rating(row.get("pcs_marking_score"))
    if act_low:
        elem_dev = pcs_dev = "N/A"
    else:
        elem_dev = elem_rating
        pcs_dev = pcs_rating

    overall = performance_overall(
        qualifying_rule_errors_rating(row.get("total_rule_errors"), activity_is_low=act_low),
        qualifying_anomaly_rating(row.get("anomaly_rate_pct"), activity_is_low=act_low),
        elem_dev,
        pcs_dev,
    )

    champs_count = _to_int(row.get("champs_competition_count")) or 0
    has_champs = champs_count > 0
    champs_overall = performance_overall(
        champs_rule_errors_rating(row.get("champs_total_rule_errors"), has_champs=has_champs),
        champs_anomaly_rating(row.get("champs_anomaly_rate_pct"), has_champs=has_champs),
        marking_score_rating(row.get("champs_element_marking_score"))
        if has_champs
        else "N/A",
        marking_score_rating(row.get("champs_pcs_marking_score")) if has_champs else "N/A",
        any_na_is_overall_na=False,
        all_na_is_overall_na=True,
    )

    name = str(row.get("directory_name") or "")
    rule_errors = _to_int(row.get("total_rule_errors"))
    anomaly = _to_number(row.get("anomaly_rate_pct"))
    return (
        _OVERALL_SORT_ORDER.get(overall, 99),
        -(rule_errors if rule_errors is not None else -1),
        -(anomaly if anomaly is not None else -1),
        _OVERALL_SORT_ORDER.get(champs_overall, 99),
        name.casefold(),
    )


def analysis_row_order(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df.copy()
    out = raw_df.copy()
    out["_analysis_sort"] = out.apply(_analysis_sort_key, axis=1)
    out = out.sort_values("_analysis_sort").drop(columns=["_analysis_sort"])
    return out.reset_index(drop=True)


def _write_lookup_sheet(wb: Workbook) -> None:
    ws = wb.create_sheet(LOOKUP_SHEET)
    ws["A1"] = "Element Marking Score"
    ws["D1"] = "PCS Marking Score"
    rows = [
        (0, "N/A", 0, "N/A"),
        (0.1, "Good", 0.1, "Good"),
        (1, "Fair", 1, "Fair"),
        (1.3, "Poor", 1.3, "Poor"),
    ]
    for idx, row in enumerate(rows, start=2):
        ws.cell(idx, 1, row[0])
        ws.cell(idx, 2, row[1])
        ws.cell(idx, 4, row[2])
        ws.cell(idx, 5, row[3])


def _write_raw_sheet(wb: Workbook, raw_df: pd.DataFrame) -> None:
    ws = wb.create_sheet(RAW_SHEET)
    for col_idx, column in enumerate(raw_df.columns, start=1):
        ws.cell(1, col_idx, column)
    for row_idx, row in enumerate(raw_df.itertuples(index=False), start=2):
        for col_idx, value in enumerate(row, start=1):
            if _is_blank(value):
                continue
            ws.cell(row_idx, col_idx, value)


def _set_analysis_headers(ws) -> None:
    ws["G1"] = "Raw Data"
    ws["AA1"] = "Analysis"
    ws["G2"] = "Last 3 years"
    ws["H2"] = "Qualifying Competition Performance (Past three years)"
    ws["Q2"] = "Champs (since 2018 for GOEs and 2022 for PCS)"
    ws["AA2"] = "Activity Analysis"
    ws["AE2"] = "Qualifying Performance Analysis"
    ws["AJ2"] = "Champs Performance Analysis"

    headers = [
        "Name",
        "Int? (current or recent)",
        "USFS #",
        "US Champs (Senior) Availability",
        "Appointment Year",
        "Last Champs in Role",
        "Total Comps (3 years) in Role",
        "PCS Marking Score",
        "Element Marking Score",
        "# Competitions",
        "# Segments",
        "# Junior/Senior Segments",
        "# Rule errors",
        "Anomaly %",
        "Element Dev Score",
        "PCS Dev Score",
        "PCS Marking Score",
        "Element Marking Score",
        "# Champs",
        "# Segments",
        "# Junior/Senior Segments",
        "# Rule errors",
        "Anomaly %",
        "Element Dev Score",
        "PCS Dev Score",
        None,
        "Total Activity",
        "Qualifying Activity",
        "Jr/Senior Activity",
        "Activity Overall",
        "Rule Errors",
        "Anomalies",
        "Element Deviation",
        "PCS Deviation",
        "Overall",
        "Rule Errors",
        "Anomalies",
        "Element Deviation",
        "PCS Deviation",
        "Overall",
    ]
    for col_idx, header in enumerate(headers, start=1):
        if header is not None:
            ws.cell(3, col_idx, header)

    bold = Font(bold=True)
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    group_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for row in (1, 2, 3):
        for col in range(1, ANALYSIS_LAST_COL + 1):
            cell = ws.cell(row, col)
            if cell.value:
                cell.font = bold
                cell.alignment = group_align if row < 3 else header_align

    ws.merge_cells("G1:P1")
    ws.merge_cells("AA1:AN1")
    ws.merge_cells("H2:P2")
    ws.merge_cells("Q2:Y2")
    ws.merge_cells("AA2:AD2")
    ws.merge_cells("AE2:AI2")
    ws.merge_cells("AJ2:AN2")

    for letter in HIDDEN_ANALYSIS_COLUMNS:
        ws.column_dimensions[letter].hidden = True

    ws.row_dimensions[1].height = 19
    ws.row_dimensions[2].height = 19
    ws.row_dimensions[3].height = 51
    ws.sheet_format.defaultRowHeight = 16


def _border(
    *,
    left: Side | None = None,
    top: Side | None = None,
    right: Side | None = None,
    bottom: Side | None = None,
) -> Border:
    return Border(
        left=left or _NO_SIDE,
        top=top or _NO_SIDE,
        right=right or _NO_SIDE,
        bottom=bottom or _NO_SIDE,
    )


def _apply_group_header_borders(ws) -> None:
    ws["G1"].border = _border(
        left=_THIN, top=_THIN, right=_THIN, bottom=_THIN
    )
    ws["AA1"].border = _border(
        left=_THIN, top=_THIN, right=_THIN, bottom=_THIN
    )
    for addr in ("H2", "Q2", "AA2", "AE2"):
        ws[addr].border = _border(left=_THIN, right=_THIN, bottom=_THIN)
    ws["AJ2"].border = _border(left=_THIN, right=_THIN)


def _apply_header_row_borders(ws) -> None:
    for col in range(1, 7):
        cell = ws.cell(3, col)
        if cell.value:
            cell.border = _border(top=_THIN, bottom=_THIN)

    for col in range(7, ANALYSIS_LAST_COL + 1):
        cell = ws.cell(3, col)
        if not cell.value:
            continue
        letter = get_column_letter(col)
        cell.border = _border(
            left=_THIN if letter in _SECTION_HEADER_LEFT_COLS else None,
            top=_THIN,
            right=_THIN if letter in _SECTION_HEADER_RIGHT_COLS else None,
            bottom=_THIN,
        )


def _apply_data_row_borders(ws, *, last_row: int) -> None:
    if last_row < ANALYSIS_FIRST_DATA_ROW:
        return
    for row in range(ANALYSIS_FIRST_DATA_ROW, last_row + 1):
        for col in range(1, ANALYSIS_LAST_COL + 1):
            letter = get_column_letter(col)
            left = _THIN if letter in _DATA_LEFT_BORDER_COLS else None
            right = _THIN if letter in _DATA_RIGHT_BORDER_COLS else None
            if left or right:
                ws.cell(row, col).border = _border(left=left, right=right)


def _apply_analysis_column_widths(ws) -> None:
    for col in range(1, ANALYSIS_LAST_COL + 1):
        letter = get_column_letter(col)
        width = _ANALYSIS_COLUMN_WIDTHS.get(letter, 10.83)
        ws.column_dimensions[letter].width = width


def _apply_analysis_sheet_layout(ws, *, last_row: int) -> None:
    _apply_analysis_column_widths(ws)
    _apply_group_header_borders(ws)
    _apply_header_row_borders(ws)
    _apply_data_row_borders(ws, last_row=last_row)
    ws.freeze_panes = ANALYSIS_FREEZE_PANES


def _write_analysis_value(ws, row: int, col: int, value: Any) -> None:
    if _is_blank(value):
        return
    ws.cell(row, col, value)


def _write_analysis_formulas(ws, row: int) -> None:
    r = row
    ws[f"O{r}"] = f"=VLOOKUP(I{r},'{LOOKUP_SHEET}'!A$2:B$5,2,TRUE)"
    ws[f"P{r}"] = f"=VLOOKUP(H{r},'{LOOKUP_SHEET}'!D$2:E$5,2,TRUE)"
    ws[f"X{r}"] = f"=VLOOKUP(R{r},'{LOOKUP_SHEET}'!A$2:B$5,2,TRUE)"
    ws[f"Y{r}"] = f"=VLOOKUP(Q{r},'{LOOKUP_SHEET}'!D$2:E$5,2,TRUE)"

    ws[f"AA{r}"] = f'=IF(G{r}<=5,"Low","")'
    ws[f"AB{r}"] = f'=IF(AND(OR(J{r}<3,K{r}<20),NOT(B{r}="X")),"Low","")'
    ws[f"AC{r}"] = f'=IF(AND(OR(L{r}<10),NOT(B{r}="X")),"Low","")'
    ws[f"AD{r}"] = (
        f'=IF(COUNTIF(AA{r}:AC{r},"?*")=3,"Low",'
        f'IF(COUNTIF(AA{r}:AC{r},"?*")=2,"Fair",'
        f'IF(COUNTIF(AA{r}:AC{r},"?*")=1,"Fair","Good")))'
    )

    ws[f"AE{r}"] = (
        f'=IF(AD{r}="Low","N/A",'
        f'IF(M{r}>=4,"Poor",IF(M{r}=3,"Fair",IF(M{r}=0,"Very Good","Good"))))'
    )
    ws[f"AF{r}"] = (
        f'=IF(AD{r}="Low","N/A",'
        f'IF(N{r}>=2,"Poor",IF(N{r}>=1,"Fair","Good")))'
    )
    ws[f"AG{r}"] = f'=IF(AD{r}="Low","N/A",O{r})'
    ws[f"AH{r}"] = f'=IF(AD{r}="Low","N/A",P{r})'
    ws[f"AI{r}"] = (
        f'=IF(COUNTIF(AE{r}:AH{r},"N/A")>0,"N/A",'
        f'IF(COUNTIF(AE{r}:AH{r},"Poor")>=2,"Poor",'
        f'IF(COUNTIF(AE{r}:AH{r},"Good")+COUNTIF(AE{r}:AH{r},"Very Good")=4,"Good",'
        f'IF(COUNTIF(AE{r}:AH{r},"Poor")=1,"Fair",'
        f'IF(AND(COUNTIF(AE{r}:AH{r},"Fair")>0,'
        f'COUNTIF(AE{r}:AH{r},"Good")+COUNTIF(AE{r}:AH{r},"Very Good")>0),'
        f'"Fair/Good","Fair")))))'
    )

    ws[f"AJ{r}"] = (
        f'=IF(S{r}=0,"N/A",'
        f'IF(V{r}=3,"Poor",IF(V{r}=2,"Fair",IF(V{r}=1,"Good","Very Good"))))'
    )
    ws[f"AK{r}"] = (
        f'=IF(S{r}=0,"N/A",IF(W{r}>=2,"Poor",IF(W{r}>=1,"Fair","Good")))'
    )
    ws[f"AL{r}"] = f'=IF(S{r}=0,"N/A",X{r})'
    ws[f"AM{r}"] = f'=IF(S{r}=0,"N/A",Y{r})'
    ws[f"AN{r}"] = (
        f'=IF(COUNTIF(AJ{r}:AM{r},"N/A")=4,"N/A",'
        f'IF(COUNTIF(AJ{r}:AM{r},"Poor")>=2,"Poor",'
        f'IF(COUNTIF(AJ{r}:AM{r},"Good")+COUNTIF(AJ{r}:AM{r},"Very Good")=4,"Good",'
        f'IF(COUNTIF(AJ{r}:AM{r},"Poor")=1,"Fair",'
        f'IF(AND(COUNTIF(AJ{r}:AM{r},"Fair")>0,'
        f'COUNTIF(AJ{r}:AM{r},"Good")+COUNTIF(AJ{r}:AM{r},"Very Good")>0),'
        f'"Fair/Good","Fair")))))'
    )


def _write_analysis_row(ws, row: int, record: pd.Series) -> None:
    _write_analysis_value(ws, row, 1, record.get("directory_name"))
    if record.get("international_judge"):
        _write_analysis_value(ws, row, 2, "X")
    _write_analysis_value(ws, row, 3, record.get("mbr_number"))
    _write_analysis_value(ws, row, 4, record.get("us_champs_senior_availability"))
    _write_analysis_value(ws, row, 5, record.get("appointment_year"))
    _write_analysis_value(ws, row, 6, record.get("last_champs_in_role"))
    _write_analysis_value(ws, row, 7, record.get("total_comps_in_role_3yr"))

    _write_analysis_value(ws, row, 8, record.get("pcs_marking_score"))
    _write_analysis_value(ws, row, 9, record.get("element_marking_score"))
    _write_analysis_value(ws, row, 10, record.get("competition_count"))
    _write_analysis_value(ws, row, 11, record.get("segment_count"))
    _write_analysis_value(ws, row, 12, record.get("junior_senior_segment_count"))
    _write_analysis_value(ws, row, 13, record.get("total_rule_errors"))
    _write_analysis_value(ws, row, 14, record.get("anomaly_rate_pct"))

    _write_analysis_value(ws, row, 17, record.get("champs_pcs_marking_score"))
    _write_analysis_value(ws, row, 18, record.get("champs_element_marking_score"))
    champs_comps = _to_int(record.get("champs_competition_count")) or 0
    _write_analysis_value(ws, row, 19, champs_comps)
    _write_analysis_value(ws, row, 20, record.get("champs_segment_count"))
    _write_analysis_value(ws, row, 21, record.get("champs_junior_senior_segment_count"))
    _write_analysis_value(ws, row, 22, record.get("champs_total_rule_errors"))
    _write_analysis_value(ws, row, 23, record.get("champs_anomaly_rate_pct"))

    _write_analysis_formulas(ws, row)


def _rating_text_rules(top_left: str) -> list[FormulaRule]:
    """Good/Very Good green, Fair yellow, Poor red (matches manual workbook)."""
    col = top_left.rstrip("0123456789")
    row = top_left[len(col) :]
    cell = f"{col}{row}"
    return [
        FormulaRule(
            formula=[f'NOT(ISERROR(SEARCH("Poor",{cell})))'],
            stopIfTrue=True,
            fill=_FILL_RED,
        ),
        FormulaRule(
            formula=[f'NOT(ISERROR(SEARCH("Fair/Good",{cell})))'],
            stopIfTrue=True,
            fill=_FILL_YELLOW,
        ),
        FormulaRule(
            formula=[f'NOT(ISERROR(SEARCH("Good",{cell})))'],
            stopIfTrue=True,
            fill=_FILL_GREEN,
        ),
        FormulaRule(
            formula=[f'NOT(ISERROR(SEARCH("Fair",{cell})))'],
            stopIfTrue=True,
            fill=_FILL_YELLOW,
        ),
        FormulaRule(
            formula=[f'NOT(ISERROR(SEARCH("Low",{cell})))'],
            stopIfTrue=True,
            fill=_FILL_RED,
        ),
    ]


def _anomaly_rules(top_left: str) -> list:
    col = top_left.rstrip("0123456789")
    row = top_left[len(col) :]
    cell = f"{col}{row}"
    return [
        FormulaRule(
            formula=[f"AND(ISNUMBER({cell}),{cell}>=2)"],
            stopIfTrue=True,
            fill=_FILL_RED,
        ),
        FormulaRule(
            formula=[f"AND(ISNUMBER({cell}),{cell}>=1,{cell}<2)"],
            stopIfTrue=True,
            fill=_FILL_YELLOW,
        ),
        FormulaRule(
            formula=[f"AND(ISNUMBER({cell}),{cell}<1)"],
            stopIfTrue=True,
            fill=_FILL_GREEN,
        ),
    ]


def _apply_analysis_conditional_formatting(ws, *, last_row: int) -> None:
    if last_row < ANALYSIS_FIRST_DATA_ROW:
        return
    first = ANALYSIS_FIRST_DATA_ROW
    cf = ws.conditional_formatting

    def addr(col: str) -> str:
        return f"{col}{first}:{col}{last_row}"

    cf.add(
        addr("D"),
        FormulaRule(
            formula=[f'LEFT(D{first},LEN("Available"))="Available"'],
            stopIfTrue=True,
            fill=_FILL_GREEN,
        ),
    )
    cf.add(
        addr("D"),
        FormulaRule(
            formula=[f'NOT(ISERROR(SEARCH("Unavailable",D{first})))'],
            stopIfTrue=True,
            fill=_FILL_RED,
        ),
    )
    cf.add(
        addr("D"),
        FormulaRule(
            formula=[f'NOT(ISERROR(SEARCH("Didn\'t reply",D{first})))'],
            stopIfTrue=True,
            fill=_FILL_YELLOW,
        ),
    )

    cf.add(addr("G"), CellIsRule(operator="lessThanOrEqual", formula=["5"], fill=_FILL_RED))
    cf.add(addr("J"), CellIsRule(operator="lessThan", formula=["3"], fill=_FILL_RED))
    cf.add(addr("K"), CellIsRule(operator="lessThan", formula=["20"], fill=_FILL_RED))
    cf.add(addr("L"), CellIsRule(operator="lessThan", formula=["10"], fill=_FILL_RED))
    cf.add(addr("M"), CellIsRule(operator="greaterThanOrEqual", formula=["4"], fill=_FILL_RED))
    cf.add(addr("V"), CellIsRule(operator="greaterThanOrEqual", formula=["3"], fill=_FILL_RED))

    for col in ("N", "W"):
        for rule in _anomaly_rules(f"{col}{first}"):
            cf.add(addr(col), rule)

    for col in ("O", "P", "X", "Y"):
        for rule in _rating_text_rules(f"{col}{first}"):
            cf.add(addr(col), rule)

    for col in ("AA", "AB", "AC", "AD"):
        for rule in _rating_text_rules(f"{col}{first}"):
            cf.add(addr(col), rule)

    for col in ("AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM", "AN"):
        for rule in _rating_text_rules(f"{col}{first}"):
            cf.add(addr(col), rule)


def write_national_sp_judge_analysis_xlsx(raw_df: pd.DataFrame, output_path: Path) -> None:
    """Write formatted workbook with analysis, raw data, and lookup sheets."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    wb.remove(wb.active)

    ws = wb.create_sheet(ANALYSIS_SHEET)
    _set_analysis_headers(ws)

    _write_raw_sheet(wb, raw_df)
    _write_lookup_sheet(wb)

    ordered = analysis_row_order(raw_df)
    for offset, (_, record) in enumerate(ordered.iterrows()):
        row = ANALYSIS_FIRST_DATA_ROW + offset
        _write_analysis_row(ws, row, record)

    last_row = ANALYSIS_FIRST_DATA_ROW + len(ordered) - 1 if len(ordered) else ANALYSIS_FIRST_DATA_ROW
    _apply_analysis_sheet_layout(ws, last_row=last_row)
    _apply_analysis_conditional_formatting(ws, last_row=last_row)
    wb.save(output_path)
