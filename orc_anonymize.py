"""Replace official names in an ORC workbook with stable Judge 1, Judge 2 labels."""

from __future__ import annotations

import re
from io import BytesIO

import openpyxl
from openpyxl.styles import Font
from openpyxl.workbook.workbook import Workbook

# Segment sheets label a row as ``J3- Ann Smith`` (panel seat, then the official).
_PANEL_LABEL = re.compile(r"^J(\d+)- (.+)$")
_ANONYMOUS_LABEL = re.compile(r"^Judge (\d+)$")
_SUMMARY_SHEET = "Summary"
_SUMMARY_NAME_ROW = 6


def assign_anonymous_judge_labels(names) -> dict[str, str]:
    """
    Map each distinct official name to ``Judge 1``, ``Judge 2``, and so on.

    Labels follow case-insensitive alphabetical order. A name that is already
    ``Judge N`` keeps that number, and that number is not given to anyone else.
    """
    reserved: set[int] = set()
    unique: list[str] = []
    seen: set[str] = set()
    for name in names:
        if not isinstance(name, str):
            continue
        if not name.strip() or name in seen:
            continue
        seen.add(name)
        already = _ANONYMOUS_LABEL.match(name)
        if already:
            reserved.add(int(already.group(1)))
            continue
        unique.append(name)
    unique.sort(key=str.casefold)

    mapping: dict[str, str] = {}
    number = 1
    for name in unique:
        while number in reserved:
            number += 1
        mapping[name] = f"Judge {number}"
        reserved.add(number)
        number += 1
    return mapping


def judge_names_from_orc_workbook(workbook: Workbook) -> list[str]:
    """Official names printed as ``J{seat}- {name}`` on segment sheets."""
    found: list[str] = []
    for sheet in workbook.worksheets:
        for row in sheet.iter_rows():
            for cell in row:
                if not isinstance(cell.value, str):
                    continue
                match = _PANEL_LABEL.match(cell.value)
                if match:
                    found.append(match.group(2))
    return found


def judge_names_from_frames(*frames) -> list[str]:
    """Names in a ``Judge Name`` column of the additional-analysis frames."""
    found: list[str] = []
    for frame in frames:
        if frame is None or getattr(frame, "empty", True):
            continue
        if "Judge Name" not in getattr(frame, "columns", []):
            continue
        for value in frame["Judge Name"].dropna().unique():
            if isinstance(value, str) and value.strip():
                found.append(value)
    return found


def anonymize_orc_workbook(workbook: Workbook, mapping: dict[str, str]) -> None:
    """
    Show ``Judge N`` wherever an official's name is printed.

    Segment rows drop the panel-seat prefix so the same person has one label
    on every sheet. Skater names are left as written, even when they match
    an official.
    """
    for sheet in workbook.worksheets:
        bare_names = sheet.title == _SUMMARY_SHEET
        for row in sheet.iter_rows():
            for cell in row:
                value = cell.value
                if not isinstance(value, str):
                    continue
                match = _PANEL_LABEL.match(value)
                if match:
                    official = match.group(2)
                    if official in mapping:
                        cell.value = mapping[official]
                    elif _ANONYMOUS_LABEL.match(official):
                        cell.value = official
                    continue
                if (
                    bare_names
                    and cell.row == _SUMMARY_NAME_ROW
                    and cell.column >= 4
                    and value in mapping
                ):
                    cell.value = mapping[value]


def anonymize_judge_name_columns(workbook: Workbook, mapping: dict[str, str]) -> None:
    """Replace the ``Judge Name`` column in an additional-analysis workbook."""
    if not mapping:
        return
    for sheet in workbook.worksheets:
        header = next(sheet.iter_rows(min_row=1, max_row=1), None)
        if header is None:
            continue
        for cell in header:
            if cell.value != "Judge Name":
                continue
            for (data_cell,) in sheet.iter_rows(
                min_row=2, min_col=cell.column, max_col=cell.column
            ):
                if isinstance(data_cell.value, str) and data_cell.value in mapping:
                    data_cell.value = mapping[data_cell.value]


def remap_judge_name_column(frame, mapping: dict[str, str]):
    """Return a copy of ``frame`` with ``Judge Name`` values relabeled."""
    if not mapping or frame is None or getattr(frame, "empty", True):
        return frame
    if "Judge Name" not in getattr(frame, "columns", []):
        return frame
    relabeled = frame.copy()
    relabeled["Judge Name"] = relabeled["Judge Name"].map(
        lambda value: mapping.get(value, value)
    )
    return relabeled


def relabel_official_names(text: str, mapping: dict[str, str]) -> str:
    """Replace official names in a warning. Longer names are replaced first."""
    if not text or not mapping:
        return text
    pattern = "|".join(re.escape(name) for name in sorted(mapping, key=len, reverse=True))
    placeholders: list[str] = []

    def _hold(match: re.Match) -> str:
        placeholders.append(mapping[match.group(0)])
        return f"\x00{len(placeholders) - 1}\x00"

    held = re.sub(pattern, _hold, text)
    for index, label in enumerate(placeholders):
        held = held.replace(f"\x00{index}\x00", label)
    return held


def build_judge_key_workbook(mapping: dict[str, str]) -> Workbook:
    """Workbook that maps each ``Judge N`` label back to the official's name."""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Judge Key"
    sheet["A1"] = "Label"
    sheet["B1"] = "Official Name"
    sheet["A1"].font = Font(bold=True)
    sheet["B1"].font = Font(bold=True)
    rows = sorted(mapping.items(), key=lambda item: _label_number(item[1]))
    for index, (official, label) in enumerate(rows, start=2):
        sheet.cell(index, 1, value=label)
        sheet.cell(index, 2, value=official)
    sheet.column_dimensions["A"].width = 16
    sheet.column_dimensions["B"].width = 36
    sheet.auto_filter.ref = f"A1:B{max(len(rows) + 1, 1)}"
    sheet.freeze_panes = "A2"
    return workbook


def judge_key_workbook_bytes(mapping: dict[str, str]) -> bytes:
    return _workbook_bytes(build_judge_key_workbook(mapping))


def anonymize_report_workbooks(
    main: bytes,
    additional: bytes | None,
    names,
) -> tuple[bytes, bytes | None, bytes, dict[str, str]]:
    """
    Relabel ``main`` and ``additional`` and build the key workbook.

    ``names`` supplements the officials found in the workbooks (for example
    judges who were skipped and only appear in a warning).
    """
    main_book = openpyxl.load_workbook(BytesIO(main))
    collected = list(names) + judge_names_from_orc_workbook(main_book)
    additional_book = openpyxl.load_workbook(BytesIO(additional)) if additional else None
    if additional_book is not None:
        collected.extend(_judge_names_from_additional_workbook(additional_book))
    mapping = assign_anonymous_judge_labels(collected)
    anonymize_orc_workbook(main_book, mapping)
    if additional_book is not None:
        anonymize_judge_name_columns(additional_book, mapping)
    return (
        _workbook_bytes(main_book),
        _workbook_bytes(additional_book) if additional_book is not None else None,
        judge_key_workbook_bytes(mapping),
        mapping,
    )


def _judge_names_from_additional_workbook(workbook: Workbook) -> list[str]:
    found: list[str] = []
    for sheet in workbook.worksheets:
        header = next(sheet.iter_rows(min_row=1, max_row=1), None)
        if header is None:
            continue
        for cell in header:
            if cell.value != "Judge Name":
                continue
            for (data_cell,) in sheet.iter_rows(
                min_row=2, min_col=cell.column, max_col=cell.column
            ):
                if isinstance(data_cell.value, str) and data_cell.value.strip():
                    found.append(data_cell.value)
    return found


def _label_number(label: str) -> int:
    match = _ANONYMOUS_LABEL.match(label or "")
    if not match:
        return 10**9
    return int(match.group(1))


def _workbook_bytes(workbook: Workbook) -> bytes:
    payload = BytesIO()
    workbook.save(payload)
    return payload.getvalue()
