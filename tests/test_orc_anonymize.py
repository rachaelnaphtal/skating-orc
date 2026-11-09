"""Anonymous Judge 1 / Judge 2 labels on ORC workbooks."""

from io import BytesIO

import openpyxl

from competition_orc_report import build_orc_workbook
from orc_anonymize import (
    anonymize_report_workbooks,
    assign_anonymous_judge_labels,
    relabel_official_names,
)


def test_a_skater_who_shares_an_official_name_stays_named():
    main, _extra = build_orc_workbook(
        "Eastern Sectional",
        [
            (
                "JUNIOR_MEN_SHORT_PROGRAM",
                ["Ann Smith", "Bob Smith"],
                {"Ann Smith": [_element(scores=[1, -5])]},
                {},
            )
        ],
        competition_start_date="2026-11-03",
        competition_year="2627",
    )
    main, _extra, _key, _mapping = anonymize_report_workbooks(
        main, None, ["Ann Smith", "Bob Smith"]
    )
    sheet = _segment_sheet(openpyxl.load_workbook(BytesIO(main)), "SHORT")
    assert sheet["A6"].value == "Judge 1"
    assert sheet["D6"].value == "Ann Smith"


def test_labels_follow_alphabetical_order_and_keep_placeholder_numbers():
    mapping = assign_anonymous_judge_labels(
        ["bob jones", "Ann Smith", "Judge 2", "Ann Smith", "  ", None]
    )
    assert mapping == {"Ann Smith": "Judge 1", "bob jones": "Judge 3"}


def test_warning_replaces_the_longer_official_name_first():
    text = relabel_official_names(
        "Scoring judges not listed as Judge No. were added after the panel: Ann Smith.",
        {"Ann": "Judge 9", "Ann Smith": "Judge 1"},
    )
    assert text.endswith("Judge 1.")
    assert "Ann Smith" not in text
    assert "Judge 9" not in text


def test_anonymized_workbook_uses_one_label_per_official_and_writes_a_key():
    main, extra = build_orc_workbook(
        "Eastern Sectional",
        [
            (
                "JUNIOR_MEN_SHORT_PROGRAM",
                ["Ann Smith", "Bob Smith"],
                {"Skater One": [_element(scores=[1, 2])]},
                {},
            ),
            (
                "JUNIOR_MEN_FREE_SKATING",
                ["Bob Smith", "Ann Smith"],
                {"Skater Two": [_element(scores=[2, 1])]},
                {},
            ),
        ],
        add_additional_analysis=True,
        competition_start_date="2026-11-03",
        competition_year="2627",
    )
    main, extra, key, mapping = anonymize_report_workbooks(
        main,
        extra,
        ["Ann Smith", "Bob Smith"],
    )
    assert mapping == {"Ann Smith": "Judge 1", "Bob Smith": "Judge 2"}

    book = openpyxl.load_workbook(BytesIO(main))
    short = _segment_sheet(book, "SHORT")
    free = _segment_sheet(book, "FREE")
    assert short["A6"].value == "Judge 1"
    assert short["A7"].value == "Judge 2"
    assert short["D6"].value == "Skater One"
    assert free["A6"].value == "Judge 2"
    assert free["A7"].value == "Judge 1"
    assert book["Summary"]["D6"].value == "Judge 1"
    assert book["Summary"]["H6"].value == "Judge 2"
    assert _cell_text(book).isdisjoint({"Ann Smith", "Bob Smith"})
    assert not any(value.startswith("J1-") or value.startswith("J2-") for value in _cell_text(book))

    extra_book = openpyxl.load_workbook(BytesIO(extra))
    assert set(_column_values(extra_book["All Elements"], "Judge Name")) == {
        "Judge 1",
        "Judge 2",
    }
    assert _cell_text(extra_book).isdisjoint({"Ann Smith", "Bob Smith"})

    key_book = openpyxl.load_workbook(BytesIO(key))
    sheet = key_book["Judge Key"]
    assert [sheet["A2"].value, sheet["B2"].value] == ["Judge 1", "Ann Smith"]
    assert [sheet["A3"].value, sheet["B3"].value] == ["Judge 2", "Bob Smith"]


def _element(scores):
    return {"Element": "3F", "Notes": "F", "Number": 1, "Scores": scores}


def _segment_sheet(workbook, token):
    return next(sheet for sheet in workbook.worksheets if token in sheet.title)


def _cell_text(workbook) -> set[str]:
    values = set()
    for sheet in workbook.worksheets:
        for row in sheet.iter_rows():
            for cell in row:
                if isinstance(cell.value, str):
                    values.add(cell.value)
    return values


def _column_values(sheet, header: str) -> list[str]:
    headers = [cell.value for cell in next(sheet.iter_rows(min_row=1, max_row=1))]
    column = headers.index(header) + 1
    values = []
    for (cell,) in sheet.iter_rows(min_row=2, min_col=column, max_col=column):
        if isinstance(cell.value, str):
            values.append(cell.value)
    return values
