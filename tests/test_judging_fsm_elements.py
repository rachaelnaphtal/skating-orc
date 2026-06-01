"""FSM protocol element-line parsing (dash judges, element-number sync)."""

from judgingParsing import (
    _fsm_normalize_pdf_text,
    _fsm_strip_element_number,
    create_all_element_dict,
    match_element_fsm,
    match_skater_fsm,
    parse_fsm_element_judge_scores,
)


def test_match_element_fsm_parses_all_dash_judge_columns():
    line = " Lz* * 0.00 0.00 - - - - - 0.00"
    m = match_element_fsm(line)
    assert m is not None
    assert m.group(1) == "Lz*"
    scores, missing = parse_fsm_element_judge_scores(m.group(6))
    assert scores == [None] * 5
    assert missing == 0
    assert float(m.group(8)) == 0.00


def test_match_element_fsm_csp4_line():
    line = " CSp4 2.60 -0.09 0 0 -1 -1 0 2.51"
    m = match_element_fsm(line)
    assert m is not None
    assert m.group(1) == "CSp4"
    scores, _ = parse_fsm_element_judge_scores(m.group(6))
    assert scores == [0.0, 0.0, -1.0, -1.0, 0.0]


def test_fsm_strip_element_number_catches_up_by_more_than_one():
    rest, num, jumped = _fsm_strip_element_number(
        "7 CSp4 2.60 -0.09 0 0 -1 -1 0 2.51", 5
    )
    assert jumped is True
    assert num == 7
    assert rest.startswith("CSp4")


def test_fsm_strip_ignores_tes_total_line():
    rest, num, jumped = _fsm_strip_element_number("38.50 49.26", 9)
    assert jumped is False
    assert num == 9
    assert rest == "38.50 49.26"


def test_fsm_strip_single_gap_is_silent_sync():
    """PDF element 6 when counter is 5: sync without treating as a skipped-element gap."""
    _rest, num, jumped = _fsm_strip_element_number("6 SlLi4 5.45 1.03 2 3 2 2 3 1 2 2 2 6.48", 5)
    assert jumped is False
    assert num == 6


def test_fsm_element_sequence_lz_stsq_csp():
    """Simulate counter after elements 1–3 with Lz dash row then StSq and CSp."""
    element_number = 4
    rows = [
        "4 Lz* * 0.00 0.00 - - - - - 0.00",
        "5 StSq1 1.80 -0.48 0 -3 -3 -3 -2 1.32",
        "7 CSp4 2.60 -0.09 0 0 -1 -1 0 2.51",
    ]
    parsed = []
    for full in rows:
        line, element_number, _jumped = _fsm_strip_element_number(full, element_number)
        m = match_element_fsm(line)
        assert m is not None, full
        parsed.append((element_number, m.group(1)))
        element_number += 1

    assert parsed == [(4, "Lz*"), (5, "StSq1"), (7, "CSp4")]


def test_match_element_fsm_unicode_minus_goe():
    line = _fsm_normalize_pdf_text(" CSp4 2.60 −0.09 0 0 -1 -1 0 2.51")
    assert match_element_fsm(line) is not None


def test_match_skater_fsm_unicode_name_and_noc():
    m = match_skater_fsm("7 Boglárka ZHANG HUN 24 87.79 41.73 46.06 0.00")
    assert m is not None
    assert m.group(2).strip() == "Boglárka ZHANG HUN"
    assert m.group(3) == "24"
    assert float(m.group(5)) == 41.73


def test_all_dash_element_rows_skip_database_scores():
    """Parsed dash columns become None; no element_score_per_judge rows are built."""
    judges = ["J1", "J2", "J3", "J4", "J5"]
    rows = create_all_element_dict(
        judges,
        {"Skater": [{"Element": "Lz*", "Scores": [None] * 5, "Possible Missing Position": 0}]},
        "TEST",
    )
    assert rows == []
