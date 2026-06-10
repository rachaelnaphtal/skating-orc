"""FSM protocol element-line parsing (dash judges, element-number sync)."""

from judgingParsing import (
    _fsm_is_protocol_junk_line,
    _fsm_normalize_pdf_text,
    _fsm_strip_element_number,
    create_all_element_dict,
    match_element_fsm,
    match_pcs_fsm,
    match_skater_fsm,
    parse_fsm_element_judge_scores,
)


def test_fsm_is_protocol_junk_line_skips_reversed_info_header():
    assert _fsm_is_protocol_junk_line("ofnI") is True
    assert _fsm_is_protocol_junk_line("ofnI Base Scores of GOE J1 J2 J3") is True
    assert _fsm_is_protocol_junk_line("Base Scores of") is True
    assert _fsm_is_protocol_junk_line("Value Panel") is True
    assert _fsm_is_protocol_junk_line("# Executed Elements") is True
    assert _fsm_is_protocol_junk_line("77.64 88.69") is True
    assert _fsm_is_protocol_junk_line("1 4Lz 11.50 1.64 2 2 2 2 0 0 0 2 2 13.14") is False


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


def test_match_skater_fsm_legacy_isu_noc_with_deductions():
    m = match_skater_fsm("1 Brian JOUBERT FRA 84.40 46.00 38.40 0.00")
    assert m is not None
    assert m.group(2).strip() == "Brian JOUBERT"
    assert m.group(3) == "FRA"
    assert float(m.group(5)) == 46.00


def test_match_element_fsm_twelve_judge_panel_with_dashes():
    line = "4T+3T 13.80 -3.20 -2 -3 -2 -2 -2 -3 -2 -2 -2 - - - 10.60"
    m = match_element_fsm(line)
    assert m is not None
    assert m.group(1) == "4T+3T"
    assert float(m.group(8)) == 10.60


def test_match_pcs_fsm_twelve_judge_panel_with_dashes():
    line = (
        "Skating Skills 0.75 7.50 8.00 7.50 7.75 8.25 8.00 7.75 8.00 8.50 "
        "- - - 7.85"
    )
    m = match_pcs_fsm(line)
    assert m is not None
    assert m.group(1) == "Skating Skills"
    assert float(m.group(4)) == 7.85


def test_match_skater_fsm_legacy_isu_noc_without_start_number():
    m = match_skater_fsm("1 Yuzuru HANYU JPN 110.56 61.52 49.04")
    assert m is not None
    assert m.group(2).strip() == "Yuzuru HANYU"
    assert m.group(3) == "JPN"
    assert float(m.group(5)) == 61.52


def test_match_skater_fsm_legacy_pairs_with_slash_name():
    m = match_skater_fsm(
        "1 Gabriella PAPADAKIS / Guillaume CIZERON FRA 76.29 38.46 37.83"
    )
    assert m is not None
    assert m.group(2).strip() == "Gabriella PAPADAKIS / Guillaume CIZERON"
    assert m.group(3) == "FRA"
    assert float(m.group(5)) == 38.46


def test_match_pcs_fsm_modern_singles_components():
    for line, component, total in [
        (
            "Composition 2.67 9.50 9.75 9.25 9.50 9.75 9.25 9.00 9.50 9.75 9.57",
            "Composition",
            9.57,
        ),
        (
            "Presentation 2.67 9.25 9.75 9.75 9.75 9.75 9.50 9.25 9.25 9.75 9.57",
            "Presentation",
            9.57,
        ),
        (
            "Skating Skills 2.67 9.75 10.00 9.75 9.50 10.00 10.00 10.00 9.75 10.00 9.89",
            "Skating Skills",
            9.89,
        ),
    ]:
        m = match_pcs_fsm(line)
        assert m is not None, line
        assert m.group(1) == component
        assert float(m.group(4)) == total


def test_match_pcs_fsm_legacy_2016_singles_and_pairs_components():
    lines = [
        (
            "Skating Skills 1.00 9.75 10.00 9.75 9.50 10.00 10.00 10.00 9.75 10.00 9.89",
            "Skating Skills",
        ),
        (
            "Transition / Linking Footwork 1.00 9.75 9.75 9.50 9.50 9.50 9.75 9.75 9.50 9.50 9.61",
            "Transition / Linking Footwork",
        ),
        (
            "Performance / Execution 1.00 10.00 10.00 10.00 9.75 10.00 10.00 10.00 9.75 9.75 9.93",
            "Performance / Execution",
        ),
        (
            "Choreography / Composition 1.00 10.00 10.00 9.75 9.50 9.75 9.50 10.00 9.75 9.75 9.79",
            "Choreography / Composition",
        ),
        (
            "Interpretation 1.00 10.00 9.75 10.00 9.50 9.75 9.75 10.00 9.75 9.75 9.82",
            "Interpretation",
        ),
    ]
    for line, component in lines:
        m = match_pcs_fsm(line)
        assert m is not None, line
        assert m.group(1) == component


def test_match_pcs_fsm_legacy_2016_ice_dance_components():
    lines = [
        (
            "Skating Skills 0.80 8.75 9.75 9.25 9.50 9.75 9.25 9.00 9.50 9.75 9.43",
            "Skating Skills",
        ),
        (
            "Linking Footwork / Movement 0.80 8.50 9.00 9.50 9.50 9.75 9.25 9.00 9.50 9.50 9.32",
            "Linking Footwork / Movement",
        ),
        (
            "Performance 0.80 9.25 9.75 9.75 9.75 9.75 9.50 9.25 9.25 9.75 9.57",
            "Performance",
        ),
        (
            "Choreography 0.80 9.00 9.50 9.50 9.25 9.50 9.25 9.00 9.25 9.50 9.28",
            "Choreography",
        ),
        (
            "Interpretation / Timing 0.80 9.25 9.50 9.50 9.25 9.50 9.25 9.00 9.25 9.50 9.28",
            "Interpretation / Timing",
        ),
    ]
    for line, component in lines:
        m = match_pcs_fsm(line)
        assert m is not None, line
        assert m.group(1) == component


def test_infer_panel_judge_names_when_panel_empty():
    from judgingParsing import infer_panel_judge_names_from_parsed_scores

    elements = {
        "Skater A": [{"Element": "3Lz", "Scores": [2, 1, 0, -1, 2, 2, 1]}],
    }
    assert infer_panel_judge_names_from_parsed_scores(elements, {}, []) == [
        "Judge 1",
        "Judge 2",
        "Judge 3",
        "Judge 4",
        "Judge 5",
        "Judge 6",
        "Judge 7",
    ]
    assert infer_panel_judge_names_from_parsed_scores(elements, {}, ["Ann"]) == ["Ann"]


def test_all_dash_element_rows_skip_database_scores():
    """Parsed dash columns become None; no element_score_per_judge rows are built."""
    judges = ["J1", "J2", "J3", "J4", "J5"]
    rows = create_all_element_dict(
        judges,
        {"Skater": [{"Element": "Lz*", "Scores": [None] * 5, "Possible Missing Position": 0}]},
        "TEST",
    )
    assert rows == []
