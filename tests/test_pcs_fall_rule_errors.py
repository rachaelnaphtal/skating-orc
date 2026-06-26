from decimal import Decimal

from pcs_fall_rule_errors import (
    detect_pcs_fall_rule_errors,
    falls_from_element_notes,
    max_pcs_for_fall_count,
    pcs_score_exceeds_fall_limit,
    program_fall_count_from_elements,
)


def test_falls_from_element_notes_single_f():
    assert falls_from_element_notes("F") == 1
    assert falls_from_element_notes("qF") == 1


def test_falls_from_element_notes_fx():
    assert falls_from_element_notes("Fx") == 2
    assert falls_from_element_notes("fx") == 2


def test_falls_from_element_notes_fx_not_double_counted():
    assert falls_from_element_notes("Fx") == 2
    assert falls_from_element_notes("F") != 2


def test_program_fall_count_sums_elements():
    elements = [
        {"Notes": "F"},
        {"Notes": "Fx"},
        {"Notes": None},
    ]
    assert program_fall_count_from_elements(elements) == 3


def test_max_pcs_for_fall_count():
    assert max_pcs_for_fall_count(0) is None
    assert max_pcs_for_fall_count(1) == Decimal("9.5")
    assert max_pcs_for_fall_count(2) == Decimal("8.75")
    assert max_pcs_for_fall_count(5) == Decimal("8.75")


def test_pcs_score_exceeds_fall_limit():
    assert pcs_score_exceeds_fall_limit(9.75, Decimal("9.5"))
    assert not pcs_score_exceeds_fall_limit(9.5, Decimal("9.5"))
    assert not pcs_score_exceeds_fall_limit(9.25, Decimal("9.5"))


def test_detect_pcs_fall_rule_errors_one_fall():
    elements = {"Alice": [{"Element": "3Lz", "Notes": "F"}]}
    pcs = {
        "Alice": [
            {
                "Component": "Skating Skills",
                "Scores": [9.75, 9.25, 9.0],
            }
        ]
    }
    judges = ["Judge A", "Judge B", "Judge C"]
    errors = detect_pcs_fall_rule_errors(
        elements,
        pcs,
        judges,
        "Men_Short_Program",
        competition_year="2425",
    )
    assert len(errors) == 1
    assert errors[0]["Judge Name"] == "Judge A"
    assert errors[0]["Component"] == "Skating Skills"
    assert errors[0]["Fall Count"] == 1


def test_detect_pcs_fall_rule_errors_two_falls():
    elements = {"Alice": [{"Element": "3Lz", "Notes": "Fx"}]}
    pcs = {
        "Alice": [
            {
                "Component": "Composition",
                "Scores": [9.0, 8.5, 8.25],
            }
        ]
    }
    judges = ["Judge A", "Judge B", "Judge C"]
    errors = detect_pcs_fall_rule_errors(
        elements,
        pcs,
        judges,
        "Women_Free_Skating",
        competition_year="2526",
    )
    assert len(errors) == 1
    assert errors[0]["Judge Name"] == "Judge A"
    assert errors[0]["Fall Count"] == 2


def test_detect_ice_dance_segment_with_fall():
    elements = {"Alice": [{"Element": "MiStW2", "Notes": "F"}]}
    pcs = {"Alice": [{"Component": "Skating Skills", "Scores": [9.75]}]}
    errors = detect_pcs_fall_rule_errors(
        elements,
        pcs,
        ["Judge A"],
        "Ice_Dance___Rhythm_Dance",
        competition_year="2526",
    )
    assert len(errors) == 1


def test_detect_synchronized_segment_with_fall():
    elements = {"Team A": [{"Element": "Ee4", "Notes": "F"}]}
    pcs = {"Team A": [{"Component": "Presentation", "Scores": [9.75]}]}
    errors = detect_pcs_fall_rule_errors(
        elements,
        pcs,
        ["Judge A"],
        "Senior_Synchronized_Skating___Short_Program",
        competition_year="2526",
    )
    assert len(errors) == 1


def test_detect_skips_pre_2425_season():
    elements = {"Alice": [{"Element": "3Lz", "Notes": "F"}]}
    pcs = {"Alice": [{"Component": "Skating Skills", "Scores": [10.0]}]}
    errors = detect_pcs_fall_rule_errors(
        elements,
        pcs,
        ["Judge A"],
        "Men_Short_Program",
        competition_year="2324",
    )
    assert errors == []
