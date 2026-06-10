from judgingParsing import (
    align_element_scores_to_judges,
    create_all_element_dict,
    create_all_pcs_dict,
    detect_element_rule_errors,
    findElementDeviations,
    findPCSDeviations,
    parse_fsm_element_judge_scores,
)
from ijs_scrape_log import pop_warnings, reset_warnings, flush_parsing_issues


def test_parse_fsm_element_judge_scores_dash_column():
    scores, missing = parse_fsm_element_judge_scores("2 2 2 2 - 3 1 2")
    assert missing == 4
    assert scores == [2.0, 2.0, 2.0, 2.0, None, 3.0, 1.0, 2.0]


def test_create_all_element_dict_inserts_at_judge_five():
    judges = ["J1", "J2", "J3", "J4", "J5", "J6", "J7", "J8", "J9"]
    elements_per_skater = {
        "Adeliia PETROSIAN": [
            {
                "Element": "3Lz+2T",
                "Scores": [2, 2, 2, 2, 3, 1, 2, 1],
                "Possible Missing Position": 4,
            },
        ],
    }
    rows = create_all_element_dict(
        judges, elements_per_skater, "WOMEN_FREE_SKATING"
    )
    assert len(rows) == 8
    by_judge = {r["Judge Name"]: r["Score"] for r in rows}
    assert "J5" not in by_judge
    assert by_judge["J6"] == 3
    assert by_judge["J4"] == 2


def test_create_all_pcs_dict_handles_none_missing_position():
    from judgingParsing import create_all_pcs_dict

    judges = ["J1", "J2", "J3", "J4", "J5", "J6", "J7", "J8", "J9"]
    pcs_per_skater = {
        "Skater A": [
            {"Component": "Composition", "Scores": [7.5, 7.25, 7.0, 7.5, 7.0, 7.25, 7.5, 7.0]},
        ],
    }
    rows = create_all_pcs_dict(judges, pcs_per_skater, "WOMEN_FREE_SKATING")
    assert len(rows) >= 7


def test_all_dash_failed_element_does_not_warn():
    """Legacy FSM ``A`` rows are all ``-``; not a parse failure."""
    reset_warnings()
    judges = [f"J{i}" for i in range(1, 13)]
    elements_per_skater = {
        "Nobunari ODA": [
            {
                "Element": "A",
                "Scores": [None] * 12,
                "Possible Missing Position": 0,
                "Notes": None,
            },
            {
                "Element": "3Lz+3T",
                "Scores": [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 2.0],
                "Notes": None,
            },
        ],
    }
    detect_element_rule_errors(
        elements_per_skater, judges, "Men___Short_Program"
    )
    findElementDeviations(elements_per_skater, judges, event_name="Men___Short_Program")
    create_all_element_dict(
        judges, elements_per_skater, "Men___Short_Program"
    )
    flush_parsing_issues("Men___Short_Program")
    assert pop_warnings() == []


def test_explicit_dash_column_does_not_warn():
    """2007 pairs PDFs use ``-`` for the non-scoring panel column."""
    reset_warnings()
    judges = [f"J{i}" for i in range(1, 13)]
    dash_scores = [2.0, 2.0, 1.0, None, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0]
    elements_per_skater = {
        "Xue SHEN / Hongbo ZHAO": [
            {
                "Element": "3T",
                "Scores": dash_scores,
                "Possible Missing Position": 3,
                "Notes": None,
            },
        ],
    }
    pcs_per_skater = {
        "Xue SHEN / Hongbo ZHAO": [
            {
                "Component": "Skating Skills",
                "Scores": [7.0, 7.25, 7.5, None, 7.25, 7.0, 7.25, 7.5, 7.0, 7.25, 7.5, 7.0],
                "Possible Missing Position": 3,
            },
        ],
    }
    findElementDeviations(elements_per_skater, judges, event_name="Pairs___Short_Program")
    findPCSDeviations(pcs_per_skater, judges, event_name="Pairs___Short_Program")
    create_all_element_dict(
        judges, elements_per_skater, "Pairs___Short_Program", pcs_per_skater
    )
    create_all_pcs_dict(judges, pcs_per_skater, "Pairs___Short_Program")
    flush_parsing_issues("Pairs___Short_Program")
    assert pop_warnings() == []


def test_align_element_scores_uses_skater_consensus():
    aligned = align_element_scores_to_judges(
        [2, 2, 2, 2, 3, 1, 2, 1],
        9,
        missing_index=4,
        event_name="WOMEN_FREE_SKATING",
        skater="Test Skater",
        element_label="3Lz+2T",
    )
    assert len(aligned) == 9
    assert aligned[4] is None
    assert aligned[5] == 3
