from database_loader import (
    _element_name_candidates_from_rule_error,
    _resolve_element_dict_key,
    _resolve_element_id,
    assign_repeated_elements,
)


def test_element_name_candidates_merges_split_asterisks():
    candidates = _element_name_candidates_from_rule_error(
        "2F+2T+1Lo<<+2Lo<<* *"
    )
    assert "2F+2T+1Lo<<+2Lo<<**" in candidates
    assert "2F+2T+1Lo<<+2Lo<<*" in candidates


def test_element_name_candidates_keeps_base_when_notes_are_separate():
    candidates = _element_name_candidates_from_rule_error("3Lz+2T F")
    assert candidates[0] == "3Lz+2T F"
    assert "3Lz+2T" in candidates


def test_resolve_element_dict_key_split_asterisks():
    elem_id_by_pair = {(99, "2F+2T+1Lo<<+2Lo<<**"): 501}
    assert (
        _resolve_element_dict_key(
            elem_id_by_pair, 99, "2F+2T+1Lo<<+2Lo<<* *"
        )
        == "2F+2T+1Lo<<+2Lo<<**"
    )


def test_resolve_element_dict_key_exact():
    elem_id_by_pair = {(1, "3A"): 10}
    assert _resolve_element_dict_key(elem_id_by_pair, 1, "3A") == "3A"


def test_resolve_element_id_uses_program_number_when_name_repeats():
    elem_id_by_pair = {
        (7, "2Lze", 3): 100,
        (7, "2Lze", 8): 200,
    }
    assert _resolve_element_id(elem_id_by_pair, 7, "2Lze", 3) == 100
    assert _resolve_element_id(elem_id_by_pair, 7, "2Lze", 8) == 200
    assert _resolve_element_id(elem_id_by_pair, 7, "2Lze", None) is None


def test_assign_repeated_elements_adopts_unnumbered_row_then_inserts():
    incoming = [(4, "2Lze", 3), (4, "2Lze", 8), (4, "2Lo", 7)]
    existing = [(1457427, 4, "2Lze", None), (1457431, 4, "2Lo", None)]
    reuse, to_insert = assign_repeated_elements(incoming, existing)
    assert reuse[(4, "2Lze", 3)] == 1457427
    assert reuse[(4, "2Lo", 7)] == 1457431
    assert to_insert == [(4, "2Lze", 8)]


def test_assign_repeated_elements_matches_existing_program_numbers():
    incoming = [(4, "2Lze", 3), (4, "2Lze", 8)]
    existing = [(10, 4, "2Lze", 8), (11, 4, "2Lze", 3)]
    reuse, to_insert = assign_repeated_elements(incoming, existing)
    assert reuse == {(4, "2Lze", 3): 11, (4, "2Lze", 8): 10}
    assert to_insert == []
