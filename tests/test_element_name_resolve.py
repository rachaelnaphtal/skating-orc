from database_loader import (
    _element_name_candidates_from_rule_error,
    _resolve_element_dict_key,
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
