from judgingParsing import (
    compute_element_max_goe,
    element_protocol_metadata_from_parsed,
    element_should_store_max_goe,
)


def test_compute_max_goe_fall_in_notes():
    assert compute_element_max_goe("3Lz", "F", "Men_Short_Program") == -3


def test_compute_max_goe_double_under():
    assert compute_element_max_goe("3Lz<<", None, "Men_Free_Skating") == -1


def test_compute_max_goe_clean_element():
    assert compute_element_max_goe("3Lz", None, "Men_Free_Skating") == 5


def test_should_store_when_notes_present():
    assert element_should_store_max_goe("3Lz", "e", "")


def test_should_not_store_clean_jump():
    assert not element_should_store_max_goe("3Lz", None, "Men_Free_Skating")


def test_protocol_metadata_includes_max_when_marked():
    parsed = {
        "Skater A": [{"Element": "3Lz", "Notes": "F"}],
        "Skater B": [{"Element": "2A", "Notes": None}],
    }
    meta = element_protocol_metadata_from_parsed(parsed, "Men_Short_Program")
    assert meta[("Skater A", "3Lz")]["max_goe_allowed"] == -3
    assert "max_goe_allowed" not in meta[("Skater B", "2A")]
