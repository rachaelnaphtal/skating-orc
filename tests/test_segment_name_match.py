from judgingParsing import (
    find_segment_match_key,
    ijs_event_label_to_db_segment_name,
    segment_name_match_key,
)


def test_match_key_collapses_nepela_index_vs_db_names():
    index_name = ijs_event_label_to_db_segment_name("Men - Short Program")
    assert index_name == "Men___Short_Program"
    assert segment_name_match_key("MEN_SHORT_PROGRAM") == segment_name_match_key(index_name)


def test_match_key_olympic_full_name():
    full = ijs_event_label_to_db_segment_name("Men Single Skating - Short Program")
    assert segment_name_match_key(full) == segment_name_match_key("MEN_SHORT_PROGRAM")


def test_match_key_women_free():
    assert segment_name_match_key("WOMEN_FREE_SKATING") == segment_name_match_key(
        ijs_event_label_to_db_segment_name("Women - Free Skating")
    )


def test_match_key_normalizes_index_womwn_typo():
    db = "(103_203)_Intermediate_Women_Grp_C_Short_Program"
    index_typo = "103_203_INTERMEDIATE_WOMWN_GRP_C_SHORT_PROGRAM"
    assert segment_name_match_key(db) == segment_name_match_key(index_typo)


def test_find_segment_match_key_fuzzy_womwn_typo():
    db = "(103_203)_Intermediate_Women_Grp_C_Short_Program"
    index_keys = [
        "103_203_INTERMEDIATE_WOMWN_GRP_C_SHORT_PROGRAM",
        "103_203_INTERMEDIATE_WOMWN_GRP_C_FREE_SKATE",
    ]
    assert (
        find_segment_match_key(db, index_keys)
        == "103_203_INTERMEDIATE_WOMWN_GRP_C_SHORT_PROGRAM"
    )
