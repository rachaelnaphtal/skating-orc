from analytics import (
    _identity_label_for_merged_jids,
    _person_names_equivalent_for_display,
    _union_find_clusters,
    _union_find_union,
)


def test_union_find_merges_us_and_isu_clusters():
    parent = {1: 1, 2: 2, 3: 3, 4: 4}
    _union_find_union(parent, 1, 2)  # same US official
    _union_find_union(parent, 2, 3)  # same ISU official (transitive)
    clusters = _union_find_clusters(parent, {1, 2, 3, 4})
    assert sorted(clusters) == [[1, 2, 3]]


def test_identity_label_uses_directory_name_and_aliases():
    judge_map = {
        1006: ("Chris Buchanan", ""),
        1013: ("Chris BUCHANAN", ""),
    }
    label = _identity_label_for_merged_jids(
        [1006, 1013],
        judge_map,
        directory_name="Christopher Buchanan",
        fallback_suffix=" (same ISU roster official)",
    )
    assert label == "Christopher Buchanan"


def test_person_names_equivalent_nickname_and_case():
    assert _person_names_equivalent_for_display(
        "Christopher Buchanan", "Chris BUCHANAN"
    )
    assert _person_names_equivalent_for_display("Chris Buchanan", "Chris BUCHANAN")
    assert not _person_names_equivalent_for_display(
        "Christopher Buchanan", "Jane Buchanan"
    )


def test_person_names_equivalent_reversed_order():
    assert _person_names_equivalent_for_display(
        "Christian Baumann", "BAUMANN Christian"
    )
    assert _person_names_equivalent_for_display("Agita ABELE", "ABELE Agita")


def test_identity_label_omits_reversed_protocol_alias():
    judge_map = {
        1: ("Christian Baumann", ""),
        2: ("BAUMANN Christian", ""),
    }
    label = _identity_label_for_merged_jids(
        [1, 2],
        judge_map,
        directory_name="Christian Baumann",
        fallback_suffix="",
    )
    assert label == "Christian Baumann"


def test_identity_label_omits_case_only_alias():
    judge_map = {
        1006: ("Chris Buchanan", ""),
        1013: ("Chris BUCHANAN", ""),
    }
    label = _identity_label_for_merged_jids(
        [1006, 1013],
        judge_map,
        directory_name="",
        fallback_suffix="",
    )
    assert label == "Chris Buchanan"
