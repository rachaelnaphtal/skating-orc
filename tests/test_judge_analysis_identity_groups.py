from analytics import (
    _identity_label_for_merged_jids,
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
    assert label == "Christopher Buchanan · Chris Buchanan"
