"""Tests for duplicate judge merge planning."""

from judge_merge import JudgeMergeStats, format_merge_stats


def test_format_merge_stats():
    stats = JudgeMergeStats(
        keeper_id=341,
        duplicate_id=2784,
        match_key="karen wolanchuk",
        keeper_name="Karen Wolanchuk",
        duplicate_name="Karen WOLANCHUK",
        repointed_element_scores=2002,
        deleted_element_scores=0,
    )
    text = format_merge_stats(stats)
    assert "341" in text
    assert "2784" in text
    assert "elem repoint=2002" in text
