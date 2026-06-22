import pandas as pd

from scripts.compare_pcs_benchmark_sigma_pools import (
    JUDGE_DISCIPLINE_GROUPS,
    MARKING_TIER_BOTTOM,
    MARKING_TIER_MIDDLE_HIGH,
    MARKING_TIER_MIDDLE_LOW,
    MARKING_TIER_TOP,
    assign_marking_score_tiers,
    filter_marks_for_discipline_group,
    parse_ranking_scenario,
    ranking_scenarios_from_args,
)


def test_parse_ranking_scenario():
    assert parse_ranking_scenario("all/junior_senior") == ("all", "junior_senior")
    assert parse_ranking_scenario("international/junior_senior") == (
        "international",
        "junior_senior",
    )


def test_ranking_scenarios_default_includes_all_and_international():
    scenarios = ranking_scenarios_from_args(
        ranking_scenarios=None,
        ranking_scope=None,
        ranking_segment_level=None,
        us_directory_only=False,
    )
    slugs = {s.slug for s in scenarios}
    assert "all_junior_senior" in slugs
    assert "international_junior_senior" in slugs


def test_ranking_scenarios_us_directory_duplicates_each():
    scenarios = ranking_scenarios_from_args(
        ranking_scenarios=["international/junior_senior"],
        ranking_scope=None,
        ranking_segment_level=None,
        us_directory_only=True,
    )
    slugs = [s.slug for s in scenarios]
    assert slugs == ["international_junior_senior", "international_junior_senior_us_directory"]


def test_filter_marks_for_discipline_group():
    marks = pd.DataFrame(
        {
            "discipline_type_id": [1, 2, 3, 5, 1],
            "judge_name": ["a", "b", "c", "d", "e"],
        }
    )
    sp = JUDGE_DISCIPLINE_GROUPS[0]
    dance = JUDGE_DISCIPLINE_GROUPS[1]
    filtered = filter_marks_for_discipline_group(marks, sp)
    assert set(filtered["judge_name"]) == {"a", "b", "e"}
    assert filter_marks_for_discipline_group(marks, dance)["judge_name"].tolist() == ["c"]


def test_assign_marking_score_tiers_four_tier_split():
    scores = pd.Series(
        {f"j{i}": float(i) for i in range(1, 11)}
    )
    tiers = assign_marking_score_tiers(
        scores, top_fraction=0.30, bottom_fraction=0.10
    )
    assert set(tiers.loc[["j1", "j2", "j3"]]) == {MARKING_TIER_TOP}
    assert tiers["j10"] == MARKING_TIER_BOTTOM
    assert tiers["j4"] == MARKING_TIER_MIDDLE_HIGH
    assert tiers["j5"] == MARKING_TIER_MIDDLE_HIGH
    assert tiers["j6"] == MARKING_TIER_MIDDLE_HIGH
    assert tiers["j7"] == MARKING_TIER_MIDDLE_LOW
    assert tiers["j8"] == MARKING_TIER_MIDDLE_LOW
    assert tiers["j9"] == MARKING_TIER_MIDDLE_LOW


def test_assign_marking_score_tiers_odd_middle_goes_to_high():
    scores = pd.Series({"a": 0.1, "b": 0.2, "c": 0.3, "d": 0.4, "e": 0.9})
    tiers = assign_marking_score_tiers(
        scores, top_fraction=0.20, bottom_fraction=0.20
    )
    assert tiers["a"] == MARKING_TIER_TOP
    assert tiers["e"] == MARKING_TIER_BOTTOM
    assert tiers["b"] == MARKING_TIER_MIDDLE_HIGH
    assert tiers["c"] == MARKING_TIER_MIDDLE_HIGH
    assert tiers["d"] == MARKING_TIER_MIDDLE_LOW
