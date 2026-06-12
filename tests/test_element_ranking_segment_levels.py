from element_deviation_ranking import (
    ELEMENT_RANKING_LEVEL_FILTER_ALL,
    ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
    ELEMENT_RANKING_LEVEL_FILTER_NOVICE_JUNIOR_SENIOR,
    ELEMENT_RANKING_LEVEL_FILTER_PRESETS,
    benchmark_scope_kwargs_from_run_params,
    benchmark_segment_level_preset,
    ranking_scope_kwargs_from_run_params,
    segment_levels_for_ranking_preset,
)
from element_ranking_cache import build_precompute_element_ranking_run_params
from segment_level import (
    LEVEL_ADVANCED_NOVICE,
    LEVEL_EXCEL_JUNIOR,
    LEVEL_INTERNATIONAL,
    LEVEL_JUNIOR,
    LEVEL_JUVENILE,
    LEVEL_NOVICE,
    LEVEL_SENIOR,
)


def test_segment_levels_all_is_unfiltered():
    assert segment_levels_for_ranking_preset(None) is None
    assert segment_levels_for_ranking_preset(ELEMENT_RANKING_LEVEL_FILTER_ALL) is None


def test_segment_levels_novice_junior_senior_exact_labels_only():
    levels = segment_levels_for_ranking_preset(
        ELEMENT_RANKING_LEVEL_FILTER_NOVICE_JUNIOR_SENIOR
    )
    assert levels == frozenset(
        {LEVEL_NOVICE, LEVEL_ADVANCED_NOVICE, LEVEL_JUNIOR, LEVEL_SENIOR}
    )
    assert LEVEL_JUVENILE not in levels
    assert LEVEL_EXCEL_JUNIOR not in levels
    assert LEVEL_INTERNATIONAL not in levels


def test_segment_levels_junior_senior_only():
    levels = segment_levels_for_ranking_preset(
        ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR
    )
    assert levels == frozenset({LEVEL_JUNIOR, LEVEL_SENIOR})


def test_precompute_run_params_carry_segment_level_preset():
    rp = build_precompute_element_ranking_run_params(
        "qualifying",
        segment_level_preset=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
    )
    assert rp[12] == ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR
    assert rp[13] == ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR
    assert len(ELEMENT_RANKING_LEVEL_FILTER_PRESETS) == 3


def test_benchmark_segment_level_preset_can_differ_from_ranking():
    run_params = (
        None,
        None,
        None,
        "qualifying",
        None,
        None,
        0,
        0.05,
        30,
        None,
        None,
        "qualifying",
        ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
        ELEMENT_RANKING_LEVEL_FILTER_NOVICE_JUNIOR_SENIOR,
    )
    assert ranking_scope_kwargs_from_run_params(run_params)[
        "segment_level_preset"
    ] == ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR
    assert benchmark_segment_level_preset(run_params) == (
        ELEMENT_RANKING_LEVEL_FILTER_NOVICE_JUNIOR_SENIOR
    )
    assert benchmark_scope_kwargs_from_run_params(run_params)[
        "segment_level_preset"
    ] == ELEMENT_RANKING_LEVEL_FILTER_NOVICE_JUNIOR_SENIOR


def test_benchmark_all_levels_distinct_from_ranking_junior_senior():
    run_params = (
        None,
        None,
        None,
        "qualifying",
        None,
        None,
        0,
        0.05,
        30,
        None,
        None,
        "qualifying",
        ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
        None,
    )
    assert ranking_scope_kwargs_from_run_params(run_params)[
        "segment_level_preset"
    ] == ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR
    assert benchmark_segment_level_preset(run_params) is None
    assert benchmark_scope_kwargs_from_run_params(run_params)[
        "segment_level_preset"
    ] is None


def test_legacy_run_params_default_benchmark_levels_to_ranking():
    run_params = (
        None,
        None,
        None,
        "qualifying",
        None,
        None,
        0,
        0.05,
        30,
        None,
        None,
        "qualifying",
        ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
    )
    assert benchmark_segment_level_preset(run_params) == (
        ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR
    )
