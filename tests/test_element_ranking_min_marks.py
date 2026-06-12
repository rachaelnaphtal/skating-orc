import pandas as pd

from element_deviation_ranking import (
    _enforce_min_marks_on_marking_display,
    apply_min_marks_to_ranking_result,
    build_ranking_display_table,
)


def test_apply_min_marks_from_marking_display_when_summary_pool_missing():
    judge_all = pd.DataFrame(
        [
            {
                "rank": 1,
                "judge_name": "Low volume",
                "n_marks": 8,
                "marking_score": 0.9,
                "mean_error": 0.1,
                "mean_abs_error": 0.2,
                "mean_sigma_hat": 0.3,
                "mean_abs_m": 0.4,
            },
            {
                "rank": 2,
                "judge_name": "High volume",
                "n_marks": 200,
                "marking_score": 0.4,
                "mean_error": 0.0,
                "mean_abs_error": 0.1,
                "mean_sigma_hat": 0.2,
                "mean_abs_m": 0.2,
            },
        ]
    )
    marking = build_ranking_display_table(judge_all)
    result = {"marking": marking, "summary": pd.DataFrame()}
    filtered = apply_min_marks_to_ranking_result(result, 150)
    assert filtered.get("_min_marks_filter_applied") is True
    assert len(filtered["marking"]) == 1
    assert filtered["marking"].iloc[0]["Judge"] == "High volume"
    assert int(filtered["marking"].iloc[0]["Element marks"]) == 200


def test_apply_min_marks_prefers_result_pool_over_stale_session_pool():
    judge_level_filtered = pd.DataFrame(
        [
            {
                "rank": 1,
                "judge_name": "Judge A",
                "n_marks": 80,
                "marking_score": 0.5,
                "mean_error": 0.0,
                "mean_abs_error": 0.1,
                "mean_sigma_hat": 0.2,
                "mean_abs_m": 0.2,
            }
        ]
    )
    stale_pool = pd.DataFrame(
        [
            {
                "rank": 1,
                "judge_name": "Judge A",
                "n_marks": 220,
                "marking_score": 0.5,
                "mean_error": 0.0,
                "mean_abs_error": 0.1,
                "mean_sigma_hat": 0.2,
                "mean_abs_m": 0.2,
            }
        ]
    )
    marking = build_ranking_display_table(judge_level_filtered)
    result = {
        "marking": marking,
        "summary": pd.DataFrame(),
        "judge_summary_all": judge_level_filtered,
    }
    filtered = apply_min_marks_to_ranking_result(
        result, 150, judge_summary_all=stale_pool
    )
    assert filtered.get("_min_marks_filter_applied") is True
    assert filtered["marking"].empty


def test_enforce_min_marks_on_marking_display():
    marking = pd.DataFrame(
        [
            {"rank": 1, "Judge": "A", "Marking score": 0.4, "Element marks": 200},
            {"rank": 2, "Judge": "B", "Marking score": 0.6, "Element marks": 80},
        ]
    )
    kept = _enforce_min_marks_on_marking_display(marking, 150)
    assert len(kept) == 1
    assert kept.iloc[0]["Judge"] == "A"
