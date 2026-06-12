import pandas as pd

from element_deviation_ranking import apply_min_marks_to_ranking_result, build_ranking_display_table
from judge_official_display_filter import apply_us_officials_display_filter_to_ranking_result


def test_us_officials_filter_preserves_minimum_marks_threshold():
    judge_all = pd.DataFrame(
        [
            {
                "rank": 1,
                "judge_name": "US High volume",
                "n_marks": 200,
                "marking_score": 0.4,
                "mean_error": 0.0,
                "mean_abs_error": 0.1,
                "mean_sigma_hat": 0.2,
                "mean_abs_m": 0.2,
            },
            {
                "rank": 2,
                "judge_name": "US Low volume",
                "n_marks": 80,
                "marking_score": 0.5,
                "mean_error": 0.0,
                "mean_abs_error": 0.1,
                "mean_sigma_hat": 0.2,
                "mean_abs_m": 0.2,
            },
            {
                "rank": 3,
                "judge_name": "Foreign High volume",
                "n_marks": 300,
                "marking_score": 0.3,
                "mean_error": 0.0,
                "mean_abs_error": 0.1,
                "mean_sigma_hat": 0.2,
                "mean_abs_m": 0.2,
            },
        ]
    )
    base = {
        "marking": build_ranking_display_table(judge_all),
        "summary": pd.DataFrame(),
        "judge_summary_all": judge_all,
    }
    filtered = apply_min_marks_to_ranking_result(base, 150)
    us_only = apply_us_officials_display_filter_to_ranking_result(
        filtered,
        {"US High volume", "US Low volume"},
    )
    assert len(us_only["marking"]) == 1
    assert us_only["marking"].iloc[0]["Judge"] == "US High volume"
    assert int(us_only["marking"].iloc[0]["Element marks"]) >= 150
