import pandas as pd

from element_deviation_ranking import (
    compute_judge_summaries,
    compute_mergeable_judge_summary,
    merge_mergeable_judge_summaries,
)


def test_merge_mergeable_judge_summaries_matches_direct():
    work = pd.DataFrame(
        {
            "judge_name": ["A", "A", "B", "B", "B"],
            "m_pj": [0.2, -0.2, 0.5, 0.5, -0.5],
            "error": [0.1, -0.1, 0.2, 0.2, -0.2],
            "sigma_hat": [1.0, 1.0, 1.0, 1.0, 1.0],
        }
    )
    part_a = work.iloc[:2]
    part_b = work.iloc[2:]
    direct = compute_judge_summaries(work)
    merged = merge_mergeable_judge_summaries(
        [
            compute_mergeable_judge_summary(part_a),
            compute_mergeable_judge_summary(part_b),
        ]
    )
    pd.testing.assert_frame_equal(
        direct.sort_values("judge_name").reset_index(drop=True),
        merged.sort_values("judge_name").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-9,
    )
