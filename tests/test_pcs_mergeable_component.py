import numpy as np
import pandas as pd

from pcs_quality_analysis import (
    build_profiles_from_component_detail,
    compute_judge_profiles,
    compute_mergeable_component_detail_from_marks,
    merge_mergeable_component_details,
)


def _synthetic_marks(n_segments: int = 4, judges_per_seg: int = 5) -> pd.DataFrame:
    rows = []
    seg_id = 0
    for _ in range(n_segments):
        for skater in range(judges_per_seg):
            for judge_id in (1, 2):
                base = float(skater + judge_id * 0.1)
                rows.append(
                    {
                        "judge_id": judge_id,
                        "skater_segment_id": 1000 + seg_id * 10 + skater,
                        "pcs_type_id": 1,
                        "segment_id": seg_id,
                        "discipline_type_id": 1,
                        "judge_score": base + (0.05 if judge_id == 1 else -0.05),
                        "panel_median": base,
                        "pcs_type_name": "Skating Skills",
                        "discipline_name": "Singles",
                        "component": "SS",
                    }
                )
        seg_id += 1
    return pd.DataFrame(rows)


def test_mergeable_pcs_matches_direct_profiles():
    marks = _synthetic_marks()
    id_map = {1: "Judge A", 2: "Judge B"}
    direct_profiles, direct_detail, _, _ = compute_judge_profiles(marks, id_map)

    mid_seg = marks["segment_id"].median()
    part_a = compute_mergeable_component_detail_from_marks(
        marks.loc[marks["segment_id"] <= mid_seg], id_map
    )
    part_b = compute_mergeable_component_detail_from_marks(
        marks.loc[marks["segment_id"] > mid_seg], id_map
    )
    merged_detail = merge_mergeable_component_details([part_a, part_b])
    merged_profiles, _, _, _ = build_profiles_from_component_detail(merged_detail)

    direct_sorted = direct_profiles.sort_values("Judge").reset_index(drop=True)
    merged_sorted = merged_profiles.sort_values("Judge").reset_index(drop=True)
    for col in ("ranking_score", "bias_score", "diff_score", "PCS marks"):
        np.testing.assert_allclose(
            direct_sorted[col].to_numpy(dtype=float),
            merged_sorted[col].to_numpy(dtype=float),
            rtol=1e-9,
            atol=1e-9,
            err_msg=col,
        )
