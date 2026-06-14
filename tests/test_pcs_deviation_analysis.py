"""Tests for PCS deviation analysis control bins and sigma fitting."""

import numpy as np
import pandas as pd

from pcs_deviation_analysis import (
    annotate_normalized_marks_pcs,
    compute_errors,
    control_bin_from_median,
    control_bin_label,
    fit_sigma_discrete_pcs,
)


def test_control_bin_from_median_ranges():
    assert control_bin_from_median(0.25) == 1
    assert control_bin_from_median(1.0) == 1
    assert control_bin_from_median(1.25) == 2
    assert control_bin_from_median(2.0) == 2
    assert control_bin_from_median(2.25) == 3
    assert control_bin_from_median(7.75) == 8


def test_control_bin_label():
    assert control_bin_label(1) == "0.25–1.00"
    assert control_bin_label(2) == "1.25–2.00"
    assert control_bin_label(3) == "2.25–3.00"


def test_fit_sigma_discrete_pcs_min_count():
    rows = []
    for i in range(40):
        rows.append(
            {
                "discipline_type_id": 1,
                "component": "SS",
                "control_score": 7.5,
                "judge_score": 7.5 + (0.1 if i % 2 == 0 else -0.1),
            }
        )
    df = compute_errors(pd.DataFrame(rows))
    params = fit_sigma_discrete_pcs(df, min_bin_count=30)
    key = (1, "SS", control_bin_from_median(7.5))
    assert key in params
    assert params[key] > 0


def test_annotate_normalized_marks_pcs_uses_fitted_sigma():
    df = compute_errors(
        pd.DataFrame(
            [
                {
                    "discipline_type_id": 1,
                    "component": "SS",
                    "control_score": 7.0,
                    "judge_score": 7.2,
                    "error": 0.2,
                }
            ]
        )
    )
    params = {(1, "SS", control_bin_from_median(7.0)): 0.25}
    out = annotate_normalized_marks_pcs(df, params, floor_sigma=0.05)
    assert np.isclose(out["m_pj"].iloc[0], 0.2 / 0.25)
    assert out["sigma_source"].iloc[0] == "fitted"
