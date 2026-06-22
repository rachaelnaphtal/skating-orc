"""Tests for PCS quadratic σ̂ model."""

import numpy as np
import pandas as pd

from pcs_deviation_analysis import (
    annotate_normalized_marks_pcs_quadratic,
    collect_sigma_bin_stats_pcs,
    collect_sigma_plot_stats_pcs,
    compare_pcs_sigma_model_rankings,
    compute_errors,
    control_bin_center,
    direct_sigma_quadratic_equation_str,
    fit_sigma_quadratic_pcs,
    sigma_bin_from_control_score,
    quadratic_sigma_equation_str,
    quadratic_sigma_eval,
    sample_error_stdev,
    sample_error_variance,
    SIGMA_SAMPLE_DDOF,
)


def _synthetic_marks(
    *,
    n_per_bin: int = 40,
    noise_by_control: dict[float, float],
) -> pd.DataFrame:
    rows = []
    for control, noise_sd in noise_by_control.items():
        rng = np.random.default_rng(int(control * 100))
        for _ in range(n_per_bin):
            err = float(rng.normal(0, noise_sd))
            rows.append(
                {
                    "discipline_type_id": 3,
                    "component": "SK",
                    "control_score": control,
                    "judge_score": control + err,
                    "judge_name": f"judge_{len(rows) % 5}",
                }
            )
    return compute_errors(pd.DataFrame(rows))


def test_control_bin_center():
    assert control_bin_center(1) == 0.625
    assert control_bin_center(8) == 7.625


def test_sample_error_variance_uses_ddof_one():
    errors = pd.Series([0.0, 2.0, -2.0])
    assert np.isclose(sample_error_variance(errors), 4.0)
    assert np.isclose(sample_error_stdev(errors), 2.0)
    pop_var = float(errors.var(ddof=0))
    assert pop_var < sample_error_variance(errors)


def test_quadratic_sigma_eval():
    # Constant variance 0.09 → σ = 0.3
    assert np.isclose(quadratic_sigma_eval(5.0, 0.0, 0.0, 0.09), 0.3)
    # Var(2) = -0.04 + 0.2 + 0.2 = 0.36 → σ = 0.6
    assert np.isclose(quadratic_sigma_eval(2.0, -0.01, 0.1, 0.2), 0.6)


def test_min_bin_count_for_sigma_model():
    from pcs_deviation_analysis import (
        MIN_BIN_COUNT,
        PCS_SIGMA_MODEL_DISCRETE,
        PCS_SIGMA_MODEL_QUADRATIC,
        min_bin_count_for_sigma_model,
    )

    assert min_bin_count_for_sigma_model(PCS_SIGMA_MODEL_DISCRETE) == MIN_BIN_COUNT
    assert min_bin_count_for_sigma_model(PCS_SIGMA_MODEL_QUADRATIC) == MIN_BIN_COUNT


def test_sigma_bin_from_control_score():
    assert sigma_bin_from_control_score(7.12) == 7.0
    assert sigma_bin_from_control_score(7.13) == 7.25
    assert sigma_bin_from_control_score(0.25, bin_width=0.125) == 0.25
    assert sigma_bin_from_control_score(0.19, bin_width=0.125) == 0.25
    assert sigma_bin_from_control_score(0.18, bin_width=0.125) == 0.125


def test_pcs_sigma_bin_width_for_model():
    from pcs_deviation_analysis import (
        PCS_SIGMA_BIN_WIDTH_DISCRETE,
        PCS_SIGMA_BIN_WIDTH_QUADRATIC,
        PCS_SIGMA_MODEL_DISCRETE,
        PCS_SIGMA_MODEL_QUADRATIC,
        pcs_sigma_bin_width_for_model,
        sigma_bin_label,
    )

    assert pcs_sigma_bin_width_for_model(PCS_SIGMA_MODEL_DISCRETE) == PCS_SIGMA_BIN_WIDTH_DISCRETE
    assert pcs_sigma_bin_width_for_model(PCS_SIGMA_MODEL_QUADRATIC) == PCS_SIGMA_BIN_WIDTH_QUADRATIC
    assert sigma_bin_label(0.25, bin_width=0.125) == "0.19–0.31"


def test_fit_sigma_quadratic_pcs_recovers_shape():
    noise = {2.0: 0.45, 4.0: 0.55, 6.0: 0.40, 8.0: 0.25, 9.5: 0.20}
    df = _synthetic_marks(n_per_bin=50, noise_by_control=noise)
    stats = collect_sigma_bin_stats_pcs(df, min_bin_count=30)
    assert len(stats) >= 4
    assert "variance_empirical" in stats.columns
    assert np.allclose(
        stats["sigma_empirical"].values,
        np.sqrt(stats["variance_empirical"].values),
        rtol=1e-6,
    )

    params = fit_sigma_quadratic_pcs(df, min_bin_count=30, min_bins_for_fit=4)
    key = (3, "SK")
    assert key in params
    p = params[key]
    assert p["fit_target"] == "sample_variance"
    assert p["sample_ddof"] == float(SIGMA_SAMPLE_DDOF)
    assert quadratic_sigma_eval(9.0, p["a"], p["b"], p["c"]) < quadratic_sigma_eval(
        4.0, p["a"], p["b"], p["c"]
    )
    assert "Var̂(c)" in quadratic_sigma_equation_str(p)
    assert "direct_sigma_a" in p
    assert "σ̂(c)" in direct_sigma_quadratic_equation_str(p)


def test_collect_sigma_plot_stats_use_production_bins():
    noise = {2.0: 0.45, 4.0: 0.55, 6.0: 0.40, 8.0: 0.25, 9.5: 0.20}
    df = _synthetic_marks(n_per_bin=50, noise_by_control=noise)
    stats = collect_sigma_bin_stats_pcs(df, min_bin_count=10)
    fine = collect_sigma_plot_stats_pcs(df, min_bin_count=10)
    assert len(fine) == len(stats)
    assert len(fine) >= 4
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
    params = {
        (1, "SS"): {
            "a": 0.0,
            "b": 0.0,
            "c": 0.0625,  # Var = 0.0625 → σ = 0.25
            "rmse_sigma": 0.0,
            "n_bins": 4,
            "n_marks": 100,
        }
    }
    out = annotate_normalized_marks_pcs_quadratic(df, params, floor_sigma=0.05)
    assert np.isclose(out["m_pj"].iloc[0], 0.2 / 0.25)
    assert out["sigma_source"].iloc[0] == "quadratic"


def test_compare_pcs_sigma_model_rankings():
    noise = {3.0: 0.5, 5.0: 0.5, 7.0: 0.3, 8.5: 0.2}
    df = _synthetic_marks(n_per_bin=60, noise_by_control=noise)
    comparison, metrics = compare_pcs_sigma_model_rankings(
        df, min_bin_count=30, floor_sigma=0.05
    )
    assert not comparison.empty
    assert metrics["n_judges"] >= 1
    assert "rank_spearman" in metrics
    assert "Marking score (discrete)" in comparison.columns
    assert "Marking score (quadratic)" in comparison.columns
    assert "Score delta (discrete − quadratic)" in comparison.columns
