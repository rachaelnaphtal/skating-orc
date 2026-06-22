"""PCS deviation run_params include σ̂ model choice."""

from pcs_deviation_analysis import (
    PCS_SIGMA_MODEL_DISCRETE,
    PCS_SIGMA_MODEL_QUADRATIC,
    run_params_benchmark_compute_key,
    run_params_ranking_compute_key,
    run_params_same_sigma_and_ranking_scope,
    sigma_model_from_run_params,
    unpack_pcs_deviation_run_params,
)


def _base_run_params(sigma_model: str = PCS_SIGMA_MODEL_DISCRETE) -> tuple:
    return (
        "2425",
        "2526",
        (1, 3),
        "international",
        None,
        None,
        0,
        0.05,
        30,
        "2223",
        "2526",
        "international",
        "junior_senior",
        "junior_senior",
        sigma_model,
    )


def test_legacy_run_params_default_discrete():
    legacy = _base_run_params()[:14]
    assert sigma_model_from_run_params(legacy) == PCS_SIGMA_MODEL_DISCRETE
    assert unpack_pcs_deviation_run_params(legacy)[14] == PCS_SIGMA_MODEL_DISCRETE


def test_quadratic_run_params_change_cache_keys():
    discrete = _base_run_params(PCS_SIGMA_MODEL_DISCRETE)
    quadratic = _base_run_params(PCS_SIGMA_MODEL_QUADRATIC)
    assert run_params_benchmark_compute_key(discrete) != run_params_benchmark_compute_key(
        quadratic
    )
    assert run_params_ranking_compute_key(discrete) != run_params_ranking_compute_key(
        quadratic
    )
    assert not run_params_same_sigma_and_ranking_scope(discrete, quadratic)


def test_build_precompute_run_params_sigma_model():
    from pcs_deviation_cache import build_precompute_pcs_deviation_run_params

    discrete = build_precompute_pcs_deviation_run_params("international")
    quadratic = build_precompute_pcs_deviation_run_params(
        "international", sigma_model=PCS_SIGMA_MODEL_QUADRATIC
    )
    assert discrete[14] == PCS_SIGMA_MODEL_DISCRETE
    assert quadratic[14] == PCS_SIGMA_MODEL_QUADRATIC
