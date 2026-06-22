"""load_cached_pcs_deviation_rankings must pass σ̂ model through."""

from unittest.mock import MagicMock, patch

from pcs_deviation_analysis import PCS_SIGMA_MODEL_QUADRATIC


def test_load_cached_passes_sigma_model_to_pipeline():
    from pcs_deviation_cache import load_cached_pcs_deviation_rankings

    session = MagicMock()
    analytics = MagicMock()
    run_params = (
        "2425",
        "2526",
        (3,),
        "international",
        None,
        None,
        0,
        0.05,
        30,
        None,
        None,
        "international",
        "junior_senior",
        "junior_senior",
        PCS_SIGMA_MODEL_QUADRATIC,
    )
    with patch("pcs_deviation_cache.run_pcs_deviation_ranking_pipeline") as pipeline:
        pipeline.return_value = {"error": None, "sigma_model": PCS_SIGMA_MODEL_QUADRATIC}
        out = load_cached_pcs_deviation_rankings(session, analytics, run_params)
    assert out is not None
    pipeline.assert_called_once()
    assert pipeline.call_args.kwargs["sigma_model"] == PCS_SIGMA_MODEL_QUADRATIC
