"""PCS deviation deep-link query parameters."""

from unittest.mock import patch

from app_query_params import apply_choice_param
from pcs_deviation_analysis import PCS_SIGMA_MODEL_DISCRETE, PCS_SIGMA_MODEL_QUADRATIC


def test_sigma_model_query_param_sets_session_state():
    session: dict = {}

    with patch("streamlit.session_state", session, create=True):
        assert apply_choice_param(
            "sigma_model",
            "pcs_deviation_sigma_model",
            (PCS_SIGMA_MODEL_DISCRETE, PCS_SIGMA_MODEL_QUADRATIC),
        ) is False

        with patch("app_query_params.qp_get", return_value="quadratic"):
            assert apply_choice_param(
                "sigma_model",
                "pcs_deviation_sigma_model",
                (PCS_SIGMA_MODEL_DISCRETE, PCS_SIGMA_MODEL_QUADRATIC),
            )

    assert session["pcs_deviation_sigma_model"] == PCS_SIGMA_MODEL_QUADRATIC


def test_sigma_model_query_param_rejects_unknown():
    session: dict = {}

    with patch("streamlit.session_state", session, create=True):
        with patch("app_query_params.qp_get", return_value="linear"):
            assert apply_choice_param(
                "sigma_model",
                "pcs_deviation_sigma_model",
                (PCS_SIGMA_MODEL_DISCRETE, PCS_SIGMA_MODEL_QUADRATIC),
            ) is False

    assert "pcs_deviation_sigma_model" not in session
