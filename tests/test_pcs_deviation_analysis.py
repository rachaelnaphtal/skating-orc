"""Tests for PCS deviation analysis control bins and sigma fitting."""

import numpy as np
import pandas as pd
import pytest

from pcs_deviation_analysis import (
    annotate_normalized_marks_pcs,
    compute_errors,
    control_bin_from_median,
    control_bin_label,
    fit_sigma_discrete_pcs,
    load_pcs_deviation_marks,
    sigma_bin_from_control_score,
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
    key = (1, "SS", sigma_bin_from_control_score(7.5))
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
    params = {(1, "SS", sigma_bin_from_control_score(7.0)): 0.25}
    out = annotate_normalized_marks_pcs(df, params, floor_sigma=0.05)
    assert np.isclose(out["m_pj"].iloc[0], 0.2 / 0.25)
    assert out["sigma_source"].iloc[0] == "fitted"


@pytest.mark.integration
def test_load_pcs_deviation_marks_sql_uses_single_scope_cte():
    from unittest.mock import patch

    from sqlalchemy.exc import OperationalError

    from analytics import JudgeAnalytics
    from database import get_db_session
    from element_deviation_ranking import ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR

    try:
        session = get_db_session()
    except OperationalError:
        pytest.skip("database unavailable")

    analytics = JudgeAnalytics(session)
    captured: list[str] = []

    def _capture(stmt, bind):
        captured.append(str(stmt.compile(bind)))
        return pd.DataFrame()

    try:
        with patch("pcs_deviation_analysis.pd.read_sql", _capture):
            load_pcs_deviation_marks(
                analytics,
                start_season_year="2526",
                end_season_year="2526",
                discipline_type_ids=[5],
                segment_level_preset=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
            )
    except OperationalError:
        pytest.skip("database unavailable")
    finally:
        session.close()

    assert len(captured) == 1
    sql = captured[0].lower()
    assert "pcs_deviation_marks_scope" in sql
    assert sql.count("join competition") == 1


@pytest.mark.integration
def test_load_pcs_deviation_marks_control_score_matches_panel_median():
    from sqlalchemy.exc import OperationalError

    from analytics import JudgeAnalytics
    from database import get_db_session
    from element_deviation_ranking import ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR

    try:
        session = get_db_session()
    except OperationalError:
        pytest.skip("database unavailable")

    try:
        analytics = JudgeAnalytics(session)
        df = load_pcs_deviation_marks(
            analytics,
            start_season_year="2526",
            end_season_year="2526",
            discipline_type_ids=[5],
            segment_level_preset=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
        )
    except OperationalError:
        session.close()
        pytest.skip("database unavailable")
    else:
        session.close()

    if df.empty:
        pytest.skip("no PCS marks for integration check")

    grouped = df.groupby(["skater_segment_id", "pcs_type_id"], sort=False)
    for (_ss, _pcs), grp in list(grouped)[:20]:
        expected = float(grp["judge_score"].median())
        actual = float(grp["control_score"].iloc[0])
        assert np.isclose(actual, expected), (actual, expected, len(grp))
