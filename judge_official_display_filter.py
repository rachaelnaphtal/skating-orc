"""
Display-only filters for ranking UIs: restrict tables to US directory-linked identities.

Metrics are computed on the full judge pool; these helpers only narrow what is shown.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from element_deviation_ranking import (
    _filter_judge_detail_by_names,
    build_ranking_display_table,
    marking_score_summary,
)
from pcs_quality_analysis import (
    PCS_METRIC_DEFINITIONS,
    build_metric_ranking_table,
)


def _marking_display_to_summary_df(marking: pd.DataFrame) -> pd.DataFrame:
    return marking.rename(
        columns={
            "Judge": "judge_name",
            "Marking score": "marking_score",
            "Element marks": "n_marks",
            "Mean GOE bias": "mean_error",
            "Mean |error|": "mean_abs_error",
            "Mean σ̂": "mean_sigma_hat",
            "Mean |m|": "mean_abs_m",
        }
    )


def _apply_us_officials_filter_to_ranking_details(
    result: dict[str, Any],
    kept_names: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    jd_all = result.get("judge_discipline_detail_all")
    je_all = result.get("judge_element_detail_all")
    if isinstance(jd_all, pd.DataFrame) and not jd_all.empty:
        return _filter_judge_detail_by_names(jd_all, je_all, kept_names)
    jd, je = result.get("judge_discipline_detail"), result.get("judge_element_detail")
    return _filter_judge_detail_by_names(
        jd if isinstance(jd, pd.DataFrame) else pd.DataFrame(),
        je if isinstance(je, pd.DataFrame) else pd.DataFrame(),
        kept_names,
    )


def apply_us_officials_display_filter_to_ranking_result(
    result: dict[str, Any],
    linked_labels: frozenset[str] | set[str],
) -> dict[str, Any]:
    """Keep only judge identities linked in ``judge_official_link``; re-rank for display."""
    if not linked_labels:
        return result

    marking = result.get("marking")
    if isinstance(marking, pd.DataFrame) and not marking.empty and "Judge" in marking.columns:
        # Subset the current rankings table so display-only filters (e.g. minimum
        # marks) already applied to ``marking`` are preserved.
        kept_marking = marking.loc[marking["Judge"].isin(linked_labels)].copy()
        kept_marking = kept_marking.sort_values("Marking score").reset_index(drop=True)
        kept_marking["rank"] = range(1, len(kept_marking) + 1)
        kept_names = set(kept_marking["Judge"])
        out = dict(result)
        out["marking"] = kept_marking
        out["summary"] = marking_score_summary(_marking_display_to_summary_df(kept_marking))
        out["n_judges_ranked_display"] = len(kept_marking)
        out["n_judges_before_us_officials_filter"] = len(marking)
        out["judge_discipline_detail"], out["judge_element_detail"] = (
            _apply_us_officials_filter_to_ranking_details(result, kept_names)
        )
        out["_us_officials_display_filter"] = True
        return out

    judge_all = result.get("judge_summary_all")
    if not isinstance(judge_all, pd.DataFrame) or judge_all.empty:
        return result

    kept = judge_all.loc[judge_all["judge_name"].isin(linked_labels)].copy()
    kept = kept.sort_values("marking_score").reset_index(drop=True)
    kept["rank"] = range(1, len(kept) + 1)
    kept_names = set(kept["judge_name"])
    out = dict(result)
    out["marking"] = build_ranking_display_table(kept)
    out["summary"] = marking_score_summary(kept)
    out["n_judges_ranked_display"] = len(kept)
    out["n_judges_before_us_officials_filter"] = len(judge_all)
    out["judge_discipline_detail"], out["judge_element_detail"] = (
        _apply_us_officials_filter_to_ranking_details(result, kept_names)
    )
    out["_us_officials_display_filter"] = True
    return out


def apply_us_officials_display_filter_to_pcs_result(
    result: dict[str, Any],
    linked_labels: frozenset[str] | set[str],
) -> dict[str, Any]:
    """Keep only US directory-linked judges in PCS quality tables; re-rank."""
    if not linked_labels:
        return result
    profiles = result.get("profiles")
    if profiles is None or not isinstance(profiles, pd.DataFrame) or profiles.empty:
        return result

    n_before = len(profiles)
    kept = profiles.loc[profiles["Judge"].isin(linked_labels)].copy()
    kept_judges = set(kept["Judge"].tolist())
    kept_rows = kept.to_dict("records")
    metric_rankings = {
        slug: build_metric_ranking_table(rows, key, label)
        for key, label, slug in PCS_METRIC_DEFINITIONS
        for rows in [kept_rows]
    }

    detail = result.get("component_detail", pd.DataFrame())
    if isinstance(detail, pd.DataFrame) and not detail.empty:
        detail = detail.loc[detail["identity"].isin(kept_judges)].copy()
    discipline_summary = result.get("discipline_summary", pd.DataFrame())
    if isinstance(discipline_summary, pd.DataFrame) and not discipline_summary.empty:
        discipline_summary = discipline_summary.loc[
            discipline_summary["identity"].isin(kept_judges)
        ].copy()

    out = dict(result)
    out.pop("marks", None)
    out["profiles"] = kept
    out["metric_rankings"] = metric_rankings
    out["component_detail"] = detail
    out["discipline_summary"] = discipline_summary
    out["n_judges"] = len(kept)
    out["n_judges_before_us_officials_filter"] = n_before
    out["_us_officials_display_filter"] = True
    return out


def filter_cross_judge_dataframe(
    df: pd.DataFrame,
    linked_labels: frozenset[str] | set[str],
    *,
    judge_col: str = "judge_name",
) -> pd.DataFrame:
    """Subset cross-judge heatmap / cell tables to linked identity labels."""
    if df.empty or not linked_labels or judge_col not in df.columns:
        return df
    return df.loc[df[judge_col].isin(linked_labels)].copy()
