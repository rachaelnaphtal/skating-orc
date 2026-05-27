"""
PCS judge quality vs panel medians (post-2022-07-01 competitions).

Three independent alignment measures (ranking, bias, differentiation)
are computed per judge × discipline × PCS component, then averaged within each
discipline (equal weight across components) and across disciplines (mark-weighted).
Each measure has its own ranking table — they are not combined into one score.
"""

from __future__ import annotations

import gc
import os
from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd
from scipy.stats import rankdata, spearmanr
from sqlalchemy import and_, func, select
from analytics import JudgeAnalytics
from models import (
    Competition,
    DisciplineType,
    PcsScorePerJudge,
    PcsType,
    Segment,
    SkaterSegment,
)
from officials_competition_types import COMPETITION_SCOPE_ALL

MIN_PCS_ANALYSIS_EVENT_DATE = date(2022, 7, 1)
MIN_SKATERS_PER_SEGMENT_RANKING = 3
RANKING_CORRELATION_FLOOR = 0.7
RANKING_CORRELATION_SPAN = 0.3
BIAS_NEUTRAL_THRESHOLD = 0.05
SPREAD_RATIO_LENIENT = 1.1
SPREAD_RATIO_HARSH = 0.9
# Heroku / small dynos: refuse full in-memory loads above this (override via env).
PCS_QUALITY_MAX_MARKS = int(os.environ.get("PCS_QUALITY_MAX_MARKS", "400000"))

PCS_COMPONENT_DESCRIPTIONS: dict[str, str] = {
    "SS": "Skating Skills — blade work, flow, speed, and ice coverage.",
    "TR": "Transitions — links between elements and movement phases.",
    "PE": "Performance — engagement, projection, and execution of the program.",
    "CO": "Composition — choreography, ideas, and use of the rink.",
    "PR": "Presentation — overall look and feel of the program (often ice dance).",
    "TI": "Timing — rhythm and precision to music (often pattern / rhythm segments).",
}


def pcs_component_description(component_code: str | None) -> str:
    """Short ISU-oriented label for a PCS component code."""
    if not component_code:
        return ""
    key = str(component_code).strip()
    return PCS_COMPONENT_DESCRIPTIONS.get(
        key, f"{key} — PCS component from competition data"
    )


_PCS_NAME_TO_CODE: dict[str, str] = {
    "skating skills": "SS",
    "transitions": "TR",
    "transition": "TR",
    "performance": "PE",
    "composition": "CO",
    "composition/skating skills": "SS",
    "presentation": "PR",
    "timing": "TI",
}


def pcs_component_label(name: str | None) -> str | None:
    """Short code when known; otherwise the PCS type name from the database."""
    if not name or not str(name).strip():
        return None
    key = name.strip().lower()
    if key in _PCS_NAME_TO_CODE:
        return _PCS_NAME_TO_CODE[key]
    for token, code in (
        ("skating skill", "SS"),
        ("transition", "TR"),
        ("performance", "PE"),
        ("composition", "CO"),
        ("presentation", "PR"),
        ("timing", "TI"),
    ):
        if token in key:
            return code
    return str(name).strip()


def _clamp01(x: float) -> float:
    if pd.isna(x) or not np.isfinite(x):
        return 0.0
    return float(max(0.0, min(1.0, x)))


def spearman_rho_vs_panel(
    judge_scores: np.ndarray, panel_scores: np.ndarray
) -> float | None:
    """Spearman ρ between judge ranks and panel-median ranks within one segment."""
    if len(judge_scores) < 3:
        return None
    if np.std(judge_scores) == 0 or np.std(panel_scores) == 0:
        return None
    rho, _ = spearmanr(rankdata(judge_scores), rankdata(panel_scores))
    if pd.isna(rho):
        return None
    return float(rho)


def _ranking_subscore_from_rho(rho: float) -> float:
    return _clamp01((rho - RANKING_CORRELATION_FLOOR) / RANKING_CORRELATION_SPAN)


def ranking_correlation_score_from_segment_events(
    marks_df: pd.DataFrame,
) -> tuple[float, float | None, int, int]:
    """
    Ranking quality from segment-level rank order.

    Each **segment** (e.g. short program, free skate) is one event: compare the
    judge’s skater ranks to panel medians among skaters in that segment only.
    Segments with fewer than ``MIN_SKATERS_PER_SEGMENT_RANKING`` skaters are
    skipped. Returns equal-weight mean of per-segment ranking sub-scores.
    """
    if marks_df.empty or "segment_id" not in marks_df.columns:
        return 0.0, None, 0, 0

    segment_subscores: list[float] = []
    rhos: list[float] = []
    n_skipped = 0
    for _seg_id, seg_df in marks_df.groupby("segment_id", sort=False):
        seg_lines = seg_df.drop_duplicates(subset=["skater_segment_id"])
        if len(seg_lines) < MIN_SKATERS_PER_SEGMENT_RANKING:
            n_skipped += 1
            continue
        js = seg_lines["judge_score"].to_numpy(dtype=float)
        ps = seg_lines["panel_median"].to_numpy(dtype=float)
        rho = spearman_rho_vs_panel(js, ps)
        if rho is None:
            n_skipped += 1
            continue
        rhos.append(rho)
        segment_subscores.append(_ranking_subscore_from_rho(rho))

    if not segment_subscores:
        return 0.0, None, 0, n_skipped
    return (
        float(np.mean(segment_subscores)),
        float(np.mean(rhos)),
        len(segment_subscores),
        n_skipped,
    )


def bias_score(judge_scores: np.ndarray, panel_scores: np.ndarray) -> float:
    if len(judge_scores) == 0:
        return 0.0
    avg_bias = float(np.mean(judge_scores - panel_scores))
    return _clamp01(1.0 - (abs(avg_bias) / 0.5))


def variance_ratio(judge_scores: np.ndarray, panel_scores: np.ndarray) -> float:
    """Judge mark variance / panel median variance (same slice as differentiation)."""
    if len(judge_scores) < 2:
        return float("nan")
    jv = float(np.var(judge_scores, ddof=1))
    pv = float(np.var(panel_scores, ddof=1))
    if jv <= 0 or pv <= 0:
        return float("nan")
    return float(jv / pv)


def bias_tendency_label(mean_bias: float) -> str:
    if not np.isfinite(mean_bias):
        return "—"
    if mean_bias > BIAS_NEUTRAL_THRESHOLD:
        return "Lenient (above panel)"
    if mean_bias < -BIAS_NEUTRAL_THRESHOLD:
        return "Harsh (below panel)"
    return "Neutral (near panel)"


def spread_tendency_label(var_ratio: float) -> str:
    if not np.isfinite(var_ratio):
        return "—"
    if var_ratio > SPREAD_RATIO_LENIENT:
        return "Wider than panel"
    if var_ratio < SPREAD_RATIO_HARSH:
        return "Narrower than panel"
    return "Similar to panel"


def differentiation_score(judge_scores: np.ndarray, panel_scores: np.ndarray) -> float:
    if len(judge_scores) < 2:
        return 0.0
    ratio = variance_ratio(judge_scores, panel_scores)
    if not np.isfinite(ratio) or ratio <= 0:
        return 0.0
    return _clamp01(1.0 - abs(float(np.log(ratio))))


def compute_component_metrics(marks_df: pd.DataFrame) -> dict[str, float]:
    """Metrics for one judge × discipline × PCS component mark set."""
    judge_scores = marks_df["judge_score"].to_numpy(dtype=float)
    panel_scores = marks_df["panel_median"].to_numpy(dtype=float)
    rk, mean_rho, n_segments, _n_seg_skipped = ranking_correlation_score_from_segment_events(
        marks_df
    )
    bs = bias_score(judge_scores, panel_scores)
    ds = differentiation_score(judge_scores, panel_scores)
    biases = judge_scores - panel_scores if len(judge_scores) else np.array([])
    vr = variance_ratio(judge_scores, panel_scores)
    return {
        "spearman_rho": mean_rho if mean_rho is not None else float("nan"),
        "ranking_score": rk,
        "bias_score": bs,
        "diff_score": ds,
        "mean_bias": float(np.mean(biases)) if len(biases) else 0.0,
        "bias_tendency": bias_tendency_label(
            float(np.mean(biases)) if len(biases) else float("nan")
        ),
        "variance_ratio": vr,
        "spread_tendency": spread_tendency_label(vr),
        "n_marks": int(len(marks_df)),
        "n_segments_ranked": int(n_segments),
    }


def _pcs_quality_effective_start(event_start_date: date | None) -> date:
    if event_start_date is None:
        return MIN_PCS_ANALYSIS_EVENT_DATE
    return max(event_start_date, MIN_PCS_ANALYSIS_EVENT_DATE)


def _pcs_quality_segment_discipline_ids(
    analytics: JudgeAnalytics,
    *,
    discipline_type_ids: Optional[list[int]],
    competition_scope: str,
) -> list[int] | None:
    core_disc = analytics._qualifying_core_disciplines_active(competition_scope)
    return analytics._merged_segment_discipline_ids(core_disc, discipline_type_ids)


def _apply_pcs_quality_scope_filters(
    query,
    analytics: JudgeAnalytics,
    *,
    seg_discipline_ids: list[int] | None,
    start_season_year: Optional[str],
    end_season_year: Optional[str],
    effective_start: date,
    event_end_date: date | None,
    competition_scope: str,
):
    if seg_discipline_ids is not None:
        query = query.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
    if start_season_year:
        query = query.filter(Competition.year >= str(start_season_year))
    if end_season_year:
        query = query.filter(Competition.year <= str(end_season_year))
    query = analytics._filter_orm_competition_scope(query, competition_scope)
    return analytics._apply_competition_event_date_range(
        query, effective_start, event_end_date
    )


def count_pcs_quality_marks(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
) -> int:
    """PCS mark rows in scope (same filters as ``load_pcs_quality_marks``)."""
    seg_discipline_ids = _pcs_quality_segment_discipline_ids(
        analytics,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
    )
    effective_start = _pcs_quality_effective_start(event_start_date)
    count_q = (
        select(func.count(PcsScorePerJudge.id))
        .select_from(PcsScorePerJudge)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
    )
    count_q = _apply_pcs_quality_scope_filters(
        count_q,
        analytics,
        seg_discipline_ids=seg_discipline_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        effective_start=effective_start,
        event_end_date=event_end_date,
        competition_scope=competition_scope,
    )
    return int(analytics.session.execute(count_q).scalar() or 0)


def load_pcs_quality_marks(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    max_marks: int | None = PCS_QUALITY_MAX_MARKS,
) -> pd.DataFrame:
    """PCS marks with panel median per skater×component (post-2022-07-01 competitions)."""
    seg_discipline_ids = _pcs_quality_segment_discipline_ids(
        analytics,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
    )
    effective_start = _pcs_quality_effective_start(event_start_date)

    if max_marks is not None and max_marks > 0:
        n_rows = count_pcs_quality_marks(
            analytics,
            start_season_year=start_season_year,
            end_season_year=end_season_year,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
            discipline_type_ids=discipline_type_ids,
            competition_scope=competition_scope,
        )
        if n_rows > max_marks:
            raise ValueError(
                f"PCS mark count ({n_rows:,}) exceeds limit ({max_marks:,}). "
                "Narrow season years, enable event dates, or reduce disciplines."
            )

    panel_sq = (
        select(
            PcsScorePerJudge.skater_segment_id.label("skater_segment_id"),
            PcsScorePerJudge.pcs_type_id.label("pcs_type_id"),
            func.percentile_cont(0.5)
            .within_group(PcsScorePerJudge.judge_score)
            .label("panel_median"),
        )
        .select_from(PcsScorePerJudge)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
    )
    panel_sq = _apply_pcs_quality_scope_filters(
        panel_sq,
        analytics,
        seg_discipline_ids=seg_discipline_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        effective_start=effective_start,
        event_end_date=event_end_date,
        competition_scope=competition_scope,
    )
    panel_sq = panel_sq.group_by(
        PcsScorePerJudge.skater_segment_id,
        PcsScorePerJudge.pcs_type_id,
    ).subquery()

    marks_q = (
        select(
            PcsScorePerJudge.judge_id,
            PcsScorePerJudge.skater_segment_id,
            PcsScorePerJudge.pcs_type_id,
            PcsType.name.label("pcs_type_name"),
            PcsScorePerJudge.judge_score,
            Segment.id.label("segment_id"),
            Segment.discipline_type_id,
            DisciplineType.name.label("discipline_name"),
            panel_sq.c.panel_median,
        )
        .select_from(PcsScorePerJudge)
        .join(PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
        .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)
        .join(
            panel_sq,
            and_(
                PcsScorePerJudge.skater_segment_id == panel_sq.c.skater_segment_id,
                PcsScorePerJudge.pcs_type_id == panel_sq.c.pcs_type_id,
            ),
        )
    )
    marks_q = _apply_pcs_quality_scope_filters(
        marks_q,
        analytics,
        seg_discipline_ids=seg_discipline_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        effective_start=effective_start,
        event_end_date=event_end_date,
        competition_scope=competition_scope,
    )

    df = pd.read_sql(marks_q, analytics.session.bind)
    if df.empty:
        return df

    df["judge_id"] = pd.to_numeric(df["judge_id"], downcast="integer")
    df["skater_segment_id"] = pd.to_numeric(
        df["skater_segment_id"], downcast="integer"
    )
    df["pcs_type_id"] = pd.to_numeric(df["pcs_type_id"], downcast="integer")
    df["segment_id"] = pd.to_numeric(df["segment_id"], downcast="integer")
    df["discipline_type_id"] = pd.to_numeric(
        df["discipline_type_id"], downcast="integer"
    )
    df["judge_score"] = df["judge_score"].astype(np.float32)
    df["panel_median"] = df["panel_median"].astype(np.float32)
    df["component"] = df["pcs_type_name"].map(pcs_component_label)
    df["discipline_name"] = df["discipline_name"].fillna("Unknown")
    df = df.dropna(subset=["component", "discipline_type_id"]).copy()
    return df


def components_by_discipline_in_period(marks: pd.DataFrame) -> dict[int, list[str]]:
    """PCS components present per discipline within the filtered mark pool."""
    if marks.empty:
        return {}
    out: dict[int, list[str]] = {}
    for disc_id, grp in marks.groupby("discipline_type_id", sort=False):
        out[int(disc_id)] = sorted(grp["component"].dropna().unique())
    return out


def _weighted_discipline_metric(
    discipline_avgs: list[tuple[float, float]],
) -> float:
    """Mark-weighted mean across disciplines: [(score, n_marks), ...]."""
    if not discipline_avgs:
        return 0.0
    weights = [w for _, w in discipline_avgs]
    total_w = sum(weights)
    if total_w <= 0:
        return float(np.mean([s for s, _ in discipline_avgs]))
    return float(
        sum(s * w for s, w in discipline_avgs) / total_w
    )


def build_metric_ranking_table(
    profile_rows: list[dict[str, Any]], metric_key: str, score_label: str
) -> pd.DataFrame:
    """Sort judges by one metric (higher = better alignment)."""
    if not profile_rows:
        return pd.DataFrame()
    df = pd.DataFrame(profile_rows)
    sort_col = {
        "ranking_score": "Ranking score",
        "bias_score": "Bias score",
        "diff_score": "Differentiation score",
    }.get(metric_key, score_label)
    if metric_key not in df.columns:
        return pd.DataFrame()
    df = df.sort_values(metric_key, ascending=False).reset_index(drop=True)
    out = pd.DataFrame(
        {
            "Rank": range(1, len(df) + 1),
            "Judge": df["Judge"],
            sort_col: df[metric_key].round(4),
            "Disciplines": df.get("Disciplines", 0),
            "PCS marks": df.get("PCS marks", 0),
        }
    )
    if metric_key == "ranking_score" and "Mean Spearman ρ" in df.columns:
        out.insert(3, "Mean Spearman ρ", df["Mean Spearman ρ"].round(3))
    if metric_key == "bias_score":
        if "mean_bias" in df.columns:
            out.insert(3, "Mean bias", df["mean_bias"].round(3))
        if "bias_tendency" in df.columns:
            out.insert(4, "Tendency", df["bias_tendency"])
    if metric_key == "diff_score":
        if "variance_ratio" in df.columns:
            out.insert(3, "Var ratio", df["variance_ratio"].round(2))
        if "spread_tendency" in df.columns:
            out.insert(4, "Spread vs panel", df["spread_tendency"])
    return out


PCS_METRIC_DEFINITIONS: tuple[tuple[str, str, str], ...] = (
    ("ranking_score", "Ranking score", "ranking"),
    ("bias_score", "Bias score", "bias"),
    ("diff_score", "Differentiation score", "differentiation"),
)


def compute_judge_profiles(
    marks: pd.DataFrame, judge_id_to_identity: dict[int, str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    """
    Build judge profiles, per-judge×discipline×component detail, discipline summaries,
    and one ranking table per metric.

    Returns ``(profiles_df, component_detail_df, discipline_summary_df, metric_rankings)``.
    """
    empty_rankings = {
        slug: pd.DataFrame() for _, _, slug in PCS_METRIC_DEFINITIONS
    }
    if marks.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), empty_rankings

    work = marks
    if "identity" not in work.columns:
        work = marks.copy()
        work["identity"] = work["judge_id"].map(judge_id_to_identity)
    work = work.dropna(subset=["identity"])

    period_by_discipline = components_by_discipline_in_period(work)

    component_rows: list[dict[str, Any]] = []
    for (identity, disc_id, comp), grp in work.groupby(
        ["identity", "discipline_type_id", "component"], sort=False
    ):
        period_components = period_by_discipline.get(int(disc_id), [])
        if comp not in period_components:
            continue
        m = compute_component_metrics(grp)
        component_rows.append(
            {
                "identity": identity,
                "discipline": str(grp["discipline_name"].iloc[0]),
                "component": comp,
                **m,
            }
        )

    if not component_rows:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), empty_rankings

    detail = pd.DataFrame(component_rows)
    discipline_rows: list[dict[str, Any]] = []
    profile_rows: list[dict[str, Any]] = []

    for identity, id_df in detail.groupby("identity", sort=False):
        disc_ranking: list[tuple[float, float]] = []
        disc_bias: list[tuple[float, float]] = []
        disc_mean_bias: list[tuple[float, float]] = []
        disc_diff: list[tuple[float, float]] = []
        disc_var_ratio: list[tuple[float, float]] = []
        rhos: list[float] = []
        pcs_marks = 0

        for (_disc_name, disc_grp) in id_df.groupby("discipline", sort=False):
            n_marks = int(disc_grp["n_marks"].sum())
            w = disc_grp["n_marks"].to_numpy(dtype=float)
            pcs_marks += n_marks
            disc_ranking.append((float(disc_grp["ranking_score"].mean()), float(n_marks)))
            disc_bias.append((float(disc_grp["bias_score"].mean()), float(n_marks)))
            mb = float(np.average(disc_grp["mean_bias"], weights=w))
            disc_mean_bias.append((mb, float(n_marks)))
            disc_diff.append((float(disc_grp["diff_score"].mean()), float(n_marks)))
            vr = float(np.average(disc_grp["variance_ratio"], weights=w))
            disc_var_ratio.append((vr, float(n_marks)))
            for rho in disc_grp["spearman_rho"]:
                if rho is not None and np.isfinite(rho):
                    rhos.append(float(rho))
            discipline_rows.append(
                {
                    "identity": identity,
                    "discipline": str(_disc_name),
                    "ranking_score": round(
                        float(disc_grp["ranking_score"].mean()), 4
                    ),
                    "bias_score": round(float(disc_grp["bias_score"].mean()), 4),
                    "mean_bias": round(mb, 4),
                    "bias_tendency": bias_tendency_label(mb),
                    "diff_score": round(float(disc_grp["diff_score"].mean()), 4),
                    "variance_ratio": round(vr, 4),
                    "spread_tendency": spread_tendency_label(vr),
                    "n_components_scored": int(len(disc_grp)),
                    "PCS marks": n_marks,
                }
            )

        pooled_mean_bias = _weighted_discipline_metric(disc_mean_bias)
        pooled_var_ratio = _weighted_discipline_metric(disc_var_ratio)
        profile_rows.append(
            {
                "Judge": identity,
                "ranking_score": round(_weighted_discipline_metric(disc_ranking), 4),
                "bias_score": round(_weighted_discipline_metric(disc_bias), 4),
                "mean_bias": round(pooled_mean_bias, 4),
                "bias_tendency": bias_tendency_label(pooled_mean_bias),
                "diff_score": round(_weighted_discipline_metric(disc_diff), 4),
                "variance_ratio": round(pooled_var_ratio, 4),
                "spread_tendency": spread_tendency_label(pooled_var_ratio),
                "Mean Spearman ρ": float(np.mean(rhos)) if rhos else float("nan"),
                "Disciplines": len(disc_ranking),
                "PCS marks": pcs_marks,
            }
        )

    profiles = pd.DataFrame(profile_rows)
    discipline_summary = pd.DataFrame(discipline_rows)
    metric_rankings = {
        slug: build_metric_ranking_table(profile_rows, key, label)
        for key, label, slug in PCS_METRIC_DEFINITIONS
    }
    return profiles, detail, discipline_summary, metric_rankings


def apply_min_pcs_marks_to_result(
    result: dict[str, Any], min_pcs_marks: int
) -> dict[str, Any]:
    """Drop judges below ``min_pcs_marks`` PCS lines in scope; re-rank survivors."""
    if not result:
        return result
    min_pcs_marks = int(min_pcs_marks or 0)
    if result.get("_min_pcs_applied") == min_pcs_marks:
        return result

    if min_pcs_marks <= 0:
        out = dict(result)
        out.pop("marks", None)
        profiles = out.get("profiles")
        if profiles is not None and not profiles.empty and not out.get("metric_rankings"):
            rows = profiles.to_dict("records")
            out["metric_rankings"] = {
                slug: build_metric_ranking_table(rows, key, label)
                for key, label, slug in PCS_METRIC_DEFINITIONS
            }
        out["_min_pcs_applied"] = 0
        out.pop("marks", None)
        return out

    profiles = result.get("profiles")
    if profiles is None or profiles.empty:
        return result

    kept = profiles.loc[profiles["PCS marks"] >= min_pcs_marks].copy()
    kept_judges = set(kept["Judge"].tolist())
    kept_rows = kept.to_dict("records")
    metric_rankings = {
        slug: build_metric_ranking_table(kept_rows, key, label)
        for key, label, slug in PCS_METRIC_DEFINITIONS
    }

    detail = result.get("component_detail", pd.DataFrame())
    if not detail.empty:
        detail = detail.loc[detail["identity"].isin(kept_judges)].copy()
    discipline_summary = result.get("discipline_summary", pd.DataFrame())
    if not discipline_summary.empty:
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
    out["n_judges_before_min_filter"] = len(profiles)
    out["min_pcs_marks"] = min_pcs_marks
    out["_min_pcs_applied"] = min_pcs_marks
    return out


def run_pcs_quality_analysis(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
) -> dict[str, Any]:
    """Load marks and return profiles, component detail, and mark counts."""
    try:
        marks = load_pcs_quality_marks(
            analytics,
            start_season_year=start_season_year,
            end_season_year=end_season_year,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
            discipline_type_ids=discipline_type_ids,
            competition_scope=competition_scope,
        )
    except ValueError as exc:
        return {
            "profiles": pd.DataFrame(),
            "metric_rankings": {
                slug: pd.DataFrame() for _, _, slug in PCS_METRIC_DEFINITIONS
            },
            "component_detail": pd.DataFrame(),
            "discipline_summary": pd.DataFrame(),
            "n_raw_marks": 0,
            "n_judges": 0,
            "error": str(exc),
        }
    id_map = analytics.get_judge_id_to_identity_label()
    profiles, detail, discipline_summary, metric_rankings = compute_judge_profiles(
        marks, id_map
    )
    n_raw = len(marks)
    del marks
    gc.collect()
    return {
        "profiles": profiles,
        "metric_rankings": metric_rankings,
        "component_detail": detail,
        "discipline_summary": discipline_summary,
        "n_raw_marks": n_raw,
        "n_judges": len(profiles),
        "error": None if not profiles.empty or n_raw == 0 else "No PCS marks in scope.",
    }
