"""
PCS judge quality vs panel medians (post-2022-07-01 competitions).

Overall quality: per discipline, equal weight across whichever PCS components
appear in that discipline; judge overall is mark-weighted across disciplines.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd
from scipy.stats import rankdata, spearmanr
from analytics import JudgeAnalytics
from models import (
    Competition,
    DisciplineType,
    Judge,
    PcsScorePerJudge,
    PcsType,
    Segment,
    Skater,
    SkaterSegment,
)
from officials_competition_types import COMPETITION_SCOPE_ALL

MIN_PCS_ANALYSIS_EVENT_DATE = date(2022, 7, 1)
MIN_SKATERS_PER_SEGMENT_RANKING = 3
RANKING_CORRELATION_FLOOR = 0.7
RANKING_CORRELATION_SPAN = 0.3

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


def differentiation_score(judge_scores: np.ndarray, panel_scores: np.ndarray) -> float:
    if len(judge_scores) < 2:
        return 0.0
    jv = float(np.var(judge_scores, ddof=1))
    pv = float(np.var(panel_scores, ddof=1))
    if jv <= 0 or pv <= 0:
        return 0.0
    ratio = jv / pv
    if ratio <= 0:
        return 0.0
    return _clamp01(1.0 - abs(float(np.log(ratio))))


def consistency_score(judge_scores: np.ndarray, panel_scores: np.ndarray) -> float:
    if len(judge_scores) < 2:
        return 0.0
    biases = judge_scores - panel_scores
    sd = float(np.std(biases, ddof=1))
    return _clamp01(1.0 - (sd / 0.75))


def component_quality_score(
    ranking: float, bias: float, diff: float, consistency: float
) -> float:
    return 0.4 * ranking + 0.3 * bias + 0.2 * diff + 0.1 * consistency


def compute_component_metrics(marks_df: pd.DataFrame) -> dict[str, float]:
    """Metrics for one judge × discipline × PCS component mark set."""
    judge_scores = marks_df["judge_score"].to_numpy(dtype=float)
    panel_scores = marks_df["panel_median"].to_numpy(dtype=float)
    rk, mean_rho, n_segments, _n_seg_skipped = ranking_correlation_score_from_segment_events(
        marks_df
    )
    bs = bias_score(judge_scores, panel_scores)
    ds = differentiation_score(judge_scores, panel_scores)
    cs = consistency_score(judge_scores, panel_scores)
    biases = judge_scores - panel_scores if len(judge_scores) else np.array([])
    bias_std = float(np.std(biases, ddof=1)) if len(biases) >= 2 else 0.0
    return {
        "spearman_rho": mean_rho if mean_rho is not None else float("nan"),
        "ranking_score": rk,
        "bias_score": bs,
        "diff_score": ds,
        "consistency_score": cs,
        "component_quality": component_quality_score(rk, bs, ds, cs),
        "mean_bias": float(np.mean(biases)) if len(biases) else 0.0,
        "bias_std": bias_std,
        "n_marks": int(len(marks_df)),
        "n_segments_ranked": int(n_segments),
    }


def load_pcs_quality_marks(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
) -> pd.DataFrame:
    """PCS marks with panel median per skater×component (post-2022-07-01 competitions)."""
    session = analytics.session
    core_disc = analytics._qualifying_core_disciplines_active(competition_scope)
    seg_discipline_ids = analytics._merged_segment_discipline_ids(
        core_disc, discipline_type_ids
    )

    effective_start = MIN_PCS_ANALYSIS_EVENT_DATE
    if event_start_date is not None:
        effective_start = max(event_start_date, MIN_PCS_ANALYSIS_EVENT_DATE)

    q = (
        session.query(
            PcsScorePerJudge.judge_id,
            Judge.name.label("judge_name"),
            PcsScorePerJudge.skater_segment_id,
            PcsScorePerJudge.pcs_type_id,
            PcsType.name.label("pcs_type_name"),
            PcsScorePerJudge.judge_score,
            Segment.id.label("segment_id"),
            Segment.discipline_type_id,
            DisciplineType.name.label("discipline_name"),
            Competition.year,
            Competition.name.label("competition_name"),
            Segment.name.label("segment_name"),
            Skater.name.label("skater_name"),
        )
        .join(Judge, PcsScorePerJudge.judge_id == Judge.id)
        .join(PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
        .join(Skater, SkaterSegment.skater_id == Skater.id)
        .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)
    )
    if seg_discipline_ids is not None:
        q = q.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
    if start_season_year:
        q = q.filter(Competition.year >= str(start_season_year))
    if end_season_year:
        q = q.filter(Competition.year <= str(end_season_year))
    q = analytics._filter_orm_competition_scope(q, competition_scope)
    q = analytics._apply_competition_event_date_range(
        q, effective_start, event_end_date
    )

    rows = q.all()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {
                "judge_id": int(r.judge_id),
                "judge_name": r.judge_name,
                "skater_segment_id": int(r.skater_segment_id),
                "pcs_type_id": int(r.pcs_type_id),
                "pcs_type_name": r.pcs_type_name,
                "judge_score": float(r.judge_score),
                "segment_id": int(r.segment_id),
                "discipline_type_id": int(r.discipline_type_id)
                if r.discipline_type_id is not None
                else None,
                "discipline_name": r.discipline_name or "Unknown",
                "season_year": str(r.year) if r.year else None,
                "competition_name": r.competition_name,
                "segment_name": r.segment_name,
                "skater_name": r.skater_name,
            }
            for r in rows
        ]
    )
    df["component"] = df["pcs_type_name"].map(pcs_component_label)
    df = df.dropna(subset=["component", "discipline_type_id"]).copy()
    if df.empty:
        return df

    df["panel_median"] = df.groupby(
        ["skater_segment_id", "pcs_type_id"], sort=False
    )["judge_score"].transform("median")
    return df


def components_by_discipline_in_period(marks: pd.DataFrame) -> dict[int, list[str]]:
    """PCS components present per discipline within the filtered mark pool."""
    if marks.empty:
        return {}
    out: dict[int, list[str]] = {}
    for disc_id, grp in marks.groupby("discipline_type_id", sort=False):
        out[int(disc_id)] = sorted(grp["component"].dropna().unique())
    return out


def _discipline_component_summary(
    disc_df: pd.DataFrame,
    period_components: list[str],
) -> tuple[list[str], list[str], dict[str, float], dict[str, float]]:
    """
    Equal-weight component qualities for one judge × one discipline.

    ``period_components`` is the full component set for that discipline in the
    filtered time window; only components the judge actually marked are scored.
    """
    judge_components = set(disc_df["component"].dropna().unique())
    scored = [c for c in period_components if c in judge_components]
    comp_scores: dict[str, float] = {}
    comp_bias: dict[str, float] = {}
    for comp in scored:
        sub = disc_df.loc[disc_df["component"] == comp]
        m = compute_component_metrics(sub)
        comp_scores[comp] = m["component_quality"]
        comp_bias[comp] = m["mean_bias"]
    return period_components, scored, comp_scores, comp_bias


def compute_judge_profiles(
    marks: pd.DataFrame, judge_id_to_identity: dict[int, str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build judge profiles, per-judge×discipline×component detail, and discipline summaries.

    Returns ``(profiles_df, component_detail_df, discipline_summary_df)``.
    """
    if marks.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    work = marks.copy()
    work["identity"] = work["judge_id"].map(judge_id_to_identity)
    work = work.dropna(subset=["identity"])

    period_by_discipline = components_by_discipline_in_period(work)

    component_rows: list[dict[str, Any]] = []
    discipline_rows: list[dict[str, Any]] = []
    profile_rows: list[dict[str, Any]] = []

    for identity, id_df in work.groupby("identity", sort=False):
        all_comp_qualities: list[float] = []
        ranking_scores: list[float] = []
        bias_scores: list[float] = []
        diff_scores: list[float] = []
        consistency_scores: list[float] = []
        disc_overalls: list[float] = []
        disc_weights: list[float] = []

        for disc_id, disc_df in id_df.groupby("discipline_type_id", sort=False):
            disc_name = str(disc_df["discipline_name"].iloc[0])
            period_components = period_by_discipline.get(int(disc_id), [])
            if not period_components:
                continue

            period_components, scored, comp_scores, _ = (
                _discipline_component_summary(disc_df, period_components)
            )
            if not comp_scores:
                continue

            disc_overall = float(np.mean(list(comp_scores.values())))
            disc_overalls.append(disc_overall)
            disc_weights.append(float(len(disc_df)))

            discipline_rows.append(
                {
                    "identity": identity,
                    "discipline": disc_name,
                    "discipline_quality": round(disc_overall, 4),
                    "n_components_scored": len(scored),
                    "PCS marks": int(len(disc_df)),
                }
            )

            for comp in scored:
                sub = disc_df.loc[disc_df["component"] == comp]
                m = compute_component_metrics(sub)
                all_comp_qualities.append(m["component_quality"])
                ranking_scores.append(m["ranking_score"])
                bias_scores.append(m["bias_score"])
                diff_scores.append(m["diff_score"])
                consistency_scores.append(m["consistency_score"])
                component_rows.append(
                    {
                        "identity": identity,
                        "discipline": disc_name,
                        "component": comp,
                        **m,
                    }
                )

        if not disc_overalls:
            continue

        overall = float(np.average(disc_overalls, weights=disc_weights))
        comp_var = (
            float(np.std(all_comp_qualities, ddof=0))
            if len(all_comp_qualities) > 1
            else 0.0
        )

        profile_rows.append(
            {
                "Judge": identity,
                "Overall quality": round(overall, 4),
                "Ranking": round(float(np.mean(ranking_scores)), 4),
                "Bias": round(float(np.mean(bias_scores)), 4),
                "Differentiation": round(float(np.mean(diff_scores)), 4),
                "Consistency": round(float(np.mean(consistency_scores)), 4),
                "Disciplines": len(disc_overalls),
                "Component σ": round(comp_var, 4),
                "PCS marks": int(len(id_df)),
            }
        )

    profiles = pd.DataFrame(profile_rows)
    if not profiles.empty:
        profiles = profiles.sort_values("Overall quality", ascending=False).reset_index(
            drop=True
        )
        profiles.insert(0, "Rank", range(1, len(profiles) + 1))

    detail = pd.DataFrame(component_rows)
    discipline_summary = pd.DataFrame(discipline_rows)
    return profiles, detail, discipline_summary


def apply_min_pcs_marks_to_result(
    result: dict[str, Any], min_pcs_marks: int
) -> dict[str, Any]:
    """Drop judges below ``min_pcs_marks`` PCS lines in scope; re-rank survivors."""
    min_pcs_marks = int(min_pcs_marks or 0)
    if min_pcs_marks <= 0 or not result:
        return result

    profiles = result.get("profiles")
    if profiles is None or profiles.empty:
        return result

    kept = profiles.loc[profiles["PCS marks"] >= min_pcs_marks].copy()
    kept = kept.sort_values("Overall quality", ascending=False).reset_index(drop=True)
    if "Rank" in kept.columns:
        kept = kept.drop(columns=["Rank"])
    kept.insert(0, "Rank", range(1, len(kept) + 1))

    kept_judges = set(kept["Judge"].tolist())
    detail = result.get("component_detail", pd.DataFrame())
    if not detail.empty:
        detail = detail.loc[detail["identity"].isin(kept_judges)].copy()
    discipline_summary = result.get("discipline_summary", pd.DataFrame())
    if not discipline_summary.empty:
        discipline_summary = discipline_summary.loc[
            discipline_summary["identity"].isin(kept_judges)
        ].copy()

    out = dict(result)
    out["profiles"] = kept
    out["component_detail"] = detail
    out["discipline_summary"] = discipline_summary
    out["n_judges"] = len(kept)
    out["n_judges_before_min_filter"] = len(profiles)
    out["min_pcs_marks"] = min_pcs_marks
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
    marks = load_pcs_quality_marks(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
    )
    id_map = analytics.get_judge_id_to_identity_label()
    profiles, detail, discipline_summary = compute_judge_profiles(marks, id_map)
    return {
        "marks": marks,
        "profiles": profiles,
        "component_detail": detail,
        "discipline_summary": discipline_summary,
        "n_raw_marks": len(marks),
        "n_judges": len(profiles),
        "error": None if not profiles.empty or marks.empty else "No PCS marks in scope.",
    }
