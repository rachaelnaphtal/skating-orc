"""
Element GOE deviation ranking (control-score / sigma model).

Shared by ``scripts/load_judge_rankings.py`` and the Streamlit
"Element Deviation Ranking Analysis" page.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from analytics import JudgeAnalytics
from models import (
    Competition,
    Element,
    ElementScorePerJudge,
    Segment,
    SkaterSegment,
)
from officials_competition_types import COMPETITION_SCOPE_ALL
from rule_errors_policy import MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS

# Current IJS element GOE scale; exclude earlier competitions from this model.
MIN_ELEMENT_MARKING_EVENT_DATE = MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS

FLOOR_SIGMA = 0.05
MIN_BIN_COUNT = 30


def sigma_model(c, alpha, beta, gamma):
    """σ̂(c) = α + β e^{γ c} (continuous bucket model; optional)."""
    return alpha + beta * np.exp(gamma * c)


def build_element_mark_filters(
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    discipline_type_ids: Optional[Iterable[int]] = None,
):
    conditions = []

    if start_season_year is not None:
        conditions.append(Competition.year >= str(start_season_year))
    if end_season_year is not None:
        conditions.append(Competition.year <= str(end_season_year))

    if discipline_type_ids:
        conditions.append(Segment.discipline_type_id.in_(list(discipline_type_ids)))

    if not conditions:
        return None
    return and_(*conditions)


def load_element_marking_data(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[Iterable[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
) -> pd.DataFrame:
    """
    element_id, judge_id, judge_score, discipline_type_id, element_type_id, competition_year.
    """
    where_clause = build_element_mark_filters(
        start_season_year, end_season_year, discipline_type_ids
    )

    stmt = (
        select(
            ElementScorePerJudge.element_id,
            ElementScorePerJudge.judge_id,
            ElementScorePerJudge.judge_score,
            Segment.discipline_type_id,
            Element.element_type_id,
            Competition.year.label("competition_year"),
        )
        .join(Element, ElementScorePerJudge.element_id == Element.id)
        .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
    )
    if where_clause is not None:
        stmt = stmt.where(where_clause)
    stmt = analytics._filter_select_competition_scope(stmt, competition_scope)
    effective_start = MIN_ELEMENT_MARKING_EVENT_DATE
    if event_start_date is not None:
        effective_start = max(event_start_date, MIN_ELEMENT_MARKING_EVENT_DATE)
    stmt = analytics._apply_competition_event_date_range(
        stmt, effective_start, event_end_date
    )
    df = pd.read_sql(stmt, session.bind)
    if df.empty:
        return df
    df["judge_score"] = df["judge_score"].astype(float)
    return df


def compute_control_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Adds control_score (median GOE per element) and error."""
    med = df.groupby("element_id")["judge_score"].median().rename("control_score")
    out = df.merge(med, on="element_id", how="left")
    out["error"] = out["judge_score"] - out["control_score"]
    return out


def load_judge_identity_map(analytics: JudgeAnalytics) -> pd.DataFrame:
    rows: list[dict] = []
    for group in analytics.get_judge_analysis_identity_groups():
        label = group["label"]
        jids = sorted({int(j) for j in group["judge_ids"]})
        judge_ids_csv = ";".join(str(j) for j in jids)
        for jid in jids:
            rows.append(
                {
                    "judge_id": jid,
                    "judge_name": label,
                    "judge_ids": judge_ids_csv,
                }
            )
    return pd.DataFrame(rows)


def attach_judge_identities(df: pd.DataFrame, analytics: JudgeAnalytics) -> pd.DataFrame:
    id_map = load_judge_identity_map(analytics)
    if id_map.empty:
        raise RuntimeError("No judges found for identity mapping.")
    out = df.merge(id_map, on="judge_id", how="left")
    missing = out["judge_name"].isna().sum()
    if missing:
        out["judge_name"] = out["judge_name"].fillna(
            out["judge_id"].astype(str).radd("judge:")
        )
        out["judge_ids"] = out["judge_ids"].fillna(out["judge_id"].astype(str))
    return out


def fit_sigma_discrete(
    df: pd.DataFrame,
    *,
    min_bin_count: int = MIN_BIN_COUNT,
) -> dict:
    """
    σ̂ for each (discipline_type_id, element_type_id, rounded control GOE).

    Returns {(disc_id, elem_type_id, control_int): sigma_hat}.
    """
    params: dict = {}
    grouped = df.dropna(subset=["discipline_type_id", "element_type_id"]).copy()
    grouped["control_int"] = grouped["control_score"].round().astype(int)

    for (disc_id, elem_type_id, k), g in grouped.groupby(
        ["discipline_type_id", "element_type_id", "control_int"]
    ):
        if len(g) < min_bin_count:
            continue
        sd = g["error"].std(ddof=1)
        if sd <= 0 or np.isnan(sd):
            continue
        params[(int(disc_id), int(elem_type_id), int(k))] = float(sd)

    return params


def sigma_hat_row_discrete(
    control_score: float,
    disc_id,
    elem_type_id,
    params: dict,
    *,
    floor_sigma: float = FLOOR_SIGMA,
    fallback_sigma: float = 0.3,
) -> float:
    if pd.isna(disc_id) or pd.isna(elem_type_id):
        return max(floor_sigma, fallback_sigma)

    k = int(round(control_score))
    key = (int(disc_id), int(elem_type_id), k)

    if key not in params:
        for dk in (-1, 1, -2, 2):
            alt_key = (int(disc_id), int(elem_type_id), k + dk)
            if alt_key in params:
                return max(params[alt_key], floor_sigma)
        return max(floor_sigma, fallback_sigma)

    return max(params[key], floor_sigma)


def _sigma_lookup_meta(
    control_score: float,
    disc_id,
    elem_type_id,
    params: dict,
    *,
    fallback_sigma: float = 0.3,
) -> tuple[float, str]:
    """Return (sigma, source) with source in fitted | neighbor | fallback."""
    if pd.isna(disc_id) or pd.isna(elem_type_id):
        return fallback_sigma, "fallback"
    k = int(round(control_score))
    key = (int(disc_id), int(elem_type_id), k)
    if key in params:
        return float(params[key]), "fitted"
    for dk in (-1, 1, -2, 2):
        alt_key = (int(disc_id), int(elem_type_id), k + dk)
        if alt_key in params:
            return float(params[alt_key]), "neighbor"
    return fallback_sigma, "fallback"


def _params_to_lookup_df(params: dict) -> pd.DataFrame:
    if not params:
        return pd.DataFrame(
            columns=[
                "discipline_type_id",
                "element_type_id",
                "control_int",
                "sigma_lookup",
            ]
        )
    rows = [
        {
            "discipline_type_id": int(d),
            "element_type_id": int(e),
            "control_int": int(k),
            "sigma_lookup": float(s),
        }
        for (d, e, k), s in params.items()
    ]
    return pd.DataFrame(rows)


def annotate_normalized_marks(
    df: pd.DataFrame,
    params: dict,
    *,
    floor_sigma: float = FLOOR_SIGMA,
) -> pd.DataFrame:
    """Add sigma_hat, m_pj, sigma_source, and rounded control_int (vectorized)."""
    fallback = max(floor_sigma, float(df["error"].std(ddof=1) or 0.3))
    lookup = _params_to_lookup_df(params)

    work = df.copy()
    work["control_int"] = work["control_score"].round().astype(int)
    work["sigma_hat"] = np.nan
    work["sigma_source"] = pd.Series(pd.NA, index=work.index, dtype="string")

    if not lookup.empty:
        fitted = work.merge(
            lookup,
            left_on=["discipline_type_id", "element_type_id", "control_int"],
            right_on=["discipline_type_id", "element_type_id", "control_int"],
            how="left",
        )
        hit = fitted["sigma_lookup"].notna()
        work.loc[hit, "sigma_hat"] = fitted.loc[hit, "sigma_lookup"].values
        work.loc[hit, "sigma_source"] = "fitted"

        miss_mask = work["sigma_hat"].isna()
        for dk in (-1, 1, -2, 2):
            if not miss_mask.any():
                break
            neighbor = lookup.copy()
            neighbor["control_int"] = neighbor["control_int"] + dk
            merged = work.loc[miss_mask].merge(
                neighbor,
                left_on=["discipline_type_id", "element_type_id", "control_int"],
                right_on=["discipline_type_id", "element_type_id", "control_int"],
                how="left",
            )
            nbr_hit = merged["sigma_lookup"].notna()
            if not nbr_hit.any():
                continue
            hit_idx = merged.index[nbr_hit]
            work.loc[hit_idx, "sigma_hat"] = merged.loc[nbr_hit, "sigma_lookup"].values
            work.loc[hit_idx, "sigma_source"] = "neighbor"
            miss_mask = work["sigma_hat"].isna()

    fb = work["sigma_hat"].isna()
    work.loc[fb, "sigma_hat"] = fallback
    work.loc[fb, "sigma_source"] = "fallback"
    work["sigma_hat"] = work["sigma_hat"].clip(lower=floor_sigma)
    work["m_pj"] = work["error"] / work["sigma_hat"]
    return work


def _partial_marking_score(series: pd.Series) -> float:
    return float(np.sqrt((series**2).mean()))


def compute_judge_summaries(work: pd.DataFrame) -> pd.DataFrame:
    """Per-judge aggregates explaining the overall marking score."""
    g = work.groupby("judge_name", sort=False)
    out = g.agg(
        n_marks=("m_pj", "size"),
        marking_score=("m_pj", _partial_marking_score),
        mean_error=("error", "mean"),
        mean_abs_error=("error", lambda s: float(s.abs().mean())),
        mean_sigma_hat=("sigma_hat", "mean"),
        mean_abs_m=("m_pj", lambda s: float(s.abs().mean())),
    ).reset_index()
    out = out.sort_values("marking_score").reset_index(drop=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    return out


def compute_judge_discipline_breakdown(
    work: pd.DataFrame,
    discipline_id_to_name: dict[int, str],
    element_type_id_to_name: dict[int, str],
) -> pd.DataFrame:
    """Per judge × discipline and per judge × discipline × element type."""
    work = work.copy()
    work["discipline"] = work["discipline_type_id"].map(discipline_id_to_name)
    work["element_type"] = work["element_type_id"].map(element_type_id_to_name)

    disc_rows = []
    for (judge_name, discipline), g in work.groupby(["judge_name", "discipline"], dropna=False):
        if pd.isna(discipline):
            continue
        disc_rows.append(
            {
                "Judge": judge_name,
                "Discipline": discipline,
                "Element marks": len(g),
                "Partial marking score": _partial_marking_score(g["m_pj"]),
                "Mean GOE bias": float(g["error"].mean()),
                "Mean |error|": float(g["error"].abs().mean()),
                "Mean σ̂": float(g["sigma_hat"].mean()),
            }
        )

    elem_rows = []
    for (judge_name, discipline, element_type), g in work.groupby(
        ["judge_name", "discipline", "element_type"], dropna=False
    ):
        if pd.isna(discipline) or pd.isna(element_type):
            continue
        elem_rows.append(
            {
                "Judge": judge_name,
                "Discipline": discipline,
                "Element type": element_type,
                "Element marks": len(g),
                "Partial marking score": _partial_marking_score(g["m_pj"]),
                "Mean GOE bias": float(g["error"].mean()),
                "Mean |error|": float(g["error"].abs().mean()),
            }
        )

    return pd.DataFrame(disc_rows), pd.DataFrame(elem_rows).sort_values(
        ["Judge", "Partial marking score"], ascending=[True, False]
    )


def build_sigma_bins_dataframe(
    df: pd.DataFrame,
    params: dict,
    discipline_id_to_name: dict[int, str],
    element_type_id_to_name: dict[int, str],
    *,
    min_bin_count: int = MIN_BIN_COUNT,
) -> pd.DataFrame:
    """σ̂ (error stdev) by discipline, element type, and rounded control GOE."""
    grouped = df.dropna(subset=["discipline_type_id", "element_type_id"]).copy()
    grouped["control_int"] = grouped["control_score"].round().astype(int)
    rows = []
    for (disc_id, elem_type_id, control_int), g in grouped.groupby(
        ["discipline_type_id", "element_type_id", "control_int"]
    ):
        n = len(g)
        sd = float(g["error"].std(ddof=1)) if n >= 2 else np.nan
        key = (int(disc_id), int(elem_type_id), int(control_int))
        in_model = key in params and n >= min_bin_count
        rows.append(
            {
                "Discipline": discipline_id_to_name.get(int(disc_id), str(disc_id)),
                "Element type": element_type_id_to_name.get(
                    int(elem_type_id), str(elem_type_id)
                ),
                "Control GOE": int(control_int),
                "Marks in bin": n,
                "Error stdev (all marks)": round(sd, 4) if pd.notna(sd) else None,
                "σ̂ used in model": round(params[key], 4) if in_model else None,
                "In model": in_model,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["Discipline", "Element type", "Control GOE"], ascending=[True, True, True]
    ).reset_index(drop=True)


def build_ranking_display_table(judge_summary: pd.DataFrame) -> pd.DataFrame:
    """Rankings for UI (no judge ids)."""
    return judge_summary.rename(
        columns={
            "judge_name": "Judge",
            "n_marks": "Element marks",
            "marking_score": "Marking score",
            "mean_error": "Mean GOE bias",
            "mean_abs_error": "Mean |error|",
            "mean_sigma_hat": "Mean σ̂",
            "mean_abs_m": "Mean |m|",
        }
    )[
        [
            "rank",
            "Judge",
            "Marking score",
            "Element marks",
            "Mean GOE bias",
            "Mean |error|",
            "Mean σ̂",
            "Mean |m|",
        ]
    ]


def compute_marking_scores(
    df: pd.DataFrame,
    params: dict,
    *,
    floor_sigma: float = FLOOR_SIGMA,
) -> pd.DataFrame:
    """Per identity: M = sqrt(mean(m²)). Prefer ``compute_judge_summaries`` for UI."""
    work = annotate_normalized_marks(df, params, floor_sigma=floor_sigma)
    return compute_judge_summaries(work)


def apply_min_marks_filter(marking: pd.DataFrame, min_marks: int) -> pd.DataFrame:
    if min_marks <= 0:
        return marking
    kept = marking.loc[marking["n_marks"] >= min_marks].copy()
    kept["rank"] = range(1, len(kept) + 1)
    return kept


def marking_score_summary(judge_summary: pd.DataFrame) -> pd.DataFrame:
    if judge_summary.empty:
        return pd.DataFrame()
    desc = judge_summary["marking_score"].describe()
    return pd.DataFrame(
        {
            "Statistic": desc.index.astype(str),
            "Value": [round(float(v), 6) if pd.notna(v) else None for v in desc.values],
        }
    )


def compute_element_deviation_rankings(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    min_marks: int = 0,
    floor_sigma: float = FLOOR_SIGMA,
    min_bin_count: int = MIN_BIN_COUNT,
) -> dict:
    """
    Run the full pipeline. Returns dict with keys:
    marking, summary, sigma_bins, judge_discipline_detail, judge_element_detail,
    params, n_raw_marks, n_sigma_buckets, error (str|None).
    """
    session = analytics.session
    disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
    elem_map = {int(i): n for i, n in analytics.get_element_types()}

    df = load_element_marking_data(
        session,
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
    )
    if df.empty:
        return {
            "marking": pd.DataFrame(),
            "summary": pd.DataFrame(),
            "sigma_bins": pd.DataFrame(),
            "judge_discipline_detail": pd.DataFrame(),
            "judge_element_detail": pd.DataFrame(),
            "params": {},
            "n_raw_marks": 0,
            "n_sigma_buckets": 0,
            "error": "No element score rows found for the selected filters.",
        }

    n_raw = len(df)
    df = compute_control_scores(df)
    df = attach_judge_identities(df, analytics)
    params = fit_sigma_discrete(df, min_bin_count=min_bin_count)
    work = annotate_normalized_marks(df, params, floor_sigma=floor_sigma)
    judge_summary = compute_judge_summaries(work)
    judge_summary = apply_min_marks_filter(judge_summary, min_marks)
    marking = build_ranking_display_table(judge_summary)
    summary = marking_score_summary(judge_summary)
    sigma_bins = build_sigma_bins_dataframe(
        df,
        params,
        disc_map,
        elem_map,
        min_bin_count=min_bin_count,
    )
    judge_discipline_detail, judge_element_detail = compute_judge_discipline_breakdown(
        work.loc[work["judge_name"].isin(judge_summary["judge_name"])],
        disc_map,
        elem_map,
    )

    return {
        "marking": marking,
        "summary": summary,
        "sigma_bins": sigma_bins,
        "judge_discipline_detail": judge_discipline_detail,
        "judge_element_detail": judge_element_detail,
        "params": params,
        "n_raw_marks": n_raw,
        "n_sigma_buckets": len(params),
        "error": None,
    }
