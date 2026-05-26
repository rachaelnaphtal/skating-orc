"""
Element GOE deviation ranking (control-score / sigma model).

Shared by ``scripts/load_judge_rankings.py`` and the Streamlit
"Element Deviation Ranking Analysis" page.
"""

from __future__ import annotations

import gc
import hashlib
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Optional

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
# Earliest season code offered in UI filters (1819 = 2018–19; matches GOE-scale cutoff).
MIN_ELEMENT_RANKING_SEASON_YEAR = "1819"

# Discipline types that do not use IJS element GOE marks (PCS-only or non-element formats).
ELEMENT_RANKING_EXCLUDED_DISCIPLINE_NAMES = frozenset(
    {
        "showcase",
        "theatre on ice",
        "athlete development",
        'uncategorized'
    }
)

FLOOR_SIGMA = 0.05
MIN_BIN_COUNT = 30
# Columns stored per season×discipline shard cache row.
SHARD_MARK_COLUMNS = (
    "element_id",
    "judge_id",
    "judge_name",
    "judge_score",
    "discipline_type_id",
    "element_type_id",
)
def memory_efficient_mode() -> bool:
    """Heroku / low-RAM: slimmer pipeline (on-demand judge detail, sidecar pickles)."""
    if os.environ.get("DYNO"):
        return True
    return os.environ.get("ELEMENT_RANKING_LOW_MEMORY", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


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
    judge_ids: Optional[Iterable[int]] = None,
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
    if judge_ids:
        stmt = stmt.where(
            ElementScorePerJudge.judge_id.in_([int(j) for j in judge_ids])
        )
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
    df["element_id"] = pd.to_numeric(df["element_id"], downcast="integer")
    df["judge_id"] = pd.to_numeric(df["judge_id"], downcast="integer")
    for col in ("discipline_type_id", "element_type_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast="integer")
    df["judge_score"] = df["judge_score"].astype(np.float32)
    return df


def compute_element_ranking_data_fingerprint(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[Iterable[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
) -> str:
    """
    Lightweight scope checksum: changes when element marks are added/updated/deleted
    in the filtered competition set (used to invalidate DB ranking caches).
    """
    where_clause = build_element_mark_filters(
        start_season_year, end_season_year, discipline_type_ids
    )
    # Same join order as ``load_element_marking_data`` (no ``select_from`` — avoids
    # SQLAlchemy adding a second ``competition`` alias when filtering on Competition).
    stmt = (
        select(
            func.count(ElementScorePerJudge.id),
            func.coalesce(func.max(ElementScorePerJudge.id), 0),
            func.count(func.distinct(Competition.id)),
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
    mark_count, max_mark_id, comp_count = session.execute(stmt).one()
    payload = f"{int(mark_count or 0)}:{int(max_mark_id or 0)}:{int(comp_count or 0)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def control_scores_by_element(df: pd.DataFrame) -> pd.DataFrame:
    """One row per element_id with panel median GOE (for low-memory judge drill-down)."""
    return (
        df.groupby("element_id", sort=False)["judge_score"]
        .median()
        .astype(np.float32)
        .rename("control_score")
        .reset_index()
    )


def compute_control_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Adds control_score (median GOE per element) and error."""
    out = df.copy()
    out["control_score"] = out.groupby("element_id", sort=False)["judge_score"].transform(
        "median"
    )
    out["error"] = out["judge_score"] - out["control_score"]
    return out


def apply_control_scores_from_table(
    df: pd.DataFrame, control_by_element: pd.DataFrame
) -> pd.DataFrame:
    if control_by_element.empty or df.empty:
        return pd.DataFrame()
    ctrl = control_by_element[["element_id", "control_score"]].copy()
    ctrl["element_id"] = ctrl["element_id"].astype("int64")
    out = df.copy()
    out["element_id"] = out["element_id"].astype("int64")
    out = out.merge(ctrl, on="element_id", how="inner")
    if out.empty:
        return out
    out["error"] = out["judge_score"] - out["control_score"]
    return out


def filter_element_ranking_season_years(years: Iterable[str]) -> list[str]:
    """Season codes from ``MIN_ELEMENT_RANKING_SEASON_YEAR`` onward (newest first)."""
    return sorted(
        {
            str(y).strip()
            for y in years
            if y
            and str(y).strip() >= MIN_ELEMENT_RANKING_SEASON_YEAR
        },
        reverse=True,
    )


def _season_index(years_sorted: list[str], season: str) -> int | None:
    try:
        return years_sorted.index(season)
    except ValueError:
        return None


def validate_element_ranking_scope(
    start_season_year: Optional[str],
    end_season_year: Optional[str],
    *,
    available_years: Optional[list[str]] = None,
) -> str | None:
    """Return an error message when a selected season year is not in the database."""
    if not available_years:
        return None
    ys = sorted({str(y).strip() for y in available_years if y}, reverse=True)
    if start_season_year and _season_index(ys, str(start_season_year)) is None:
        return "Selected start season year is not available in the database."
    if end_season_year and _season_index(ys, str(end_season_year)) is None:
        return "Selected end season year is not available in the database."
    return None


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
    id_map = id_map[["judge_id", "judge_name", "judge_ids"]].drop_duplicates("judge_id")
    out = df.copy()
    if "judge_name" in out.columns or "judge_ids" in out.columns:
        out = out.drop(columns=["judge_name", "judge_ids"], errors="ignore")
    out = out.merge(id_map, on="judge_id", how="left")
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
    grouped = df.dropna(subset=["discipline_type_id", "element_type_id"])
    if grouped.empty:
        return {}
    control_int = grouped["control_score"].round().astype(np.int16)
    stats = (
        grouped.assign(control_int=control_int)
        .groupby(
            ["discipline_type_id", "element_type_id", "control_int"],
            sort=False,
        )["error"]
        .agg(["std", "count"])
    )
    stats = stats[(stats["count"] >= min_bin_count) & stats["std"].notna() & (stats["std"] > 0)]
    return {
        (int(d), int(e), int(k)): float(sd)
        for (d, e, k), sd in stats["std"].items()
    }


def judge_ids_for_identity_label(analytics: JudgeAnalytics, judge_name: str) -> list[int]:
    for group in analytics.get_judge_analysis_identity_groups():
        if group["label"] == judge_name:
            return sorted({int(j) for j in group["judge_ids"]})
    return []


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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per judge × discipline and per judge × discipline × element type."""
    w = work.copy()
    w["discipline"] = w["discipline_type_id"].map(discipline_id_to_name)
    w["element_type"] = w["element_type_id"].map(element_type_id_to_name)

    disc_base = w.dropna(subset=["discipline"])
    if disc_base.empty:
        return pd.DataFrame(), pd.DataFrame()

    disc = (
        disc_base.groupby(["judge_name", "discipline"], sort=False)
        .agg(
            element_marks=("m_pj", "size"),
            partial_marking_score=("m_pj", _partial_marking_score),
            mean_goe_bias=("error", "mean"),
            mean_abs_error=("error", lambda s: float(s.abs().mean())),
            mean_sigma=("sigma_hat", "mean"),
        )
        .reset_index()
        .rename(
            columns={
                "judge_name": "Judge",
                "element_marks": "Element marks",
                "partial_marking_score": "Partial marking score",
                "mean_goe_bias": "Mean GOE bias",
                "mean_abs_error": "Mean |error|",
                "mean_sigma": "Mean σ̂",
            }
        )
    )

    elem_base = w.dropna(subset=["discipline", "element_type"])
    elem = (
        elem_base.groupby(
            ["judge_name", "discipline", "element_type"], sort=False
        )
        .agg(
            element_marks=("m_pj", "size"),
            partial_marking_score=("m_pj", _partial_marking_score),
            mean_goe_bias=("error", "mean"),
            mean_abs_error=("error", lambda s: float(s.abs().mean())),
        )
        .reset_index()
        .rename(
            columns={
                "judge_name": "Judge",
                "element_type": "Element type",
                "element_marks": "Element marks",
                "partial_marking_score": "Partial marking score",
                "mean_goe_bias": "Mean GOE bias",
                "mean_abs_error": "Mean |error|",
            }
        )
    )
    return disc, elem.sort_values(
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
    grouped = df.dropna(subset=["discipline_type_id", "element_type_id"])
    if grouped.empty:
        return pd.DataFrame()
    stats = (
        grouped.assign(
            control_int=grouped["control_score"].round().astype(np.int16)
        )
        .groupby(
            ["discipline_type_id", "element_type_id", "control_int"],
            sort=False,
        )["error"]
        .agg(["std", "count"])
        .reset_index()
    )
    rows = []
    for row in stats.itertuples(index=False):
        disc_id = int(row.discipline_type_id)
        elem_type_id = int(row.element_type_id)
        control_int = int(row.control_int)
        n = int(row.count)
        sd = float(row.std) if n >= 2 and pd.notna(row.std) else np.nan
        key = (disc_id, elem_type_id, control_int)
        in_model = key in params and n >= min_bin_count
        rows.append(
            {
                "Discipline": discipline_id_to_name.get(disc_id, str(disc_id)),
                "Element type": element_type_id_to_name.get(
                    elem_type_id, str(elem_type_id)
                ),
                "Control GOE": control_int,
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


def unpack_element_ranking_run_params(run_params: tuple) -> tuple:
    """Normalize 9-, 11-, or 12-tuples to ``(…, bench_start, bench_end, bench_scope)``."""
    if len(run_params) >= 12:
        return run_params[:12]
    if len(run_params) >= 11:
        return (*run_params[:11], None)
    base = run_params[:9] if len(run_params) >= 9 else run_params
    return (*base, None, None, None)


def benchmark_season_bounds(run_params: tuple) -> tuple[str | None, str | None]:
    """Season window for σ̂ fitting (``None`` = any season)."""
    rp = unpack_element_ranking_run_params(run_params)
    return rp[9], rp[10]


def benchmark_competition_scope(run_params: tuple) -> str:
    """Competition scope for σ̂ fitting (defaults to all competitions)."""
    rp = unpack_element_ranking_run_params(run_params)
    if rp[11]:
        return str(rp[11])
    return COMPETITION_SCOPE_ALL


def _mark_scope_identity(scope: dict[str, Any]) -> tuple:
    disc = scope.get("discipline_type_ids")
    return (
        scope.get("start_season_year"),
        scope.get("end_season_year"),
        tuple(disc) if disc else None,
        scope.get("competition_scope"),
    )


def uses_separate_benchmark_pool(run_params: tuple) -> bool:
    rp = unpack_element_ranking_run_params(run_params)
    if rp[4] or rp[5]:
        return True
    rank = ranking_scope_kwargs_from_run_params(run_params)
    bench = benchmark_scope_kwargs_from_run_params(run_params)
    return _mark_scope_identity(rank) != _mark_scope_identity(bench)


def run_params_ranking_compute_key(run_params: tuple) -> tuple:
    """Ranking mark scope (excludes minimum marks and benchmark seasons)."""
    rp = unpack_element_ranking_run_params(run_params)
    return (rp[0], rp[1], rp[2], rp[3], rp[4], rp[5], rp[7], rp[8])


def run_params_benchmark_compute_key(run_params: tuple) -> tuple:
    """σ̂ benchmark pool scope (no event dates; excludes minimum marks)."""
    bs, be = benchmark_season_bounds(run_params)
    rp = unpack_element_ranking_run_params(run_params)
    return (bs, be, rp[2], benchmark_competition_scope(run_params), rp[7], rp[8])


def run_params_compute_key(run_params: tuple) -> tuple:
    """Backward-compatible alias for ranking scope identity."""
    return run_params_ranking_compute_key(run_params)


def run_params_same_sigma_and_ranking_scope(a: tuple, b: tuple) -> bool:
    return (
        run_params_ranking_compute_key(a) == run_params_ranking_compute_key(b)
        and run_params_benchmark_compute_key(a) == run_params_benchmark_compute_key(b)
    )


def widest_benchmark_season_window(years: list[str]) -> tuple[str | None, str | None]:
    """Full element-ranking season span (oldest through newest)."""
    ys = filter_element_ranking_season_years(years)
    if not ys:
        return None, None
    return ys[-1], ys[0]


def ranking_scope_kwargs_from_run_params(run_params: tuple) -> dict[str, Any]:
    rp = unpack_element_ranking_run_params(run_params)
    return {
        "start_season_year": rp[0],
        "end_season_year": rp[1],
        "event_start_date": date.fromisoformat(rp[4]) if rp[4] else None,
        "event_end_date": date.fromisoformat(rp[5]) if rp[5] else None,
        "discipline_type_ids": list(rp[2]) if rp[2] else None,
        "competition_scope": rp[3],
    }


def benchmark_scope_kwargs_from_run_params(run_params: tuple) -> dict[str, Any]:
    bs, be = benchmark_season_bounds(run_params)
    rp = unpack_element_ranking_run_params(run_params)
    return {
        "start_season_year": bs,
        "end_season_year": be,
        "event_start_date": None,
        "event_end_date": None,
        "discipline_type_ids": list(rp[2]) if rp[2] else None,
        "competition_scope": benchmark_competition_scope(run_params),
    }


def fit_sigma_params_from_marks(
    df: pd.DataFrame, *, min_bin_count: int = MIN_BIN_COUNT
) -> dict:
    if df.empty:
        return {}
    work = df.copy()
    if "control_score" not in work.columns:
        work = compute_control_scores(work)
    return fit_sigma_discrete(work, min_bin_count=min_bin_count)


def _filter_judge_detail_by_names(
    jd: pd.DataFrame, je: pd.DataFrame, judge_names: set[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if jd is not None and not jd.empty and "Judge" in jd.columns:
        jd = jd.loc[jd["Judge"].isin(judge_names)]
    else:
        jd = pd.DataFrame()
    if je is not None and not je.empty and "Judge" in je.columns:
        je = je.loc[je["Judge"].isin(judge_names)]
    else:
        je = pd.DataFrame()
    return jd, je


def apply_min_marks_to_ranking_result(
    result: dict[str, Any], min_marks: int
) -> dict[str, Any]:
    """
    Re-apply the per-judge minimum marks threshold without recomputing σ̂ or panel medians.

    Requires ``judge_summary_all`` from a prior run (or legacy unfiltered tables).
    """
    judge_all = result.get("judge_summary_all")
    if not isinstance(judge_all, pd.DataFrame) or judge_all.empty:
        return result

    min_marks = int(min_marks or 0)
    judge_summary = apply_min_marks_filter(judge_all.copy(), min_marks)
    kept_names = set(judge_summary["judge_name"])
    out = dict(result)
    out["marking"] = build_ranking_display_table(judge_summary)
    out["summary"] = marking_score_summary(judge_summary)

    jd_all = result.get("judge_discipline_detail_all")
    je_all = result.get("judge_element_detail_all")
    if isinstance(jd_all, pd.DataFrame) and not jd_all.empty:
        out["judge_discipline_detail"], out["judge_element_detail"] = (
            _filter_judge_detail_by_names(jd_all, je_all, kept_names)
        )
    elif min_marks > 0:
        jd, je = result.get("judge_discipline_detail"), result.get("judge_element_detail")
        out["judge_discipline_detail"], out["judge_element_detail"] = (
            _filter_judge_detail_by_names(
                jd if isinstance(jd, pd.DataFrame) else pd.DataFrame(),
                je if isinstance(je, pd.DataFrame) else pd.DataFrame(),
                kept_names,
            )
        )
    return out


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


@dataclass(frozen=True)
class ElementRankingShard:
    """One season × one discipline slice of element marks."""

    season_year: str
    discipline_type_id: int
    competition_scope: str
    event_start_iso: str | None = None
    event_end_iso: str | None = None


def season_years_in_run_range(
    start_season_year: Optional[str],
    end_season_year: Optional[str],
    available_years: list[str],
) -> list[str]:
    ys = filter_element_ranking_season_years(available_years)
    if not start_season_year and not end_season_year:
        return ys
    out: list[str] = []
    for y in ys:
        if start_season_year and y < str(start_season_year):
            continue
        if end_season_year and y > str(end_season_year):
            continue
        out.append(y)
    return out


def discipline_uses_element_scoring(discipline_name: str) -> bool:
    return discipline_name.strip().lower() not in ELEMENT_RANKING_EXCLUDED_DISCIPLINE_NAMES


def element_ranking_discipline_types(
    analytics: JudgeAnalytics, competition_scope: str
) -> list[tuple[int, str]]:
    """Discipline types that participate in element GOE scoring (excludes Showcase, etc.)."""
    if competition_scope != COMPETITION_SCOPE_ALL:
        types = analytics.qualifying_event_segment_discipline_types()
    else:
        types = analytics.get_discipline_types()
    return [
        (int(dt_id), name)
        for dt_id, name in types
        if name and discipline_uses_element_scoring(name)
    ]


def element_ranking_discipline_names_for_scope(
    analytics: JudgeAnalytics, competition_scope: str
) -> list[str]:
    return [name for _dt_id, name in element_ranking_discipline_types(analytics, competition_scope)]


def discipline_ids_for_element_ranking(
    analytics: JudgeAnalytics,
    discipline_type_ids: Optional[list[int]],
    competition_scope: str,
) -> list[int]:
    allowed = {
        dt_id for dt_id, _ in element_ranking_discipline_types(analytics, competition_scope)
    }
    if discipline_type_ids:
        return [int(d) for d in discipline_type_ids if int(d) in allowed]
    return sorted(allowed)


def iter_element_ranking_shards(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
) -> list[ElementRankingShard]:
    years = season_years_in_run_range(
        start_season_year,
        end_season_year,
        [str(y) for y in analytics.get_years()],
    )
    disc_ids = discipline_ids_for_element_ranking(
        analytics, discipline_type_ids, competition_scope
    )
    event_start_iso = event_start_date.isoformat() if event_start_date else None
    event_end_iso = event_end_date.isoformat() if event_end_date else None
    return [
        ElementRankingShard(
            season_year=sy,
            discipline_type_id=dt_id,
            competition_scope=competition_scope,
            event_start_iso=event_start_iso,
            event_end_iso=event_end_iso,
        )
        for sy in years
        for dt_id in disc_ids
    ]


def finish_element_deviation_rankings_from_marks(
    analytics: JudgeAnalytics,
    df: pd.DataFrame,
    *,
    min_marks: int = 0,
    floor_sigma: float = FLOOR_SIGMA,
    min_bin_count: int = MIN_BIN_COUNT,
    include_judge_detail: bool | None = None,
    params: dict | None = None,
    sigma_reference_df: pd.DataFrame | None = None,
    benchmark_start_season_year: str | None = None,
    benchmark_end_season_year: str | None = None,
    benchmark_competition_scope_key: str | None = None,
) -> dict:
    """Judge summaries and tables from ranking-scope marks; optional pre-fitted σ̂."""
    disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
    elem_map = {int(i): n for i, n in analytics.get_element_types()}

    if df.empty:
        return {
            "marking": pd.DataFrame(),
            "summary": pd.DataFrame(),
            "sigma_bins": pd.DataFrame(),
            "judge_discipline_detail": pd.DataFrame(),
            "judge_element_detail": pd.DataFrame(),
            "control_by_element": pd.DataFrame(),
            "params": {},
            "n_raw_marks": 0,
            "n_sigma_buckets": 0,
            "error": "No element score rows found for the selected filters.",
            "low_memory": memory_efficient_mode(),
        }

    low_memory = (
        include_judge_detail is False
        if include_judge_detail is not None
        else memory_efficient_mode()
    )

    n_raw = len(df)
    if "judge_name" not in df.columns:
        df = attach_judge_identities(df, analytics)
    control_by_element = control_scores_by_element(df)
    df = compute_control_scores(df)
    if params is None:
        params = fit_sigma_discrete(df, min_bin_count=min_bin_count)
    work = annotate_normalized_marks(df, params, floor_sigma=floor_sigma)
    judge_summary_all = compute_judge_summaries(work)
    judge_summary = apply_min_marks_filter(judge_summary_all.copy(), min_marks)
    marking = build_ranking_display_table(judge_summary)
    summary = marking_score_summary(judge_summary)
    bins_df = sigma_reference_df if sigma_reference_df is not None else df
    if "control_score" not in bins_df.columns and not bins_df.empty:
        bins_df = compute_control_scores(bins_df.copy())
    sigma_bins = build_sigma_bins_dataframe(
        bins_df,
        params,
        disc_map,
        elem_map,
        min_bin_count=min_bin_count,
    )

    if low_memory:
        judge_discipline_detail = pd.DataFrame()
        judge_element_detail = pd.DataFrame()
        judge_discipline_detail_all = pd.DataFrame()
        judge_element_detail_all = pd.DataFrame()
    else:
        judge_discipline_detail_all, judge_element_detail_all = (
            compute_judge_discipline_breakdown(work, disc_map, elem_map)
        )
        kept_names = set(judge_summary["judge_name"])
        judge_discipline_detail, judge_element_detail = _filter_judge_detail_by_names(
            judge_discipline_detail_all,
            judge_element_detail_all,
            kept_names,
        )

    del df, work
    gc.collect()

    return {
        "marking": marking,
        "summary": summary,
        "sigma_bins": sigma_bins,
        "judge_summary_all": judge_summary_all,
        "judge_discipline_detail": judge_discipline_detail,
        "judge_element_detail": judge_element_detail,
        "judge_discipline_detail_all": judge_discipline_detail_all,
        "judge_element_detail_all": judge_element_detail_all,
        "control_by_element": control_by_element if low_memory else pd.DataFrame(),
        "params": params,
        "n_raw_marks": n_raw,
        "n_sigma_buckets": len(params),
        "error": None,
        "low_memory": low_memory,
        "benchmark_start_season_year": benchmark_start_season_year,
        "benchmark_end_season_year": benchmark_end_season_year,
        "benchmark_competition_scope": benchmark_competition_scope_key,
    }


def compute_judge_detail_for_identity(
    analytics: JudgeAnalytics,
    judge_name: str,
    control_by_element: pd.DataFrame,
    params: dict,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    floor_sigma: float = FLOOR_SIGMA,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load one identity's marks and build drill-down tables (uses precomputed panel medians)."""
    judge_ids = judge_ids_for_identity_label(analytics, judge_name)
    if not judge_ids or control_by_element.empty:
        return pd.DataFrame(), pd.DataFrame()

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
        judge_ids=judge_ids,
    )
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = apply_control_scores_from_table(df, control_by_element)
    df["judge_name"] = judge_name
    work = annotate_normalized_marks(df, params, floor_sigma=floor_sigma)
    return compute_judge_discipline_breakdown(work, disc_map, elem_map)


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
    include_judge_detail: bool | None = None,
) -> dict:
    """
    Run the full pipeline. Returns dict with keys:
    marking, summary, sigma_bins, judge_discipline_detail, judge_element_detail,
    params, n_raw_marks, n_sigma_buckets, error (str|None).
    """
    scope_err = validate_element_ranking_scope(start_season_year, end_season_year)
    if scope_err:
        return {
            "marking": pd.DataFrame(),
            "summary": pd.DataFrame(),
            "sigma_bins": pd.DataFrame(),
            "judge_discipline_detail": pd.DataFrame(),
            "judge_element_detail": pd.DataFrame(),
            "control_by_element": pd.DataFrame(),
            "params": {},
            "n_raw_marks": 0,
            "n_sigma_buckets": 0,
            "error": scope_err,
            "low_memory": memory_efficient_mode(),
        }

    from element_ranking_cache import run_element_deviation_ranking_pipeline

    return run_element_deviation_ranking_pipeline(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        min_marks=min_marks,
        floor_sigma=floor_sigma,
        min_bin_count=min_bin_count,
        include_judge_detail=include_judge_detail,
    )


def compute_element_deviation_rankings_from_run_params(
    analytics: JudgeAnalytics,
    run_params: tuple,
    *,
    include_judge_detail: bool | None = None,
    cache_only: bool = False,
    persist_shards: bool = True,
) -> dict:
    """Full pipeline from a 9-, 11-, or 12-element ``run_params`` tuple."""
    rp = unpack_element_ranking_run_params(run_params)
    bench_scope = benchmark_scope_kwargs_from_run_params(run_params)
    rank_scope = ranking_scope_kwargs_from_run_params(run_params)
    from element_ranking_cache import run_element_deviation_ranking_pipeline

    return run_element_deviation_ranking_pipeline(
        analytics,
        start_season_year=rank_scope["start_season_year"],
        end_season_year=rank_scope["end_season_year"],
        event_start_date=rank_scope["event_start_date"],
        event_end_date=rank_scope["event_end_date"],
        discipline_type_ids=rank_scope["discipline_type_ids"],
        competition_scope=rank_scope["competition_scope"],
        min_marks=int(rp[6] or 0),
        floor_sigma=float(rp[7]),
        min_bin_count=int(rp[8]),
        include_judge_detail=include_judge_detail,
        benchmark_start_season_year=bench_scope["start_season_year"],
        benchmark_end_season_year=bench_scope["end_season_year"],
        benchmark_competition_scope_key=bench_scope["competition_scope"],
        cache_only=cache_only,
        persist_shards=persist_shards,
    )
