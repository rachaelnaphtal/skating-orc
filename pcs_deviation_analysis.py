"""
PCS deviation ranking (panel-median / sigma model).

Mirrors ``element_deviation_ranking`` for program components: judges are ranked by
how closely their PCS marks track panel medians, after normalizing spread by
discipline, PCS component, and panel-median score bin (groupings of 1: 0.25–1,
1.25–2, etc.).
"""

from __future__ import annotations

import gc
import hashlib
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from analytics import JudgeAnalytics
from element_deviation_ranking import (
    ELEMENT_RANKING_LEVEL_FILTER_ALL,
    ELEMENT_RANKING_LEVEL_FILTER_LABELS,
    ELEMENT_RANKING_LEVEL_FILTER_PRESETS,
    apply_min_marks_filter,
    attach_judge_identities,
    judge_ids_for_identity_label,
    load_judge_identity_map,
    segment_levels_for_ranking_preset,
)
from models import (
    Competition,
    DisciplineType,
    PcsScorePerJudge,
    PcsType,
    Segment,
    SkaterSegment,
)
from officials_competition_types import COMPETITION_SCOPE_ALL
from pcs_quality_analysis import MIN_PCS_ANALYSIS_EVENT_DATE, pcs_component_label

MIN_PCS_DEVIATION_EVENT_DATE = MIN_PCS_ANALYSIS_EVENT_DATE
MIN_PCS_DEVIATION_SEASON_YEAR = "2223"
PCS_DEVIATION_DISCIPLINE_IDS = frozenset({1, 2, 3, 5})

FLOOR_SIGMA = 0.05
MIN_BIN_COUNT = 30

PCS_DEVIATION_SHARD_MARK_COLUMNS = (
    "judge_id",
    "judge_name",
    "skater_segment_id",
    "pcs_type_id",
    "component",
    "discipline_type_id",
    "judge_score",
    "control_score",
)

PCS_DEVIATION_COMPETITION_SCOPES: tuple[str, ...] = (
    COMPETITION_SCOPE_ALL,
    "qualifying",
    "sectionals_and_championships",
    "championships_only",
    "international",
)

PCS_DEVIATION_COMPETITION_SCOPE_LABELS: tuple[str, ...] = (
    "All competitions",
    "Qualifying only",
    "Sectionals & championships",
    "Championships only",
    "International",
)

PCS_DEVIATION_SCOPE_LABEL_TO_KEY: dict[str, str] = {
    "All competitions": COMPETITION_SCOPE_ALL,
    "Qualifying only": "qualifying",
    "Sectionals & championships": "sectionals_and_championships",
    "Championships only": "championships_only",
    "International": "international",
}


def pcs_deviation_competition_scope_key(scope_label: str) -> str:
    return PCS_DEVIATION_SCOPE_LABEL_TO_KEY.get(scope_label, COMPETITION_SCOPE_ALL)


def control_bin_from_median(panel_median: float) -> int:
    """
  Map panel median PCS to a width-1 bin labeled k where scores fall in
  [k − 0.75, k] (e.g. 0.25–1, 1.25–2, 2.25–3).
    """
    if pd.isna(panel_median):
        return 1
    return max(1, int(np.ceil(float(panel_median) - 0.25)))


def control_bin_label(control_bin: int) -> str:
    lo = float(control_bin) - 0.75
    hi = float(control_bin)
    return f"{lo:.2f}–{hi:.2f}"


def filter_pcs_deviation_season_years(years: Iterable[str]) -> list[str]:
    return sorted(
        {
            str(y).strip()
            for y in years
            if y and str(y).strip() >= MIN_PCS_DEVIATION_SEASON_YEAR
        },
        reverse=True,
    )


def season_years_in_run_range(
    start_season_year: Optional[str],
    end_season_year: Optional[str],
    available_years: list[str],
) -> list[str]:
    ys = filter_pcs_deviation_season_years(available_years)
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


@dataclass(frozen=True)
class PcsDeviationShard:
    """One season × one discipline slice of PCS deviation marks."""

    season_year: str
    discipline_type_id: int
    competition_scope: str
    event_start_iso: str | None = None
    event_end_iso: str | None = None
    segment_level_preset: str | None = None


def iter_pcs_deviation_shards(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    segment_level_preset: str | None = None,
) -> list[PcsDeviationShard]:
    years = season_years_in_run_range(
        start_season_year,
        end_season_year,
        [str(y) for y in analytics.get_years()],
    )
    disc_ids = discipline_ids_for_pcs_deviation(
        analytics, discipline_type_ids, competition_scope
    )
    event_start_iso = event_start_date.isoformat() if event_start_date else None
    event_end_iso = event_end_date.isoformat() if event_end_date else None
    return [
        PcsDeviationShard(
            season_year=sy,
            discipline_type_id=dt_id,
            competition_scope=competition_scope,
            event_start_iso=event_start_iso,
            event_end_iso=event_end_iso,
            segment_level_preset=segment_level_preset,
        )
        for sy in years
        for dt_id in disc_ids
    ]


def compute_pcs_deviation_data_fingerprint(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    segment_levels: Optional[Iterable[str]] = None,
) -> str:
    """Checksum for PCS marks in scope (invalidates shard cache when data changes)."""
    seg_discipline_ids = _segment_discipline_ids(
        analytics, discipline_type_ids, competition_scope
    )
    if not seg_discipline_ids:
        return hashlib.sha256(b"empty").hexdigest()
    effective_start = _effective_start(event_start_date)
    stmt = (
        select(
            func.count(PcsScorePerJudge.id),
            func.coalesce(func.max(PcsScorePerJudge.id), 0),
            func.count(func.distinct(Segment.competition_id)),
        )
        .select_from(PcsScorePerJudge)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
    )
    stmt = _apply_scope_filters(
        stmt,
        analytics,
        seg_discipline_ids=seg_discipline_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        effective_start=effective_start,
        event_end_date=event_end_date,
        competition_scope=competition_scope,
        segment_levels=segment_levels,
    )
    mark_count, max_mark_id, comp_count = session.execute(stmt).one()
    payload = f"{int(mark_count or 0)}:{int(max_mark_id or 0)}:{int(comp_count or 0)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def attach_judge_identities_with_map(
    df: pd.DataFrame, id_map: pd.DataFrame
) -> pd.DataFrame:
    """Merge judge identities without re-querying the database."""
    if id_map.empty:
        raise RuntimeError("No judges found for identity mapping.")
    lookup = id_map[["judge_id", "judge_name", "judge_ids"]].drop_duplicates("judge_id")
    out = df.copy()
    if "judge_name" in out.columns or "judge_ids" in out.columns:
        out = out.drop(columns=["judge_name", "judge_ids"], errors="ignore")
    out = out.merge(lookup, on="judge_id", how="left")
    missing = out["judge_name"].isna().sum()
    if missing:
        out["judge_name"] = out["judge_name"].fillna(
            out["judge_id"].astype(str).radd("judge:")
        )
        out["judge_ids"] = out["judge_ids"].fillna(out["judge_id"].astype(str))
    return out


def normalize_pcs_deviation_shard_marks(
    df: pd.DataFrame,
    analytics: JudgeAnalytics,
    *,
    id_map: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=list(PCS_DEVIATION_SHARD_MARK_COLUMNS))
    if "judge_id" not in df.columns:
        raise ValueError("Shard marks missing required column: judge_id")
    # Remap judge_name from judge_id so identity-link changes apply without
    # rewriting mark shards.
    if id_map is not None:
        out = attach_judge_identities_with_map(df, id_map)
    else:
        out = attach_judge_identities(df, analytics)
    for col in PCS_DEVIATION_SHARD_MARK_COLUMNS:
        if col not in out.columns:
            raise ValueError(f"Shard marks missing required column: {col}")
    return out[list(PCS_DEVIATION_SHARD_MARK_COLUMNS)].copy()


def validate_pcs_deviation_scope(
    start_season_year: Optional[str],
    end_season_year: Optional[str],
    *,
    available_years: Optional[list[str]] = None,
) -> str | None:
    if not available_years:
        return None
    ys = sorted({str(y).strip() for y in available_years if y}, reverse=True)
    if start_season_year and str(start_season_year) not in ys:
        return "Selected start season year is not available in the database."
    if end_season_year and str(end_season_year) not in ys:
        return "Selected end season year is not available in the database."
    return None


def pcs_deviation_discipline_types(
    analytics: JudgeAnalytics, competition_scope: str
) -> list[tuple[int, str]]:
    all_types = (
        analytics.qualifying_event_segment_discipline_types()
        if competition_scope != COMPETITION_SCOPE_ALL
        else analytics.get_discipline_types()
    )
    return [
        (int(dt_id), name)
        for dt_id, name in all_types
        if int(dt_id) in PCS_DEVIATION_DISCIPLINE_IDS
    ]


def pcs_deviation_discipline_names_for_scope(
    analytics: JudgeAnalytics, competition_scope: str
) -> list[str]:
    return [name for _dt_id, name in pcs_deviation_discipline_types(analytics, competition_scope)]


def discipline_ids_for_pcs_deviation(
    analytics: JudgeAnalytics,
    discipline_type_ids: Optional[list[int]],
    competition_scope: str,
) -> list[int]:
    allowed = {dt_id for dt_id, _ in pcs_deviation_discipline_types(analytics, competition_scope)}
    if discipline_type_ids:
        return [int(d) for d in discipline_type_ids if int(d) in allowed]
    return sorted(allowed)


def _effective_start(event_start_date: date | None) -> date:
    if event_start_date is None:
        return MIN_PCS_DEVIATION_EVENT_DATE
    return max(event_start_date, MIN_PCS_DEVIATION_EVENT_DATE)


def _segment_discipline_ids(
    analytics: JudgeAnalytics,
    discipline_type_ids: Optional[list[int]],
    competition_scope: str,
) -> list[int]:
    core = [d for d in PCS_DEVIATION_DISCIPLINE_IDS]
    if discipline_type_ids:
        core = [d for d in core if d in {int(x) for x in discipline_type_ids}]
    if competition_scope != COMPETITION_SCOPE_ALL:
        allowed = {dt_id for dt_id, _ in analytics.qualifying_event_segment_discipline_types()}
        core = [d for d in core if d in allowed]
    return sorted(core)


def _apply_scope_filters(
    query,
    analytics: JudgeAnalytics,
    *,
    seg_discipline_ids: list[int],
    start_season_year: Optional[str],
    end_season_year: Optional[str],
    effective_start: date,
    event_end_date: date | None,
    competition_scope: str,
    segment_levels: Optional[Iterable[str]] = None,
):
    query = query.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
    if start_season_year:
        query = query.filter(Competition.year >= str(start_season_year))
    if end_season_year:
        query = query.filter(Competition.year <= str(end_season_year))
    if segment_levels:
        query = query.filter(Segment.level.in_(list(segment_levels)))
    query = analytics._filter_orm_competition_scope(query, competition_scope)
    return analytics._apply_competition_event_date_range(
        query, effective_start, event_end_date
    )


def load_pcs_deviation_marks(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    segment_level_preset: str | None = None,
    judge_ids: Optional[Iterable[int]] = None,
) -> pd.DataFrame:
    """PCS marks with panel median per skater×component."""
    seg_discipline_ids = _segment_discipline_ids(
        analytics, discipline_type_ids, competition_scope
    )
    if not seg_discipline_ids:
        return pd.DataFrame()

    segment_levels = segment_levels_for_ranking_preset(segment_level_preset)
    effective_start = _effective_start(event_start_date)

    panel_sq = (
        select(
            PcsScorePerJudge.skater_segment_id.label("skater_segment_id"),
            PcsScorePerJudge.pcs_type_id.label("pcs_type_id"),
            func.percentile_cont(0.5)
            .within_group(PcsScorePerJudge.judge_score)
            .label("control_score"),
        )
        .select_from(PcsScorePerJudge)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
    )
    panel_sq = _apply_scope_filters(
        panel_sq,
        analytics,
        seg_discipline_ids=seg_discipline_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        effective_start=effective_start,
        event_end_date=event_end_date,
        competition_scope=competition_scope,
        segment_levels=segment_levels,
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
            Segment.discipline_type_id,
            DisciplineType.name.label("discipline_name"),
            panel_sq.c.control_score,
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
    marks_q = _apply_scope_filters(
        marks_q,
        analytics,
        seg_discipline_ids=seg_discipline_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        effective_start=effective_start,
        event_end_date=event_end_date,
        competition_scope=competition_scope,
        segment_levels=segment_levels,
    )
    if judge_ids is not None:
        judge_id_list = [int(j) for j in judge_ids]
        if judge_id_list:
            marks_q = marks_q.where(PcsScorePerJudge.judge_id.in_(judge_id_list))

    df = pd.read_sql(marks_q, analytics.session.bind)
    if df.empty:
        return df
    df["judge_id"] = pd.to_numeric(df["judge_id"], downcast="integer")
    df["discipline_type_id"] = pd.to_numeric(df["discipline_type_id"], downcast="integer")
    df["judge_score"] = df["judge_score"].astype(np.float32)
    df["control_score"] = df["control_score"].astype(np.float32)
    df["component"] = df["pcs_type_name"].map(pcs_component_label)
    return df.dropna(subset=["component", "discipline_type_id"]).copy()


def compute_errors(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["error"] = out["judge_score"] - out["control_score"]
    out["control_bin"] = out["control_score"].map(control_bin_from_median).astype(np.int16)
    return out


def fit_sigma_discrete_pcs(
    df: pd.DataFrame,
    *,
    min_bin_count: int = MIN_BIN_COUNT,
) -> dict:
    """σ̂ for each (discipline_type_id, component, control_bin)."""
    grouped = df.dropna(subset=["discipline_type_id", "component", "control_bin"])
    if grouped.empty:
        return {}
    stats = (
        grouped.groupby(
            ["discipline_type_id", "component", "control_bin"],
            sort=False,
        )["error"]
        .agg(["std", "count"])
    )
    stats = stats[(stats["count"] >= min_bin_count) & stats["std"].notna() & (stats["std"] > 0)]
    return {
        (int(d), str(c), int(k)): float(sd)
        for (d, c, k), sd in stats["std"].items()
    }


def sigma_hat_row_pcs(
    control_score: float,
    disc_id,
    component,
    params: dict,
    *,
    floor_sigma: float = FLOOR_SIGMA,
    fallback_sigma: float = 0.3,
) -> float:
    if pd.isna(disc_id) or component is None or (isinstance(component, float) and pd.isna(component)):
        return max(floor_sigma, fallback_sigma)
    k = control_bin_from_median(control_score)
    key = (int(disc_id), str(component), k)
    if key not in params:
        for dk in (-1, 1, -2, 2):
            alt_key = (int(disc_id), str(component), k + dk)
            if alt_key in params:
                return max(params[alt_key], floor_sigma)
        return max(floor_sigma, fallback_sigma)
    return max(params[key], floor_sigma)


def _params_to_lookup_df(params: dict) -> pd.DataFrame:
    if not params:
        return pd.DataFrame(
            columns=["discipline_type_id", "component", "control_bin", "sigma_lookup"]
        )
    rows = [
        {
            "discipline_type_id": int(d),
            "component": str(c),
            "control_bin": int(k),
            "sigma_lookup": float(s),
        }
        for (d, c, k), s in params.items()
    ]
    return pd.DataFrame(rows)


def annotate_normalized_marks_pcs(
    df: pd.DataFrame,
    params: dict,
    *,
    floor_sigma: float = FLOOR_SIGMA,
) -> pd.DataFrame:
    fallback = max(floor_sigma, float(df["error"].std(ddof=1) or 0.3))
    lookup = _params_to_lookup_df(params)
    work = df.copy()
    if "control_bin" not in work.columns:
        work["control_bin"] = work["control_score"].map(control_bin_from_median)
    work["sigma_hat"] = np.nan
    work["sigma_source"] = pd.Series(pd.NA, index=work.index, dtype="string")

    if not lookup.empty:
        fitted = work.merge(
            lookup,
            on=["discipline_type_id", "component", "control_bin"],
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
            neighbor["control_bin"] = neighbor["control_bin"] + dk
            merged = work.loc[miss_mask].merge(
                neighbor,
                on=["discipline_type_id", "component", "control_bin"],
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


def compute_judge_summaries_pcs(work: pd.DataFrame) -> pd.DataFrame:
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


def compute_mergeable_judge_summary_pcs(work: pd.DataFrame) -> pd.DataFrame:
    """Per-judge sums mergeable across shards before sqrt(mean(m²))."""
    if work.empty:
        return pd.DataFrame(
            columns=[
                "judge_name",
                "n_marks",
                "sum_m2",
                "sum_error",
                "sum_abs_error",
                "sum_sigma",
                "sum_abs_m",
            ]
        )
    g = work.groupby("judge_name", sort=False)
    return g.agg(
        n_marks=("m_pj", "size"),
        sum_m2=("m_pj", lambda s: float((s**2).sum())),
        sum_error=("error", "sum"),
        sum_abs_error=("error", lambda s: float(s.abs().sum())),
        sum_sigma=("sigma_hat", "sum"),
        sum_abs_m=("m_pj", lambda s: float(s.abs().sum())),
    ).reset_index()


def fit_sigma_params_from_marks(
    df: pd.DataFrame, *, min_bin_count: int = MIN_BIN_COUNT
) -> dict:
    if df.empty:
        return {}
    work = compute_errors(df.copy())
    return fit_sigma_discrete_pcs(work, min_bin_count=min_bin_count)


def build_ranking_display_table_pcs(judge_summary: pd.DataFrame) -> pd.DataFrame:
    return judge_summary.rename(
        columns={
            "judge_name": "Judge",
            "n_marks": "PCS marks",
            "marking_score": "Marking score",
            "mean_error": "Mean PCS bias",
            "mean_abs_error": "Mean |error|",
            "mean_sigma_hat": "Mean σ̂",
            "mean_abs_m": "Mean |m|",
        }
    )[
        [
            "rank",
            "Judge",
            "Marking score",
            "PCS marks",
            "Mean PCS bias",
            "Mean |error|",
            "Mean σ̂",
            "Mean |m|",
        ]
    ]


def marking_score_summary_pcs(judge_summary: pd.DataFrame) -> pd.DataFrame:
    if judge_summary.empty:
        return pd.DataFrame()
    desc = judge_summary["marking_score"].describe()
    return pd.DataFrame(
        {
            "Statistic": desc.index.astype(str),
            "Value": [round(float(v), 6) if pd.notna(v) else None for v in desc.values],
        }
    )


def compute_judge_discipline_breakdown_pcs(
    work: pd.DataFrame,
    discipline_id_to_name: dict[int, str],
) -> pd.DataFrame:
    w = work.copy()
    w["discipline"] = w["discipline_type_id"].map(discipline_id_to_name)
    base = w.dropna(subset=["discipline"])
    if base.empty:
        return pd.DataFrame()
    return (
        base.groupby(["judge_name", "discipline"], sort=False)
        .agg(
            pcs_marks=("m_pj", "size"),
            partial_marking_score=("m_pj", _partial_marking_score),
            mean_pcs_bias=("error", "mean"),
            mean_abs_error=("error", lambda s: float(s.abs().mean())),
            mean_sigma=("sigma_hat", "mean"),
        )
        .reset_index()
        .rename(
            columns={
                "judge_name": "Judge",
                "pcs_marks": "PCS marks",
                "partial_marking_score": "Partial marking score",
                "mean_pcs_bias": "Mean PCS bias",
                "mean_abs_error": "Mean |error|",
                "mean_sigma": "Mean σ̂",
            }
        )
    )


def compute_judge_component_breakdown_pcs(
    work: pd.DataFrame,
    discipline_id_to_name: dict[int, str],
) -> pd.DataFrame:
    w = work.copy()
    w["discipline"] = w["discipline_type_id"].map(discipline_id_to_name)
    base = w.dropna(subset=["discipline", "component"])
    if base.empty:
        return pd.DataFrame()
    return (
        base.groupby(["judge_name", "discipline", "component"], sort=False)
        .agg(
            pcs_marks=("m_pj", "size"),
            partial_marking_score=("m_pj", _partial_marking_score),
            mean_pcs_bias=("error", "mean"),
            mean_abs_error=("error", lambda s: float(s.abs().mean())),
        )
        .reset_index()
        .rename(
            columns={
                "judge_name": "Judge",
                "component": "Component",
                "pcs_marks": "PCS marks",
                "partial_marking_score": "Partial marking score",
                "mean_pcs_bias": "Mean PCS bias",
                "mean_abs_error": "Mean |error|",
            }
        )
        .sort_values(["Judge", "Partial marking score"], ascending=[True, False])
    )


def compute_judge_control_bin_breakdown_pcs(
    work: pd.DataFrame,
    discipline_id_to_name: dict[int, str],
) -> pd.DataFrame:
    w = work.copy()
    if "control_bin" not in w.columns:
        w["control_bin"] = w["control_score"].map(control_bin_from_median)
    w["discipline"] = w["discipline_type_id"].map(discipline_id_to_name)
    w["Control median range"] = w["control_bin"].map(control_bin_label)
    base = w.dropna(subset=["discipline", "component", "control_bin"])
    if base.empty:
        return pd.DataFrame()
    return (
        base.groupby(
            ["judge_name", "discipline", "component", "control_bin", "Control median range"],
            sort=False,
        )
        .agg(
            pcs_marks=("m_pj", "size"),
            partial_marking_score=("m_pj", _partial_marking_score),
            mean_pcs_bias=("error", "mean"),
            mean_abs_error=("error", lambda s: float(s.abs().mean())),
        )
        .reset_index()
        .rename(
            columns={
                "judge_name": "Judge",
                "component": "Component",
                "control_bin": "Control bin",
                "pcs_marks": "PCS marks",
                "partial_marking_score": "Partial marking score",
                "mean_pcs_bias": "Mean PCS bias",
                "mean_abs_error": "Mean |error|",
            }
        )
        .sort_values(["Judge", "Partial marking score"], ascending=[True, False])
    )


def build_sigma_bins_dataframe_pcs(
    df: pd.DataFrame,
    params: dict,
    discipline_id_to_name: dict[int, str],
    *,
    min_bin_count: int = MIN_BIN_COUNT,
) -> pd.DataFrame:
    grouped = df.dropna(subset=["discipline_type_id", "component", "control_bin"])
    if grouped.empty:
        return pd.DataFrame()
    stats = (
        grouped.groupby(
            ["discipline_type_id", "component", "control_bin"],
            sort=False,
        )["error"]
        .agg(["std", "count"])
        .reset_index()
    )
    rows = []
    for row in stats.itertuples(index=False):
        disc_id = int(row.discipline_type_id)
        component = str(row.component)
        control_bin = int(row.control_bin)
        n = int(row.count)
        sd = float(row.std) if n >= 2 and pd.notna(row.std) else np.nan
        key = (disc_id, component, control_bin)
        in_model = key in params and n >= min_bin_count
        rows.append(
            {
                "Discipline": discipline_id_to_name.get(disc_id, str(disc_id)),
                "Component": component,
                "Control median range": control_bin_label(control_bin),
                "Marks in bin": n,
                "Error stdev (all marks)": round(sd, 4) if pd.notna(sd) else None,
                "σ̂ used in model": round(params[key], 4) if in_model else None,
                "In model": in_model,
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values(
        ["Discipline", "Component", "Control median range"], ascending=[True, True, True]
    ).reset_index(drop=True)


def _filter_judge_detail_by_names(
    jd: pd.DataFrame, jc: pd.DataFrame, jb: pd.DataFrame, judge_names: set[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def _filt(df: pd.DataFrame) -> pd.DataFrame:
        if df is not None and not df.empty and "Judge" in df.columns:
            return df.loc[df["Judge"].isin(judge_names)]
        return pd.DataFrame()

    return _filt(jd), _filt(jc), _filt(jb)


def apply_min_marks_to_pcs_deviation_result(
    result: dict[str, Any],
    min_marks: int,
) -> dict[str, Any]:
    judge_all = result.get("judge_summary_all")
    if not isinstance(judge_all, pd.DataFrame) or judge_all.empty:
        out = dict(result)
        out["_min_marks_filter_applied"] = False
        return out

    min_marks = int(min_marks or 0)
    judge_summary = apply_min_marks_filter(judge_all.copy(), min_marks)
    kept_names = set(judge_summary["judge_name"])
    out = dict(result)
    out["judge_summary_all"] = judge_all
    marking = build_ranking_display_table_pcs(judge_summary)
    if min_marks > 0 and not marking.empty:
        counts = pd.to_numeric(marking["PCS marks"], errors="coerce").fillna(0)
        marking = marking.loc[counts >= min_marks].copy()
        marking = marking.sort_values("Marking score").reset_index(drop=True)
        marking["rank"] = range(1, len(marking) + 1)
    out["marking"] = marking
    out["summary"] = marking_score_summary_pcs(judge_summary.loc[judge_summary["judge_name"].isin(kept_names)])
    out["_min_marks_filter_applied"] = True

    jd_all = result.get("judge_discipline_detail_all")
    jc_all = result.get("judge_component_detail_all")
    jb_all = result.get("judge_control_bin_detail_all")
    if isinstance(jd_all, pd.DataFrame):
        out["judge_discipline_detail"], out["judge_component_detail"], out["judge_control_bin_detail"] = (
            _filter_judge_detail_by_names(jd_all, jc_all, jb_all, kept_names)
        )
    return out


def ranking_scope_kwargs_from_run_params(run_params: tuple) -> dict[str, Any]:
    rp = unpack_pcs_deviation_run_params(run_params)
    return {
        "start_season_year": rp[0],
        "end_season_year": rp[1],
        "event_start_date": date.fromisoformat(rp[4]) if rp[4] else None,
        "event_end_date": date.fromisoformat(rp[5]) if rp[5] else None,
        "discipline_type_ids": list(rp[2]) if rp[2] else None,
        "competition_scope": rp[3],
        "segment_level_preset": rp[12],
    }


def benchmark_scope_kwargs_from_run_params(run_params: tuple) -> dict[str, Any]:
    rp = unpack_pcs_deviation_run_params(run_params)
    return {
        "start_season_year": rp[9],
        "end_season_year": rp[10],
        "event_start_date": None,
        "event_end_date": None,
        "discipline_type_ids": list(rp[2]) if rp[2] else None,
        "competition_scope": rp[11] or COMPETITION_SCOPE_ALL,
        "segment_level_preset": rp[13] if rp[13] is not None else rp[12],
    }


def unpack_pcs_deviation_run_params(run_params: tuple) -> tuple:
    if len(run_params) >= 14:
        return run_params[:14]
    if len(run_params) >= 9:
        base = run_params[:9]
        return (*base, None, None, None, None, None)
    raise ValueError(f"PCS deviation run_params must have at least 9 elements, got {len(run_params)}")


def benchmark_season_bounds(run_params: tuple) -> tuple[str | None, str | None]:
    rp = unpack_pcs_deviation_run_params(run_params)
    return rp[9], rp[10]


def benchmark_competition_scope(run_params: tuple) -> str:
    rp = unpack_pcs_deviation_run_params(run_params)
    if rp[11]:
        return str(rp[11])
    return COMPETITION_SCOPE_ALL


def benchmark_segment_level_preset(run_params: tuple) -> str | None:
    rp = unpack_pcs_deviation_run_params(run_params)
    return rp[13]


def run_params_ranking_compute_key(run_params: tuple) -> tuple:
    rp = unpack_pcs_deviation_run_params(run_params)
    return (rp[0], rp[1], rp[2], rp[3], rp[4], rp[5], rp[7], rp[8], rp[12])


def run_params_benchmark_compute_key(run_params: tuple) -> tuple:
    bs, be = benchmark_season_bounds(run_params)
    rp = unpack_pcs_deviation_run_params(run_params)
    return (
        bs,
        be,
        rp[2],
        benchmark_competition_scope(run_params),
        rp[7],
        rp[8],
        benchmark_segment_level_preset(run_params),
    )


def run_params_same_sigma_and_ranking_scope(a: tuple, b: tuple) -> bool:
    ra = unpack_pcs_deviation_run_params(a)
    rb = unpack_pcs_deviation_run_params(b)
    rank_a = (ra[0], ra[1], ra[2], ra[3], ra[4], ra[5], ra[7], ra[8], ra[12])
    rank_b = (rb[0], rb[1], rb[2], rb[3], rb[4], rb[5], rb[7], rb[8], rb[12])
    bench_a = (ra[9], ra[10], ra[2], ra[11] or COMPETITION_SCOPE_ALL, ra[7], ra[8], ra[13] if ra[13] is not None else ra[12])
    bench_b = (rb[9], rb[10], rb[2], rb[11] or COMPETITION_SCOPE_ALL, rb[7], rb[8], rb[13] if rb[13] is not None else rb[12])
    return rank_a == rank_b and bench_a == bench_b


def uses_separate_benchmark_pool(run_params: tuple) -> bool:
    rp = unpack_pcs_deviation_run_params(run_params)
    if rp[4] or rp[5]:
        return True
    rank = ranking_scope_kwargs_from_run_params(run_params)
    bench = benchmark_scope_kwargs_from_run_params(run_params)
    rank_id = (
        rank["start_season_year"],
        rank["end_season_year"],
        tuple(rank["discipline_type_ids"] or ()),
        rank["competition_scope"],
        rank["segment_level_preset"],
    )
    bench_id = (
        bench["start_season_year"],
        bench["end_season_year"],
        tuple(bench["discipline_type_ids"] or ()),
        bench["competition_scope"],
        bench["segment_level_preset"],
    )
    return rank_id != bench_id


def finish_pcs_deviation_rankings_from_marks(
    analytics: JudgeAnalytics,
    df: pd.DataFrame,
    *,
    min_marks: int = 0,
    floor_sigma: float = FLOOR_SIGMA,
    min_bin_count: int = MIN_BIN_COUNT,
    params: dict | None = None,
    sigma_reference_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    disc_map = {int(i): n for i, n in analytics.get_discipline_types()}

    if df.empty:
        return {
            "marking": pd.DataFrame(),
            "summary": pd.DataFrame(),
            "sigma_bins": pd.DataFrame(),
            "judge_discipline_detail": pd.DataFrame(),
            "judge_component_detail": pd.DataFrame(),
            "judge_control_bin_detail": pd.DataFrame(),
            "params": {},
            "n_raw_marks": 0,
            "n_sigma_buckets": 0,
            "error": "No PCS score rows found for the selected filters.",
        }

    n_raw = len(df)
    if "judge_id" in df.columns:
        df = attach_judge_identities(df, analytics)
    df = compute_errors(df)
    if params is None:
        params = fit_sigma_discrete_pcs(df, min_bin_count=min_bin_count)
    work = annotate_normalized_marks_pcs(df, params, floor_sigma=floor_sigma)
    judge_summary_all = compute_judge_summaries_pcs(work)
    judge_summary = apply_min_marks_filter(judge_summary_all.copy(), min_marks)
    marking = build_ranking_display_table_pcs(judge_summary)
    summary = marking_score_summary_pcs(judge_summary)

    bins_df = sigma_reference_df if sigma_reference_df is not None else df
    if "error" not in bins_df.columns and not bins_df.empty:
        bins_df = compute_errors(bins_df.copy())
    sigma_bins = build_sigma_bins_dataframe_pcs(
        bins_df, params, disc_map, min_bin_count=min_bin_count
    )

    judge_discipline_detail_all = compute_judge_discipline_breakdown_pcs(work, disc_map)
    judge_component_detail_all = compute_judge_component_breakdown_pcs(work, disc_map)
    judge_control_bin_detail_all = compute_judge_control_bin_breakdown_pcs(work, disc_map)
    kept_names = set(judge_summary["judge_name"])
    judge_discipline_detail, judge_component_detail, judge_control_bin_detail = (
        _filter_judge_detail_by_names(
            judge_discipline_detail_all,
            judge_component_detail_all,
            judge_control_bin_detail_all,
            kept_names,
        )
    )

    del df, work
    gc.collect()

    return {
        "marking": marking,
        "summary": summary,
        "sigma_bins": sigma_bins,
        "judge_summary_all": judge_summary_all,
        "judge_discipline_detail": judge_discipline_detail,
        "judge_component_detail": judge_component_detail,
        "judge_control_bin_detail": judge_control_bin_detail,
        "judge_discipline_detail_all": judge_discipline_detail_all,
        "judge_component_detail_all": judge_component_detail_all,
        "judge_control_bin_detail_all": judge_control_bin_detail_all,
        "params": params,
        "n_raw_marks": n_raw,
        "n_sigma_buckets": len(params),
        "error": None,
    }


def compute_pcs_deviation_rankings(
    analytics: JudgeAnalytics,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    segment_level_preset: str | None = None,
    min_marks: int = 0,
    floor_sigma: float = FLOOR_SIGMA,
    min_bin_count: int = MIN_BIN_COUNT,
    benchmark_start_season_year: Optional[str] = None,
    benchmark_end_season_year: Optional[str] = None,
    benchmark_competition_scope: str | None = None,
    benchmark_segment_level_preset: str | None = None,
    cache_only: bool = False,
    persist_shards: bool = True,
) -> dict[str, Any]:
    from pcs_deviation_cache import run_pcs_deviation_ranking_pipeline

    scope_err = validate_pcs_deviation_scope(start_season_year, end_season_year)
    if scope_err:
        empty = finish_pcs_deviation_rankings_from_marks(
            analytics, pd.DataFrame(), min_marks=min_marks
        )
        empty["error"] = scope_err
        return empty

    return run_pcs_deviation_ranking_pipeline(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        segment_level_preset=segment_level_preset,
        min_marks=min_marks,
        floor_sigma=floor_sigma,
        min_bin_count=min_bin_count,
        benchmark_start_season_year=benchmark_start_season_year,
        benchmark_end_season_year=benchmark_end_season_year,
        benchmark_competition_scope_key=benchmark_competition_scope,
        benchmark_segment_level_preset=benchmark_segment_level_preset,
        cache_only=cache_only,
        persist_shards=persist_shards,
    )


def compute_pcs_deviation_rankings_from_run_params(
    analytics: JudgeAnalytics,
    run_params: tuple,
    *,
    cache_only: bool = False,
    persist_shards: bool = True,
) -> dict[str, Any]:
    rp = unpack_pcs_deviation_run_params(run_params)
    rank = ranking_scope_kwargs_from_run_params(run_params)
    bench = benchmark_scope_kwargs_from_run_params(run_params)
    return compute_pcs_deviation_rankings(
        analytics,
        start_season_year=rank["start_season_year"],
        end_season_year=rank["end_season_year"],
        event_start_date=rank["event_start_date"],
        event_end_date=rank["event_end_date"],
        discipline_type_ids=rank["discipline_type_ids"],
        competition_scope=rank["competition_scope"],
        segment_level_preset=rank["segment_level_preset"],
        min_marks=int(rp[6] or 0),
        floor_sigma=float(rp[7]),
        min_bin_count=int(rp[8]),
        benchmark_start_season_year=bench["start_season_year"],
        benchmark_end_season_year=bench["end_season_year"],
        benchmark_competition_scope=bench["competition_scope"],
        benchmark_segment_level_preset=bench["segment_level_preset"],
        cache_only=cache_only,
        persist_shards=persist_shards,
    )


def compute_judge_detail_for_identity_pcs(
    analytics: JudgeAnalytics,
    judge_name: str,
    params: dict,
    *,
    start_season_year: Optional[str] = None,
    end_season_year: Optional[str] = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: Optional[list[int]] = None,
    segment_level_preset: str | None = None,
    competition_scope: str = COMPETITION_SCOPE_ALL,
    floor_sigma: float = FLOOR_SIGMA,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from pcs_deviation_cache import collect_marks_for_judge_detail

    judge_ids = judge_ids_for_identity_label(analytics, judge_name)
    if not judge_ids:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = collect_marks_for_judge_detail(
        analytics,
        judge_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        segment_level_preset=segment_level_preset,
    )
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
    df["judge_name"] = judge_name
    df = compute_errors(df)
    work = annotate_normalized_marks_pcs(df, params, floor_sigma=floor_sigma)
    return (
        compute_judge_discipline_breakdown_pcs(work, disc_map),
        compute_judge_component_breakdown_pcs(work, disc_map),
        compute_judge_control_bin_breakdown_pcs(work, disc_map),
    )


__all__ = [
    "ELEMENT_RANKING_LEVEL_FILTER_ALL",
    "ELEMENT_RANKING_LEVEL_FILTER_LABELS",
    "ELEMENT_RANKING_LEVEL_FILTER_PRESETS",
    "FLOOR_SIGMA",
    "MIN_BIN_COUNT",
    "MIN_PCS_DEVIATION_EVENT_DATE",
    "MIN_PCS_DEVIATION_SEASON_YEAR",
    "PCS_DEVIATION_COMPETITION_SCOPE_LABELS",
    "PCS_DEVIATION_DISCIPLINE_IDS",
    "apply_min_marks_to_pcs_deviation_result",
    "compute_judge_detail_for_identity_pcs",
    "compute_pcs_deviation_rankings",
    "compute_pcs_deviation_rankings_from_run_params",
    "control_bin_from_median",
    "control_bin_label",
    "filter_pcs_deviation_season_years",
    "pcs_deviation_competition_scope_key",
    "pcs_deviation_discipline_names_for_scope",
    "pcs_deviation_discipline_types",
    "run_params_ranking_compute_key",
    "run_params_same_sigma_and_ranking_scope",
    "unpack_pcs_deviation_run_params",
    "uses_separate_benchmark_pool",
    "validate_pcs_deviation_scope",
]
