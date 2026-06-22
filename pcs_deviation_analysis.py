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
from officials_competition_types import (
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_INTERNATIONAL,
    COMPETITION_SCOPE_ISU_EVENT,
    COMPETITION_SCOPE_QUALIFYING,
    COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
)
from pcs_quality_analysis import MIN_PCS_ANALYSIS_EVENT_DATE, pcs_component_label

MIN_PCS_DEVIATION_EVENT_DATE = MIN_PCS_ANALYSIS_EVENT_DATE
MIN_PCS_DEVIATION_SEASON_YEAR = "2223"
PCS_DEVIATION_DISCIPLINE_IDS = frozenset({1, 2, 3, 5})

FLOOR_SIGMA = 0.05
MIN_BIN_COUNT_DISCRETE = 10
MIN_BIN_COUNT_QUADRATIC = 10
MIN_BIN_COUNT = 10
MIN_BINS_FOR_QUADRATIC_FIT = 4
# σ̂ bins: fixed-width steps in panel control score (used for rankings and plots).
PCS_SIGMA_BIN_WIDTH_DISCRETE = 0.25
PCS_SIGMA_BIN_WIDTH_QUADRATIC = 0.125
PCS_SIGMA_BIN_WIDTH = PCS_SIGMA_BIN_WIDTH_DISCRETE
PCS_SIGMA_PLOT_BIN_WIDTH = PCS_SIGMA_BIN_WIDTH
PCS_SIGMA_PLOT_MIN_BIN_COUNT = MIN_BIN_COUNT
PCS_SIGMA_PER_COMPETITION_MIN_MARKS = 30
# Sample (not population) spread of marking errors within each bin.
SIGMA_SAMPLE_DDOF = 1

PCS_SIGMA_MODEL_DISCRETE = "discrete"
PCS_SIGMA_MODEL_QUADRATIC = "quadratic"
PCS_SIGMA_MODEL_LABELS: dict[str, str] = {
    PCS_SIGMA_MODEL_DISCRETE: "Discrete bins (production default)",
    PCS_SIGMA_MODEL_QUADRATIC: "Quadratic variance (smooth σ̂ vs control score)",
}

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
    COMPETITION_SCOPE_QUALIFYING,
    COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_INTERNATIONAL,
    COMPETITION_SCOPE_ISU_EVENT,
)

PCS_DEVIATION_COMPETITION_SCOPE_LABELS: tuple[str, ...] = (
    "All competitions",
    "Qualifying only",
    "Sectionals & championships",
    "Championships only",
    "International",
    "ISU events",
)

PCS_DEVIATION_SCOPE_LABEL_TO_KEY: dict[str, str] = {
    "All competitions": COMPETITION_SCOPE_ALL,
    "Qualifying only": COMPETITION_SCOPE_QUALIFYING,
    "Sectionals & championships": COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    "Championships only": COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    "International": COMPETITION_SCOPE_INTERNATIONAL,
    "ISU events": COMPETITION_SCOPE_ISU_EVENT,
}


def pcs_deviation_competition_scope_key(scope_label: str) -> str:
    return PCS_DEVIATION_SCOPE_LABEL_TO_KEY.get(scope_label, COMPETITION_SCOPE_ALL)


def pcs_sigma_bin_width_for_model(sigma_model: str | None) -> float:
    """Discrete σ̂ uses 0.25 PCS bins; quadratic uses 0.125 (centers at 0.125, 0.25, …)."""
    if normalize_sigma_model(sigma_model) == PCS_SIGMA_MODEL_QUADRATIC:
        return PCS_SIGMA_BIN_WIDTH_QUADRATIC
    return PCS_SIGMA_BIN_WIDTH_DISCRETE


def control_bin_from_median(panel_median: float) -> int:
    """
  Map panel median PCS to a width-1 bin labeled k where scores fall in
  [k − 0.75, k] (e.g. 0.25–1, 1.25–2, 2.25–3).

    Legacy width-1 bins; production σ̂ uses ``sigma_bin_from_control_score``.
    """
    if pd.isna(panel_median):
        return 1
    return max(1, int(np.floor(float(panel_median) + 0.75)))


def sigma_bin_from_control_score(
    control_score: float,
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
) -> float:
    """Round control score to the nearest σ̂ bin center (width set by model or ``bin_width``)."""
    width = float(bin_width)
    if width <= 0:
        raise ValueError("bin_width must be positive")
    return round(float(control_score) / width) * width


def sigma_bin_label(
    sigma_bin: float,
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
) -> str:
    """Display range for a σ̂ bin centered at ``sigma_bin``."""
    b = float(sigma_bin)
    half = float(bin_width) / 2.0
    return f"{b - half:.2f}–{b + half:.2f}"


def sigma_bin_neighbor_offsets(
    *,
    bin_width: float | None = None,
) -> tuple[float, ...]:
    """Neighbor steps in units of ``bin_width`` (±1, ±2 bins)."""
    w = float(bin_width if bin_width is not None else PCS_SIGMA_BIN_WIDTH_DISCRETE)
    return (-w, w, -2 * w, 2 * w)


def control_bin_label(control_bin: int) -> str:
    lo = float(control_bin) - 0.75
    hi = float(control_bin)
    return f"{lo:.2f}–{hi:.2f}"


def control_bin_center(control_bin: int) -> float:
    """Midpoint of the panel-median PCS range for bin *k* ([k−0.75, k])."""
    return float(control_bin) - 0.375


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


def _pcs_deviation_marks_scope_query(
    analytics: JudgeAnalytics,
    *,
    seg_discipline_ids: list[int],
    start_season_year: Optional[str],
    end_season_year: Optional[str],
    effective_start: date,
    event_end_date: date | None,
    competition_scope: str,
    segment_levels: Optional[Iterable[str]],
):
    """Scoped PCS mark rows (joins + filters) shared by panel median and output."""
    marks_q = (
        select(
            PcsScorePerJudge.judge_id,
            PcsScorePerJudge.skater_segment_id,
            PcsScorePerJudge.pcs_type_id,
            PcsType.name.label("pcs_type_name"),
            PcsScorePerJudge.judge_score,
            Segment.discipline_type_id,
            DisciplineType.name.label("discipline_name"),
            Competition.id.label("competition_id"),
        )
        .select_from(PcsScorePerJudge)
        .join(PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
        .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)
    )
    return _apply_scope_filters(
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

    # PostgreSQL does not support percentile_cont(...) OVER (...), so apply scope
    # joins/filters once in a CTE, aggregate panel medians from that, then join back.
    marks_cte = _pcs_deviation_marks_scope_query(
        analytics,
        seg_discipline_ids=seg_discipline_ids,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        effective_start=effective_start,
        event_end_date=event_end_date,
        competition_scope=competition_scope,
        segment_levels=segment_levels,
    ).cte("pcs_deviation_marks_scope")

    panel_sq = (
        select(
            marks_cte.c.skater_segment_id,
            marks_cte.c.pcs_type_id,
            func.percentile_cont(0.5)
            .within_group(marks_cte.c.judge_score)
            .label("control_score"),
        )
        .group_by(marks_cte.c.skater_segment_id, marks_cte.c.pcs_type_id)
        .subquery("pcs_deviation_panel_medians")
    )

    marks_q = select(
        marks_cte.c.judge_id,
        marks_cte.c.skater_segment_id,
        marks_cte.c.pcs_type_id,
        marks_cte.c.pcs_type_name,
        marks_cte.c.judge_score,
        marks_cte.c.discipline_type_id,
        marks_cte.c.discipline_name,
        marks_cte.c.competition_id,
        panel_sq.c.control_score,
    ).select_from(
        marks_cte.join(
            panel_sq,
            and_(
                marks_cte.c.skater_segment_id == panel_sq.c.skater_segment_id,
                marks_cte.c.pcs_type_id == panel_sq.c.pcs_type_id,
            ),
        )
    )
    if judge_ids is not None:
        judge_id_list = [int(j) for j in judge_ids]
        if judge_id_list:
            marks_q = marks_q.where(marks_cte.c.judge_id.in_(judge_id_list))

    df = pd.read_sql(marks_q, analytics.session.bind)
    if df.empty:
        return df
    df["judge_id"] = pd.to_numeric(df["judge_id"], downcast="integer")
    df["discipline_type_id"] = pd.to_numeric(df["discipline_type_id"], downcast="integer")
    df["judge_score"] = df["judge_score"].astype(np.float32)
    df["control_score"] = df["control_score"].astype(np.float32)
    df["component"] = df["pcs_type_name"].map(pcs_component_label)
    return df.dropna(subset=["component", "discipline_type_id"]).copy()


def compute_errors(
    df: pd.DataFrame,
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH_DISCRETE,
) -> pd.DataFrame:
    out = df.copy()
    out["error"] = out["judge_score"] - out["control_score"]
    out["control_bin"] = (
        out["control_score"]
        .map(lambda c: sigma_bin_from_control_score(c, bin_width=bin_width))
        .astype(np.float32)
    )
    return out


def normalize_sigma_model(model: str | None) -> str:
    if model == PCS_SIGMA_MODEL_QUADRATIC:
        return PCS_SIGMA_MODEL_QUADRATIC
    return PCS_SIGMA_MODEL_DISCRETE


def min_bin_count_for_sigma_model(sigma_model: str | None) -> int:
    """Minimum marks per 0.25 PCS σ̂ bin (same for discrete and quadratic)."""
    _ = normalize_sigma_model(sigma_model)
    return MIN_BIN_COUNT


def sigma_model_from_run_params(run_params: tuple) -> str:
    if len(run_params) >= 15:
        return normalize_sigma_model(run_params[14])
    return PCS_SIGMA_MODEL_DISCRETE


def sample_error_variance(errors: pd.Series | np.ndarray) -> float:
    """Unbiased sample variance of marking errors (Bessel correction, ddof=1)."""
    s = pd.Series(errors).dropna()
    n = len(s)
    if n < 2:
        return float("nan")
    return float(s.var(ddof=SIGMA_SAMPLE_DDOF))


def sample_error_stdev(errors: pd.Series | np.ndarray) -> float:
    """Sample standard deviation of marking errors (sqrt of unbiased sample variance)."""
    var = sample_error_variance(errors)
    if not np.isfinite(var) or var <= 0:
        return float("nan")
    return float(np.sqrt(var))


def aggregate_pcs_sigma_bin_stats(
    df: pd.DataFrame,
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
    min_bin_count: int = 0,
) -> pd.DataFrame:
    """
    Per-bin sample spread of PCS marking errors (``PCS_SIGMA_BIN_WIDTH`` steps).

    Uses ddof=1 throughout because bins are finite samples, not the full population.
    """
    work = df.dropna(subset=["discipline_type_id", "component", "control_score", "error"]).copy()
    if work.empty:
        return pd.DataFrame(
            columns=[
                "discipline_type_id",
                "component",
                "control_bin",
                "control_score_mean",
                "variance_empirical",
                "sigma_empirical",
                "count",
            ]
        )
    if "control_bin" not in work.columns:
        work = compute_errors(work)
    work = work.copy()
    work["control_bin"] = work["control_score"].map(
        lambda c: sigma_bin_from_control_score(c, bin_width=bin_width)
    )

    group_cols = ["discipline_type_id", "component", "control_bin"]
    spread = (
        work.groupby(group_cols, sort=False)["error"]
        .agg(
            count="count",
            variance_empirical=lambda s: sample_error_variance(s),
            sigma_empirical=lambda s: sample_error_stdev(s),
        )
        .reset_index()
    )
    means = (
        work.groupby(group_cols, sort=False)["control_score"]
        .mean()
        .reset_index()
        .rename(columns={"control_score": "control_score_mean"})
    )
    stats = spread.merge(means, on=group_cols, how="left")
    min_n = max(2, int(min_bin_count))
    stats = stats[
        stats["count"] >= min_n
        & stats["variance_empirical"].notna()
        & (stats["variance_empirical"] > 0)
        & stats["sigma_empirical"].notna()
        & (stats["sigma_empirical"] > 0)
    ]
    return stats


def fine_control_bin_key(control_score: float, *, bin_width: float = PCS_SIGMA_BIN_WIDTH) -> float:
    """Backward-compatible alias for ``sigma_bin_from_control_score``."""
    return sigma_bin_from_control_score(control_score, bin_width=bin_width)


def aggregate_pcs_sigma_fine_bin_stats(
    df: pd.DataFrame,
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
    min_bin_count: int = 0,
) -> pd.DataFrame:
    """Backward-compatible alias for ``aggregate_pcs_sigma_bin_stats``."""
    return aggregate_pcs_sigma_bin_stats(
        df, bin_width=bin_width, min_bin_count=min_bin_count
    )


def collect_sigma_plot_stats_pcs(
    df: pd.DataFrame,
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
    min_bin_count: int = MIN_BIN_COUNT,
) -> pd.DataFrame:
    """Empirical sample σ by σ̂ bin (same bins as production rankings)."""
    work = df.dropna(subset=["discipline_type_id", "component", "control_score", "error"])
    if work.empty:
        return pd.DataFrame(
            columns=[
                "discipline_type_id",
                "component",
                "control_bin",
                "control_score_mean",
                "variance_empirical",
                "sigma_empirical",
                "count",
            ]
        )
    if "error" not in work.columns:
        work = compute_errors(work)
    return aggregate_pcs_sigma_bin_stats(
        work, bin_width=bin_width, min_bin_count=min_bin_count
    )


def collect_sigma_per_competition_stats_pcs(
    df: pd.DataFrame,
    *,
    min_marks: int = PCS_SIGMA_PER_COMPETITION_MIN_MARKS,
) -> pd.DataFrame:
    """One empirical σ per competition (pooled over all marks in that event)."""
    work = df.dropna(subset=["discipline_type_id", "component", "control_score", "error"])
    if work.empty or "competition_id" not in work.columns:
        return pd.DataFrame(
            columns=[
                "discipline_type_id",
                "component",
                "competition_id",
                "control_score_mean",
                "variance_empirical",
                "sigma_empirical",
                "count",
            ]
        )
    if "error" not in work.columns:
        work = compute_errors(work)
    rows: list[dict[str, Any]] = []
    group_cols = ["discipline_type_id", "component", "competition_id"]
    for key, grp in work.groupby(group_cols, sort=False):
        if len(grp) < int(min_marks):
            continue
        var = sample_error_variance(grp["error"])
        sd = sample_error_stdev(grp["error"])
        if not np.isfinite(var) or var <= 0 or not np.isfinite(sd) or sd <= 0:
            continue
        disc_id, component, comp_id = key
        rows.append(
            {
                "discipline_type_id": int(disc_id),
                "component": str(component),
                "competition_id": int(comp_id),
                "control_score_mean": float(grp["control_score"].mean()),
                "variance_empirical": float(var),
                "sigma_empirical": float(sd),
                "count": int(len(grp)),
            }
        )
    return pd.DataFrame(rows)


def fit_direct_sigma_quadratic_from_stats(stats: pd.DataFrame) -> dict[str, float] | None:
    """Weighted quadratic in σ (slide-style), not variance."""
    if stats is None or stats.empty or len(stats) < 3:
        return None
    x = stats["control_score_mean"].astype(float).values
    y = stats["sigma_empirical"].astype(float).values
    w = stats["count"].astype(float).values
    if len(x) < 3:
        return None
    coeffs = np.polyfit(x, y, deg=2, w=w)
    y_hat = np.polyval(coeffs, x)
    rmse = float(np.sqrt(np.average((y - y_hat) ** 2, weights=w)))
    return {
        "direct_sigma_a": float(coeffs[0]),
        "direct_sigma_b": float(coeffs[1]),
        "direct_sigma_c0": float(coeffs[2]),
        "rmse_direct_sigma": rmse,
        "n_plot_bins": float(len(stats)),
    }


def direct_sigma_quadratic_eval(
    control_score: float | np.ndarray,
    a: float,
    b: float,
    c0: float,
    *,
    floor_sigma: float = FLOOR_SIGMA,
) -> float | np.ndarray:
    """Evaluate slide-style σ̂(c) = a·c² + b·c + c₀, floored at ``floor_sigma``."""
    c_arr = np.asarray(control_score, dtype=float)
    return np.maximum(float(floor_sigma), a * c_arr**2 + b * c_arr + c0)


def direct_sigma_quadratic_equation_str(params: dict[str, float]) -> str:
    a = float(params["direct_sigma_a"])
    b = float(params["direct_sigma_b"])
    c0 = float(params["direct_sigma_c0"])
    return f"σ̂(c) = {c0:.3f} + {b:.4f}·c + {a:.5f}·c²"


def heteroscedasticity_annotation_text(
    variance_params: dict[str, float],
    *,
    direct_fit: dict[str, float] | None = None,
) -> str:
    rmse_var = variance_params.get("rmse_sigma", np.nan)
    lines = [
        quadratic_sigma_equation_str(variance_params),
        f"RMSE (variance fit, σ) = {rmse_var:.3f}",
    ]
    if direct_fit and all(
        k in direct_fit for k in ("direct_sigma_a", "direct_sigma_b", "direct_sigma_c0")
    ):
        lines.append(direct_sigma_quadratic_equation_str(direct_fit))
        lines.append(f"RMSE (direct σ on plot bins) = {direct_fit['rmse_direct_sigma']:.3f}")
    return "<br>".join(lines)


def attach_direct_sigma_plot_metadata(
    params: dict[tuple[int, str], dict[str, float]],
    df: pd.DataFrame,
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
    plot_min_bin_count: int = MIN_BIN_COUNT,
    per_competition_min_marks: int = PCS_SIGMA_PER_COMPETITION_MIN_MARKS,
) -> dict[tuple[int, str], dict[str, float]]:
    """Add slide-style direct-σ fit and per-competition counts to quadratic params."""
    if not params or df is None or df.empty:
        return params
    work = compute_errors(df.copy()) if "error" not in df.columns else df
    out = dict(params)
    for key in list(out):
        disc_id, component = key
        series = work[
            (work["discipline_type_id"] == int(disc_id))
            & (work["component"] == str(component))
        ]
        if series.empty:
            continue
        plot_stats = collect_sigma_plot_stats_pcs(
            series, bin_width=bin_width, min_bin_count=plot_min_bin_count
        )
        direct = fit_direct_sigma_quadratic_from_stats(plot_stats)
        if direct:
            out[key] = {**out[key], **direct}
        per_comp = collect_sigma_per_competition_stats_pcs(
            series, min_marks=per_competition_min_marks
        )
        out[key]["n_competitions_plot"] = float(len(per_comp))
    return out


def build_pcs_heteroscedasticity_figure(
    plot_stats: pd.DataFrame,
    variance_params: dict[str, float],
    *,
    title: str,
    per_competition_stats: pd.DataFrame | None = None,
    x_max: float = 10.0,
    floor_sigma: float = FLOOR_SIGMA,
    bin_width: float = PCS_SIGMA_BIN_WIDTH_QUADRATIC,
):
    """Fine-bin scatter, optional per-competition overlay, variance + direct σ curves."""
    if plot_stats.empty or not variance_params:
        return None

    import plotly.graph_objects as go

    direct_fit = (
        variance_params
        if all(k in variance_params for k in ("direct_sigma_a", "direct_sigma_b", "direct_sigma_c0"))
        else fit_direct_sigma_quadratic_from_stats(plot_stats)
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=plot_stats["control_score_mean"],
            y=plot_stats["sigma_empirical"],
            mode="markers",
            name=f"σ bins ({bin_width:g} PCS)",
            marker=dict(
                size=9,
                color=plot_stats["count"],
                colorscale="RdYlGn_r",
                cmin=float(plot_stats["count"].min()),
                cmax=float(plot_stats["count"].max()),
                showscale=True,
                colorbar=dict(title="Marks"),
                line=dict(width=0.5, color="rgba(0,0,0,0.3)"),
            ),
            hovertemplate="c_p=%{x:.2f}<br>σ=%{y:.3f}<extra></extra>",
        )
    )

    if per_competition_stats is not None and not per_competition_stats.empty:
        fig.add_trace(
            go.Scatter(
                x=per_competition_stats["control_score_mean"],
                y=per_competition_stats["sigma_empirical"],
                mode="markers",
                name="Per competition",
                marker=dict(
                    size=7,
                    color="rgba(80,80,80,0.45)",
                    symbol="circle-open",
                    line=dict(width=1.5, color="rgba(60,60,60,0.7)"),
                ),
                hovertemplate="c_p=%{x:.2f}<br>σ=%{y:.3f}<extra></extra>",
            )
        )

    x_min = max(0.5, float(plot_stats["control_score_mean"].min()) - 0.5)
    xs = np.linspace(x_min, x_max, 200)
    ys_var = quadratic_sigma_eval(
        xs, variance_params["a"], variance_params["b"], variance_params["c"]
    )
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys_var,
            mode="lines",
            name="Variance fit (√Var̂)",
            line=dict(color="black", width=2),
        )
    )
    if direct_fit:
        ys_direct = direct_sigma_quadratic_eval(
            xs,
            direct_fit["direct_sigma_a"],
            direct_fit["direct_sigma_b"],
            direct_fit["direct_sigma_c0"],
            floor_sigma=floor_sigma,
        )
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys_direct,
                mode="lines",
                name="Direct σ fit (plot bins)",
                line=dict(color="black", width=2, dash="dash"),
            )
        )

    fig.update_layout(
        title=title,
        xaxis_title="Control score c_p",
        yaxis_title="Intrinsic judging error variability σ̂(c_p)",
        template="plotly_white",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        annotations=[
            dict(
                x=0.98,
                y=0.02,
                xref="paper",
                yref="paper",
                showarrow=False,
                align="right",
                text=heteroscedasticity_annotation_text(
                    variance_params, direct_fit=direct_fit
                ),
                font=dict(size=10),
            )
        ],
    )
    return fig


def fit_sigma_discrete_pcs(
    df: pd.DataFrame,
    *,
    min_bin_count: int = MIN_BIN_COUNT,
    bin_width: float = PCS_SIGMA_BIN_WIDTH_DISCRETE,
) -> dict:
    """σ̂ for each (discipline_type_id, component, control_bin) using sample stdev (ddof=1)."""
    if df.empty:
        return {}
    work = compute_errors(df.copy(), bin_width=bin_width)
    grouped = work.dropna(subset=["discipline_type_id", "component", "control_bin"])
    if grouped.empty:
        return {}
    stats = aggregate_pcs_sigma_bin_stats(
        grouped, min_bin_count=min_bin_count, bin_width=bin_width
    )
    return {
        (int(row.discipline_type_id), str(row.component), float(row.control_bin)): float(
            row.sigma_empirical
        )
        for row in stats.itertuples(index=False)
    }


def collect_sigma_bin_stats_pcs(
    df: pd.DataFrame,
    *,
    min_bin_count: int = 0,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
) -> pd.DataFrame:
    """
    Empirical sample variance / σ by *(discipline, component, control bin)*.

    Returns columns: discipline_type_id, component, control_bin, control_score_mean,
    variance_empirical, sigma_empirical, count.
    """
    work = df.dropna(subset=["discipline_type_id", "component", "control_score", "error"])
    if work.empty:
        return pd.DataFrame(
            columns=[
                "discipline_type_id",
                "component",
                "control_bin",
                "control_score_mean",
                "variance_empirical",
                "sigma_empirical",
                "count",
            ]
        )
    if "control_bin" not in work.columns:
        work = compute_errors(work)
    return aggregate_pcs_sigma_bin_stats(
        work, bin_width=bin_width, min_bin_count=min_bin_count
    )


def quadratic_variance_eval(
    control_score: float | np.ndarray,
    a: float,
    b: float,
    c: float,
) -> float | np.ndarray:
    """Sample variance estimate as a quadratic in control score: Var(c) = a·c² + b·c + c₀."""
    c_arr = np.asarray(control_score, dtype=float)
    return a * c_arr**2 + b * c_arr + c


def quadratic_sigma_from_variance_eval(
    control_score: float | np.ndarray,
    a: float,
    b: float,
    c: float,
    *,
    floor_sigma: float = FLOOR_SIGMA,
) -> float | np.ndarray:
    """σ̂(c) = sqrt(max(floor², fitted sample variance at c))."""
    var = quadratic_variance_eval(control_score, a, b, c)
    floor_var = float(floor_sigma) ** 2
    return np.sqrt(np.maximum(floor_var, var))


def quadratic_sigma_eval(
    control_score: float | np.ndarray,
    a: float,
    b: float,
    c: float,
) -> float | np.ndarray:
    """Backward-compatible alias: evaluate σ from a variance-polynomial parameterization."""
    return quadratic_sigma_from_variance_eval(control_score, a, b, c)


def quadratic_sigma_equation_str(params: dict[str, float]) -> str:
    """Human-readable variance fit; σ is the square root of this expression."""
    a, b, c0 = float(params["a"]), float(params["b"]), float(params["c"])
    return f"Var̂(c) = {c0:.4f} + {b:.4f}·c + {a:.5f}·c²  →  σ̂(c)=√Var̂(c)"


def fit_sigma_quadratic_pcs(
    df: pd.DataFrame,
    *,
    min_bin_count: int = MIN_BIN_COUNT,
    min_bins_for_fit: int = MIN_BINS_FOR_QUADRATIC_FIT,
    floor_sigma: float = FLOOR_SIGMA,
    bin_width: float = PCS_SIGMA_BIN_WIDTH_QUADRATIC,
) -> dict[tuple[int, str], dict[str, float]]:
    """
    Weighted quadratic sample-variance model per *(discipline_type_id, component)*.

    Fits bin-level unbiased sample variances (ddof=1, weighted by mark count) against
    mean panel control score in each bin. σ̂(c) is the square root of the fitted
  variance, floored at ``floor_sigma``.
    """
    work = compute_errors(df.copy(), bin_width=bin_width)
    stats = collect_sigma_bin_stats_pcs(
        work, min_bin_count=min_bin_count, bin_width=bin_width
    )
    if stats.empty:
        return {}

    params: dict[tuple[int, str], dict[str, float]] = {}
    for (disc_id, component), grp in stats.groupby(
        ["discipline_type_id", "component"], sort=False
    ):
        if len(grp) < int(min_bins_for_fit):
            continue
        x = grp["control_score_mean"].astype(float).values
        y = grp["variance_empirical"].astype(float).values
        w = grp["count"].astype(float).values
        if len(x) < 3:
            continue
        coeffs = np.polyfit(x, y, deg=2, w=w)
        y_hat = np.polyval(coeffs, x)
        rmse = float(np.sqrt(np.average((y - y_hat) ** 2, weights=w)))
        sigma_hat = quadratic_sigma_from_variance_eval(
            x, coeffs[0], coeffs[1], coeffs[2], floor_sigma=floor_sigma
        )
        rmse_sigma = float(np.sqrt(np.average((grp["sigma_empirical"].values - sigma_hat) ** 2, weights=w)))
        params[(int(disc_id), str(component))] = {
            "a": float(coeffs[0]),
            "b": float(coeffs[1]),
            "c": float(coeffs[2]),
            "rmse_variance": rmse,
            "rmse_sigma": rmse_sigma,
            "n_bins": float(len(grp)),
            "n_marks": float(grp["count"].sum()),
            "fit_target": "sample_variance",
            "sample_ddof": float(SIGMA_SAMPLE_DDOF),
        }
    return attach_direct_sigma_plot_metadata(
        params,
        work,
        bin_width=bin_width,
        plot_min_bin_count=min_bin_count,
    )


def _quadratic_params_to_df(params: dict[tuple[int, str], dict[str, float]]) -> pd.DataFrame:
    if not params:
        return pd.DataFrame(
            columns=["discipline_type_id", "component", "a", "b", "c"]
        )
    rows = [
        {
            "discipline_type_id": int(d),
            "component": str(c),
            "a": float(p["a"]),
            "b": float(p["b"]),
            "c": float(p["c"]),
        }
        for (d, c), p in params.items()
    ]
    return pd.DataFrame(rows)


def annotate_normalized_marks_pcs_quadratic(
    df: pd.DataFrame,
    params: dict[tuple[int, str], dict[str, float]],
    *,
    floor_sigma: float = FLOOR_SIGMA,
) -> pd.DataFrame:
    """Like ``annotate_normalized_marks_pcs`` but σ̂ from a continuous quadratic in control score."""
    work = df.copy()
    if "error" not in work.columns:
        work = compute_errors(work, bin_width=PCS_SIGMA_BIN_WIDTH_QUADRATIC)
    fallback = max(floor_sigma, float(work["error"].std(ddof=1) or 0.3))
    coeffs = _quadratic_params_to_df(params)
    work["sigma_hat"] = np.nan
    work["sigma_source"] = pd.Series(pd.NA, index=work.index, dtype="string")

    if not coeffs.empty:
        merged = work.merge(coeffs, on=["discipline_type_id", "component"], how="left")
        fit_mask = merged["a"].notna()
        if fit_mask.any():
            c_score = merged.loc[fit_mask, "control_score"].astype(float)
            merged.loc[fit_mask, "sigma_hat"] = quadratic_sigma_from_variance_eval(
                c_score.values,
                merged.loc[fit_mask, "a"].values,
                merged.loc[fit_mask, "b"].values,
                merged.loc[fit_mask, "c"].values,
                floor_sigma=floor_sigma,
            )
            merged.loc[fit_mask, "sigma_source"] = "quadratic"
        fb = merged["sigma_hat"].isna()
        merged.loc[fb, "sigma_hat"] = fallback
        merged.loc[fb, "sigma_source"] = "fallback"
        merged["sigma_hat"] = merged["sigma_hat"].clip(lower=floor_sigma)
        merged["m_pj"] = merged["error"] / merged["sigma_hat"]
        return merged

    work["sigma_hat"] = fallback
    work["sigma_source"] = "fallback"
    work["sigma_hat"] = work["sigma_hat"].clip(lower=floor_sigma)
    work["m_pj"] = work["error"] / work["sigma_hat"]
    return work


def compare_pcs_sigma_model_rankings(
    df: pd.DataFrame,
    *,
    discrete_params: dict | None = None,
    quadratic_params: dict[tuple[int, str], dict[str, float]] | None = None,
    floor_sigma: float = FLOOR_SIGMA,
    min_bin_count: int = MIN_BIN_COUNT,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """
    Compare judge marking scores under discrete-bin vs quadratic σ̂ models.

    Returns per-judge comparison table and summary metrics (rank correlation, etc.).
    """
    work = compute_errors(df.copy()) if "error" not in df.columns else df.copy()
    if discrete_params is None:
        discrete_params = fit_sigma_discrete_pcs(work, min_bin_count=min_bin_count)
    if quadratic_params is None:
        quadratic_params = fit_sigma_quadratic_pcs(
            work,
            min_bin_count=min_bin_count,
            floor_sigma=floor_sigma,
        )

    disc_work = annotate_normalized_marks_pcs(
        work, discrete_params, floor_sigma=floor_sigma
    )
    quad_work = annotate_normalized_marks_pcs_quadratic(
        work, quadratic_params, floor_sigma=floor_sigma
    )
    disc_sum = compute_judge_summaries_pcs(disc_work)[
        ["judge_name", "marking_score", "n_marks"]
    ]
    quad_sum = compute_judge_summaries_pcs(quad_work)[
        ["judge_name", "marking_score", "n_marks"]
    ]
    merged = disc_sum.merge(quad_sum, on="judge_name", suffixes=("_discrete", "_quadratic"))
    merged["rank_discrete"] = merged["marking_score_discrete"].rank(method="min")
    merged["rank_quadratic"] = merged["marking_score_quadratic"].rank(method="min")
    merged["rank_delta"] = merged["rank_discrete"] - merged["rank_quadratic"]
    merged["score_delta"] = (
        merged["marking_score_discrete"] - merged["marking_score_quadratic"]
    )
    n_marks = merged["n_marks_discrete"].combine_first(merged["n_marks_quadratic"])
    out = pd.DataFrame(
        {
            "Judge": merged["judge_name"],
            "PCS marks": n_marks.astype(int),
            "Marking score (discrete)": merged["marking_score_discrete"].round(4),
            "Marking score (quadratic)": merged["marking_score_quadratic"].round(4),
            "Rank (discrete)": merged["rank_discrete"].astype(int),
            "Rank (quadratic)": merged["rank_quadratic"].astype(int),
            "Score delta (discrete − quadratic)": merged["score_delta"].round(4),
            "Rank delta (discrete − quadratic)": merged["rank_delta"].astype(int),
        }
    )

    metrics: dict[str, float] = {}
    if len(merged) >= 2:
        metrics["rank_spearman"] = float(
            merged["rank_discrete"].corr(merged["rank_quadratic"], method="spearman")
        )
        metrics["score_pearson"] = float(
            merged["marking_score_discrete"].corr(
                merged["marking_score_quadratic"], method="pearson"
            )
        )
    metrics["n_judges"] = float(len(merged))
    metrics["mean_abs_rank_delta"] = float(merged["rank_delta"].abs().mean())
    metrics["n_quadratic_series"] = float(len(quadratic_params or {}))
    metrics["n_discrete_bins"] = float(len(discrete_params or {}))
    return out.sort_values("Rank (discrete)").reset_index(drop=True), metrics


def sigma_hat_row_pcs(
    control_score: float,
    disc_id,
    component,
    params: dict,
    *,
    floor_sigma: float = FLOOR_SIGMA,
    fallback_sigma: float = 0.3,
    bin_width: float = PCS_SIGMA_BIN_WIDTH_DISCRETE,
) -> float:
    if pd.isna(disc_id) or component is None or (isinstance(component, float) and pd.isna(component)):
        return max(floor_sigma, fallback_sigma)
    k = sigma_bin_from_control_score(control_score, bin_width=bin_width)
    key = (int(disc_id), str(component), float(k))
    if key not in params:
        for delta in sigma_bin_neighbor_offsets(bin_width=bin_width):
            alt_key = (int(disc_id), str(component), float(k + delta))
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
            "control_bin": float(k),
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
    bin_width: float = PCS_SIGMA_BIN_WIDTH_DISCRETE,
) -> pd.DataFrame:
    fallback = max(floor_sigma, float(df["error"].std(ddof=1) or 0.3))
    lookup = _params_to_lookup_df(params)
    work = df.copy()
    if "error" not in work.columns:
        work = compute_errors(work, bin_width=bin_width)
    elif "control_bin" not in work.columns:
        work["control_bin"] = work["control_score"].map(
            lambda c: sigma_bin_from_control_score(c, bin_width=bin_width)
        )
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
        for delta in sigma_bin_neighbor_offsets(bin_width=bin_width):
            if not miss_mask.any():
                break
            neighbor = lookup.copy()
            neighbor["control_bin"] = neighbor["control_bin"] + float(delta)
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
    df: pd.DataFrame,
    *,
    min_bin_count: int = MIN_BIN_COUNT,
    sigma_model: str = PCS_SIGMA_MODEL_DISCRETE,
    floor_sigma: float = FLOOR_SIGMA,
) -> dict:
    if df.empty:
        return {}
    bin_width = pcs_sigma_bin_width_for_model(sigma_model)
    work = compute_errors(df.copy(), bin_width=bin_width)
    if normalize_sigma_model(sigma_model) == PCS_SIGMA_MODEL_QUADRATIC:
        return fit_sigma_quadratic_pcs(
            work,
            min_bin_count=min_bin_count,
            floor_sigma=floor_sigma,
            bin_width=bin_width,
        )
    return fit_sigma_discrete_pcs(work, min_bin_count=min_bin_count, bin_width=bin_width)


def annotate_normalized_marks_for_sigma_model(
    df: pd.DataFrame,
    params: dict,
    *,
    sigma_model: str = PCS_SIGMA_MODEL_DISCRETE,
    floor_sigma: float = FLOOR_SIGMA,
) -> pd.DataFrame:
    if normalize_sigma_model(sigma_model) == PCS_SIGMA_MODEL_QUADRATIC:
        return annotate_normalized_marks_pcs_quadratic(
            df, params, floor_sigma=floor_sigma
        )
    bin_width = pcs_sigma_bin_width_for_model(sigma_model)
    work = df
    if "error" not in df.columns:
        work = compute_errors(df.copy(), bin_width=bin_width)
    elif "control_bin" not in df.columns:
        work = df.copy()
        work["control_bin"] = work["control_score"].map(
            lambda c: sigma_bin_from_control_score(c, bin_width=bin_width)
        )
    return annotate_normalized_marks_pcs(
        work, params, floor_sigma=floor_sigma, bin_width=bin_width
    )


def count_sigma_model_params(params: dict, sigma_model: str) -> int:
    if not params:
        return 0
    return len(params)


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
    *,
    bin_width: float = PCS_SIGMA_BIN_WIDTH_DISCRETE,
) -> pd.DataFrame:
    w = work.copy()
    if "control_bin" not in w.columns:
        w["control_bin"] = w["control_score"].map(
            lambda c: sigma_bin_from_control_score(c, bin_width=bin_width)
        )
    w["discipline"] = w["discipline_type_id"].map(discipline_id_to_name)
    w["Control median range"] = w["control_bin"].map(
        lambda b: sigma_bin_label(b, bin_width=bin_width)
    )
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
    bin_width: float = PCS_SIGMA_BIN_WIDTH_DISCRETE,
) -> pd.DataFrame:
    grouped = df.dropna(subset=["discipline_type_id", "component", "control_bin"])
    if grouped.empty:
        return pd.DataFrame()
    stats = aggregate_pcs_sigma_bin_stats(
        grouped, min_bin_count=0, bin_width=bin_width
    )
    rows = []
    for row in stats.itertuples(index=False):
        disc_id = int(row.discipline_type_id)
        component = str(row.component)
        control_bin = float(row.control_bin)
        n = int(row.count)
        sd = float(row.sigma_empirical)
        var = float(row.variance_empirical)
        key = (disc_id, component, control_bin)
        in_model = key in params and n >= min_bin_count
        rows.append(
            {
                "Discipline": discipline_id_to_name.get(disc_id, str(disc_id)),
                "Component": component,
                "Control median range": sigma_bin_label(control_bin, bin_width=bin_width),
                "Marks in bin": n,
                "Error variance (sample, ddof=1)": round(var, 6) if np.isfinite(var) else None,
                "Error stdev (sample, ddof=1)": round(sd, 4) if np.isfinite(sd) else None,
                "σ̂ used in model": round(params[key], 4) if in_model else None,
                "In model": in_model,
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values(
        ["Discipline", "Component", "Control median range"], ascending=[True, True, True]
    ).reset_index(drop=True)


def build_quadratic_sigma_fits_dataframe_pcs(
    params: dict[tuple[int, str], dict[str, float]],
    discipline_id_to_name: dict[int, str],
) -> pd.DataFrame:
    if not params:
        return pd.DataFrame()
    rows = []
    for (disc_id, component), p in sorted(params.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        row = {
            "Discipline": discipline_id_to_name.get(int(disc_id), str(disc_id)),
            "Component": str(component),
            "Bins fitted": int(p.get("n_bins", 0)),
            "Marks in fit": int(p.get("n_marks", 0)),
            "Variance fit equation": quadratic_sigma_equation_str(p),
            "RMSE (variance fit, σ)": round(float(p.get("rmse_sigma", np.nan)), 4)
            if np.isfinite(p.get("rmse_sigma", np.nan))
            else None,
        }
        if all(k in p for k in ("direct_sigma_a", "direct_sigma_b", "direct_sigma_c0")):
            row["Direct σ equation (plot bins)"] = direct_sigma_quadratic_equation_str(p)
            row["RMSE (direct σ, plot bins)"] = (
                round(float(p.get("rmse_direct_sigma", np.nan)), 4)
                if np.isfinite(p.get("rmse_direct_sigma", np.nan))
                else None
            )
            row["Plot bins"] = int(p.get("n_plot_bins", 0))
        if p.get("n_competitions_plot") is not None:
            row["Competitions (plot overlay)"] = int(p.get("n_competitions_plot", 0))
        rows.append(row)
    return pd.DataFrame(rows)


def build_quadratic_sigma_bins_dataframe_pcs(
    df: pd.DataFrame,
    params: dict[tuple[int, str], dict[str, float]],
    discipline_id_to_name: dict[int, str],
    *,
    min_bin_count: int = MIN_BIN_COUNT,
    floor_sigma: float = FLOOR_SIGMA,
    bin_width: float = PCS_SIGMA_BIN_WIDTH_QUADRATIC,
) -> pd.DataFrame:
    """Empirical bin spread plus σ̂ from the quadratic variance fit at each bin mean."""
    grouped = df.dropna(subset=["discipline_type_id", "component", "control_bin"])
    if grouped.empty:
        return pd.DataFrame()
    stats = aggregate_pcs_sigma_bin_stats(
        grouped, min_bin_count=0, bin_width=bin_width
    )
    rows = []
    for row in stats.itertuples(index=False):
        disc_id = int(row.discipline_type_id)
        component = str(row.component)
        control_bin = float(row.control_bin)
        n = int(row.count)
        var = float(row.variance_empirical)
        sd = float(row.sigma_empirical)
        c_mean = float(row.control_score_mean)
        series_key = (disc_id, component)
        fitted = None
        in_model = n >= min_bin_count and series_key in params
        if series_key in params:
            p = params[series_key]
            fitted = float(
                quadratic_sigma_from_variance_eval(
                    c_mean,
                    p["a"],
                    p["b"],
                    p["c"],
                    floor_sigma=floor_sigma,
                )
            )
        rows.append(
            {
                "Discipline": discipline_id_to_name.get(disc_id, str(disc_id)),
                "Component": component,
                "Control median range": sigma_bin_label(control_bin, bin_width=bin_width),
                "Mean control score": round(c_mean, 3),
                "Marks in bin": n,
                "Error variance (sample, ddof=1)": round(var, 6) if np.isfinite(var) else None,
                "Error stdev (sample, ddof=1)": round(sd, 4) if np.isfinite(sd) else None,
                "σ̂ fitted (quadratic)": round(fitted, 4) if fitted is not None else None,
                "In model": in_model,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["Discipline", "Component", "Control median range"], ascending=[True, True, True]
    )


def build_sigma_display_dataframe_pcs(
    df: pd.DataFrame,
    params: dict,
    discipline_id_to_name: dict[int, str],
    *,
    min_bin_count: int = MIN_BIN_COUNT,
    sigma_model: str = PCS_SIGMA_MODEL_DISCRETE,
    floor_sigma: float = FLOOR_SIGMA,
) -> pd.DataFrame:
    bin_width = pcs_sigma_bin_width_for_model(sigma_model)
    work = df
    if not work.empty:
        work = compute_errors(work.copy(), bin_width=bin_width)
    if normalize_sigma_model(sigma_model) == PCS_SIGMA_MODEL_QUADRATIC:
        return build_quadratic_sigma_bins_dataframe_pcs(
            work,
            params,
            discipline_id_to_name,
            min_bin_count=min_bin_count,
            floor_sigma=floor_sigma,
            bin_width=bin_width,
        )
    return build_sigma_bins_dataframe_pcs(
        work,
        params,
        discipline_id_to_name,
        min_bin_count=min_bin_count,
        bin_width=bin_width,
    )


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
    if len(run_params) >= 15:
        return run_params[:15]
    if len(run_params) >= 14:
        return (*run_params[:14], PCS_SIGMA_MODEL_DISCRETE)
    if len(run_params) >= 9:
        base = run_params[:9]
        return (*base, None, None, None, None, None, PCS_SIGMA_MODEL_DISCRETE)
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
    return (rp[0], rp[1], rp[2], rp[3], rp[4], rp[5], rp[7], rp[8], rp[12], rp[14])


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
        rp[14],
        pcs_sigma_bin_width_for_model(rp[14]),
    )


def run_params_same_sigma_and_ranking_scope(a: tuple, b: tuple) -> bool:
    ra = unpack_pcs_deviation_run_params(a)
    rb = unpack_pcs_deviation_run_params(b)
    rank_a = (ra[0], ra[1], ra[2], ra[3], ra[4], ra[5], ra[7], ra[8], ra[12], ra[14])
    rank_b = (rb[0], rb[1], rb[2], rb[3], rb[4], rb[5], rb[7], rb[8], rb[12], rb[14])
    bench_a = (
        ra[9],
        ra[10],
        ra[2],
        ra[11] or COMPETITION_SCOPE_ALL,
        ra[7],
        ra[8],
        ra[13] if ra[13] is not None else ra[12],
        ra[14],
    )
    bench_b = (
        rb[9],
        rb[10],
        rb[2],
        rb[11] or COMPETITION_SCOPE_ALL,
        rb[7],
        rb[8],
        rb[13] if rb[13] is not None else rb[12],
        rb[14],
    )
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
    sigma_model: str = PCS_SIGMA_MODEL_DISCRETE,
) -> dict[str, Any]:
    disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
    sigma_model = normalize_sigma_model(sigma_model)

    if df.empty:
        return {
            "marking": pd.DataFrame(),
            "summary": pd.DataFrame(),
            "sigma_bins": pd.DataFrame(),
            "sigma_fits": pd.DataFrame(),
            "judge_discipline_detail": pd.DataFrame(),
            "judge_component_detail": pd.DataFrame(),
            "judge_control_bin_detail": pd.DataFrame(),
            "params": {},
            "sigma_model": sigma_model,
            "n_raw_marks": 0,
            "n_sigma_buckets": 0,
            "error": "No PCS score rows found for the selected filters.",
        }

    n_raw = len(df)
    if "judge_id" in df.columns:
        df = attach_judge_identities(df, analytics)
    bin_width = pcs_sigma_bin_width_for_model(sigma_model)
    df = compute_errors(df, bin_width=bin_width)
    if params is None:
        params = fit_sigma_params_from_marks(
            df,
            min_bin_count=min_bin_count,
            sigma_model=sigma_model,
            floor_sigma=floor_sigma,
        )
    work = annotate_normalized_marks_for_sigma_model(
        df, params, sigma_model=sigma_model, floor_sigma=floor_sigma
    )
    judge_summary_all = compute_judge_summaries_pcs(work)
    judge_summary = apply_min_marks_filter(judge_summary_all.copy(), min_marks)
    marking = build_ranking_display_table_pcs(judge_summary)
    summary = marking_score_summary_pcs(judge_summary)

    bins_df = sigma_reference_df if sigma_reference_df is not None else df
    if not bins_df.empty:
        bins_df = compute_errors(bins_df.copy(), bin_width=bin_width)
    sigma_bins = build_sigma_display_dataframe_pcs(
        bins_df,
        params,
        disc_map,
        min_bin_count=min_bin_count,
        sigma_model=sigma_model,
        floor_sigma=floor_sigma,
    )
    sigma_fits = (
        build_quadratic_sigma_fits_dataframe_pcs(params, disc_map)
        if sigma_model == PCS_SIGMA_MODEL_QUADRATIC
        else pd.DataFrame()
    )

    judge_discipline_detail_all = compute_judge_discipline_breakdown_pcs(work, disc_map)
    judge_component_detail_all = compute_judge_component_breakdown_pcs(work, disc_map)
    judge_control_bin_detail_all = compute_judge_control_bin_breakdown_pcs(
        work, disc_map, bin_width=bin_width
    )
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
        "sigma_fits": sigma_fits,
        "judge_summary_all": judge_summary_all,
        "judge_discipline_detail": judge_discipline_detail,
        "judge_component_detail": judge_component_detail,
        "judge_control_bin_detail": judge_control_bin_detail,
        "judge_discipline_detail_all": judge_discipline_detail_all,
        "judge_component_detail_all": judge_component_detail_all,
        "judge_control_bin_detail_all": judge_control_bin_detail_all,
        "params": params,
        "sigma_model": sigma_model,
        "n_raw_marks": n_raw,
        "n_sigma_buckets": count_sigma_model_params(params, sigma_model),
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
    sigma_model: str = PCS_SIGMA_MODEL_DISCRETE,
    cache_only: bool = False,
    persist_shards: bool = True,
) -> dict[str, Any]:
    from pcs_deviation_cache import run_pcs_deviation_ranking_pipeline

    scope_err = validate_pcs_deviation_scope(start_season_year, end_season_year)
    if scope_err:
        empty = finish_pcs_deviation_rankings_from_marks(
            analytics, pd.DataFrame(), min_marks=min_marks, sigma_model=sigma_model
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
        sigma_model=sigma_model,
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
        sigma_model=sigma_model_from_run_params(run_params),
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
    sigma_model: str = PCS_SIGMA_MODEL_DISCRETE,
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
    bin_width = pcs_sigma_bin_width_for_model(sigma_model)
    df = compute_errors(df, bin_width=bin_width)
    work = annotate_normalized_marks_for_sigma_model(
        df, params, sigma_model=sigma_model, floor_sigma=floor_sigma
    )
    return (
        compute_judge_discipline_breakdown_pcs(work, disc_map),
        compute_judge_component_breakdown_pcs(work, disc_map),
        compute_judge_control_bin_breakdown_pcs(work, disc_map, bin_width=bin_width),
    )


__all__ = [
    "ELEMENT_RANKING_LEVEL_FILTER_ALL",
    "ELEMENT_RANKING_LEVEL_FILTER_LABELS",
    "ELEMENT_RANKING_LEVEL_FILTER_PRESETS",
    "FLOOR_SIGMA",
    "MIN_BIN_COUNT",
    "MIN_BIN_COUNT_DISCRETE",
    "MIN_BIN_COUNT_QUADRATIC",
    "MIN_BINS_FOR_QUADRATIC_FIT",
    "MIN_PCS_DEVIATION_EVENT_DATE",
    "MIN_PCS_DEVIATION_SEASON_YEAR",
    "PCS_DEVIATION_COMPETITION_SCOPE_LABELS",
    "PCS_DEVIATION_DISCIPLINE_IDS",
    "PCS_SIGMA_MODEL_DISCRETE",
    "PCS_SIGMA_MODEL_LABELS",
    "PCS_SIGMA_MODEL_QUADRATIC",
    "PCS_SIGMA_PER_COMPETITION_MIN_MARKS",
    "PCS_SIGMA_BIN_WIDTH",
    "PCS_SIGMA_BIN_WIDTH_DISCRETE",
    "PCS_SIGMA_BIN_WIDTH_QUADRATIC",
    "PCS_SIGMA_PLOT_BIN_WIDTH",
    "PCS_SIGMA_PLOT_MIN_BIN_COUNT",
    "SIGMA_SAMPLE_DDOF",
    "aggregate_pcs_sigma_bin_stats",
    "aggregate_pcs_sigma_fine_bin_stats",
    "annotate_normalized_marks_for_sigma_model",
    "annotate_normalized_marks_pcs_quadratic",
    "apply_min_marks_to_pcs_deviation_result",
    "attach_direct_sigma_plot_metadata",
    "build_pcs_heteroscedasticity_figure",
    "build_quadratic_sigma_bins_dataframe_pcs",
    "build_quadratic_sigma_fits_dataframe_pcs",
    "build_sigma_display_dataframe_pcs",
    "collect_sigma_bin_stats_pcs",
    "collect_sigma_per_competition_stats_pcs",
    "collect_sigma_plot_stats_pcs",
    "compare_pcs_sigma_model_rankings",
    "compute_judge_detail_for_identity_pcs",
    "compute_pcs_deviation_rankings",
    "compute_pcs_deviation_rankings_from_run_params",
    "control_bin_center",
    "control_bin_from_median",
    "direct_sigma_quadratic_equation_str",
    "direct_sigma_quadratic_eval",
    "filter_pcs_deviation_season_years",
    "sigma_bin_from_control_score",
    "sigma_bin_label",
    "sigma_bin_neighbor_offsets",
    "fit_direct_sigma_quadratic_from_stats",
    "fit_sigma_quadratic_pcs",
    "heteroscedasticity_annotation_text",
    "min_bin_count_for_sigma_model",
    "normalize_sigma_model",
    "pcs_sigma_bin_width_for_model",
    "pcs_deviation_competition_scope_key",
    "pcs_deviation_discipline_names_for_scope",
    "pcs_deviation_discipline_types",
    "quadratic_sigma_equation_str",
    "quadratic_sigma_eval",
    "quadratic_sigma_from_variance_eval",
    "quadratic_variance_eval",
    "sample_error_stdev",
    "sample_error_variance",
    "sigma_model_from_run_params",
    "run_params_ranking_compute_key",
    "run_params_same_sigma_and_ranking_scope",
    "unpack_pcs_deviation_run_params",
    "uses_separate_benchmark_pool",
    "validate_pcs_deviation_scope",
]
