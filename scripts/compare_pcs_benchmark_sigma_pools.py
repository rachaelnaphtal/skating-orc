#!/usr/bin/env python3
"""
Compare benchmark σ̂ fits across competition scope and segment-level pools.

Reads fitted σ̂ parameters from ``pcs_deviation_ranking_sigma_cache`` (no mark
reload) and writes curve tables, cross-pool spread summaries, overlay plots,
and optional judge marking-score / tier-stability comparisons.

Judge tiers are assigned **within discipline group** (Singles & Pairs, Ice Dance,
Synchronized) so judges are only ranked against peers in the same discipline.

Example::

    python scripts/compare_pcs_benchmark_sigma_pools.py \\
        --sigma-model quadratic \\
        -o analysisTemp/pcs_benchmark_sigma_pools

    python scripts/compare_pcs_benchmark_sigma_pools.py \\
        --scopes international isu_event \\
        --segment-levels junior_senior \\
        --ranking-scope international --ranking-segment-level junior_senior \\
        --min-marks 100 \\
        -o analysisTemp/pcs_benchmark_sigma_pools_intl
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from analytics import JudgeAnalytics  # noqa: E402
from database import get_db_session  # noqa: E402
from element_deviation_ranking import (  # noqa: E402
    ELEMENT_RANKING_LEVEL_FILTER_ALL,
    ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
    ELEMENT_RANKING_LEVEL_FILTER_LABELS,
    ELEMENT_RANKING_LEVEL_FILTER_PRESETS,
    apply_min_marks_filter,
    attach_judge_identities,
)
from models import PcsDeviationRankingSigmaCache  # noqa: E402
from pcs_deviation_analysis import (  # noqa: E402
    FLOOR_SIGMA,
    PCS_DEVIATION_COMPETITION_SCOPES,
    PCS_DEVIATION_COMPETITION_SCOPE_LABELS,
    PCS_SIGMA_BIN_WIDTH_DISCRETE,
    PCS_SIGMA_BIN_WIDTH_QUADRATIC,
    PCS_SIGMA_MODEL_DISCRETE,
    PCS_SIGMA_MODEL_QUADRATIC,
    annotate_normalized_marks_for_sigma_model,
    compute_errors,
    compute_judge_summaries_pcs,
    normalize_sigma_model,
    pcs_sigma_bin_width_for_model,
    quadratic_sigma_from_variance_eval,
    sigma_bin_from_control_score,
    sigma_bin_neighbor_offsets,
)
from pcs_deviation_cache import collect_marks_for_run  # noqa: E402


@dataclass(frozen=True)
class BenchmarkSigmaPool:
    competition_scope: str
    segment_level_preset: str | None
    sigma_model: str
    floor_sigma: float
    min_bin_count: int
    n_marks: int
    sigma_key: str
    params: dict


def _slug(s: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in s).strip("_")


def _scope_label(scope_key: str) -> str:
    try:
        idx = PCS_DEVIATION_COMPETITION_SCOPES.index(scope_key)
        return PCS_DEVIATION_COMPETITION_SCOPE_LABELS[idx]
    except ValueError:
        return scope_key


def _level_label(preset: str | None) -> str:
    key = preset or ELEMENT_RANKING_LEVEL_FILTER_ALL
    return ELEMENT_RANKING_LEVEL_FILTER_LABELS.get(key, key)


def _pool_label(scope_key: str, level_preset: str | None) -> str:
    return f"{_scope_label(scope_key)} / {_level_label(level_preset)}"


def parse_benchmark_scope_json(payload: list) -> dict:
    """Decode ``scope_json`` from ``PcsDeviationRankingSigmaCache``."""
    if len(payload) == 7:
        bs, be, disc, scope_key, floor, minbin, levels = payload
        model = PCS_SIGMA_MODEL_DISCRETE
        bin_width = PCS_SIGMA_BIN_WIDTH_DISCRETE
    elif len(payload) == 8:
        bs, be, disc, scope_key, floor, minbin, levels, model = payload
        model = normalize_sigma_model(model)
        bin_width = pcs_sigma_bin_width_for_model(model)
    elif len(payload) >= 9:
        bs, be, disc, scope_key, floor, minbin, levels, model, bin_width = payload[:9]
        model = normalize_sigma_model(model)
    else:
        raise ValueError(f"Unexpected benchmark scope_json length: {len(payload)}")
    return {
        "benchmark_start_season_year": bs,
        "benchmark_end_season_year": be,
        "discipline_type_ids": disc,
        "competition_scope": str(scope_key),
        "floor_sigma": float(floor),
        "min_bin_count": int(minbin),
        "segment_level_preset": levels,
        "sigma_model": model,
        "bin_width": float(bin_width),
    }


def load_cached_benchmark_sigma_pools(
    session,
    *,
    sigma_model: str,
    scopes: list[str] | None = None,
    segment_levels: list[str] | None = None,
) -> list[BenchmarkSigmaPool]:
    """Load one pool per (scope, segment level) from the σ̂ cache."""
    sigma_model = normalize_sigma_model(sigma_model)
    scope_set = set(scopes) if scopes else None
    level_set = set(segment_levels) if segment_levels else None

    rows = session.query(PcsDeviationRankingSigmaCache).all()
    best: dict[tuple[str, str | None], BenchmarkSigmaPool] = {}
    for row in rows:
        meta = parse_benchmark_scope_json(json.loads(row.scope_json))
        if meta["sigma_model"] != sigma_model:
            continue
        if (
            meta["benchmark_start_season_year"] is not None
            or meta["benchmark_end_season_year"] is not None
            or meta["discipline_type_ids"] is not None
        ):
            continue
        scope_key = meta["competition_scope"]
        level_key = meta["segment_level_preset"]
        if scope_set is not None and scope_key not in scope_set:
            continue
        if level_set is not None:
            normalized = level_key or ELEMENT_RANKING_LEVEL_FILTER_ALL
            if normalized not in level_set and level_key not in level_set:
                continue
        try:
            params = pickle.loads(row.params_payload)
        except Exception:
            continue
        if not isinstance(params, dict) or not params:
            continue

        combo = (scope_key, level_key)
        candidate = BenchmarkSigmaPool(
            competition_scope=scope_key,
            segment_level_preset=level_key,
            sigma_model=sigma_model,
            floor_sigma=meta["floor_sigma"],
            min_bin_count=meta["min_bin_count"],
            n_marks=int(row.n_marks or 0),
            sigma_key=row.sigma_key,
            params=params,
        )
        existing = best.get(combo)
        if existing is None or candidate.n_marks > existing.n_marks:
            best[combo] = candidate
    return sorted(
        best.values(),
        key=lambda p: (
            PCS_DEVIATION_COMPETITION_SCOPES.index(p.competition_scope)
            if p.competition_scope in PCS_DEVIATION_COMPETITION_SCOPES
            else 999,
            _level_label(p.segment_level_preset),
        ),
    )


def _discrete_sigma_lookup(
    params: dict,
    *,
    discipline_type_id: int,
    component: str,
    control_score: float,
    bin_width: float,
    floor_sigma: float,
) -> float | None:
    control_bin = sigma_bin_from_control_score(control_score, bin_width=bin_width)
    for delta in (0.0, *sigma_bin_neighbor_offsets(bin_width=bin_width)):
        key = (int(discipline_type_id), str(component), float(control_bin + delta))
        if key in params:
            return max(float(params[key]), floor_sigma)
    return None


def evaluate_sigma_at_control_scores(
    params: dict,
    *,
    discipline_type_id: int,
    component: str,
    control_scores: np.ndarray,
    sigma_model: str,
    floor_sigma: float,
) -> np.ndarray:
    sigma_model = normalize_sigma_model(sigma_model)
    if sigma_model == PCS_SIGMA_MODEL_QUADRATIC:
        key = (int(discipline_type_id), str(component))
        if key not in params:
            return np.full(len(control_scores), np.nan)
        p = params[key]
        return quadratic_sigma_from_variance_eval(
            control_scores,
            float(p["a"]),
            float(p["b"]),
            float(p["c"]),
            floor_sigma=floor_sigma,
        )

    bin_width = pcs_sigma_bin_width_for_model(sigma_model)
    out = np.full(len(control_scores), np.nan, dtype=float)
    for i, c in enumerate(control_scores):
        val = _discrete_sigma_lookup(
            params,
            discipline_type_id=discipline_type_id,
            component=component,
            control_score=float(c),
            bin_width=bin_width,
            floor_sigma=floor_sigma,
        )
        if val is not None:
            out[i] = val
    return out


def build_sigma_curves_dataframe(
    pools: list[BenchmarkSigmaPool],
    *,
    discipline_type_ids: list[int] | None,
    discipline_id_to_name: dict[int, str],
    control_scores: np.ndarray,
) -> pd.DataFrame:
    series_keys: set[tuple[int, str]] = set()
    for pool in pools:
        for key in pool.params:
            if len(key) == 2:
                series_keys.add((int(key[0]), str(key[1])))
            elif len(key) == 3:
                series_keys.add((int(key[0]), str(key[1])))

    if discipline_type_ids:
        allowed = {int(d) for d in discipline_type_ids}
        series_keys = {k for k in series_keys if k[0] in allowed}

    rows: list[dict] = []
    for disc_id, component in sorted(series_keys):
        for pool in pools:
            sigma = evaluate_sigma_at_control_scores(
                pool.params,
                discipline_type_id=disc_id,
                component=component,
                control_scores=control_scores,
                sigma_model=pool.sigma_model,
                floor_sigma=pool.floor_sigma,
            )
            for c, s in zip(control_scores, sigma, strict=True):
                if not np.isfinite(s):
                    continue
                rows.append(
                    {
                        "competition_scope": pool.competition_scope,
                        "competition_scope_label": _scope_label(pool.competition_scope),
                        "segment_level_preset": pool.segment_level_preset
                        or ELEMENT_RANKING_LEVEL_FILTER_ALL,
                        "segment_level_label": _level_label(pool.segment_level_preset),
                        "pool_label": _pool_label(
                            pool.competition_scope, pool.segment_level_preset
                        ),
                        "sigma_key": pool.sigma_key,
                        "n_marks": pool.n_marks,
                        "discipline_type_id": disc_id,
                        "discipline": discipline_id_to_name.get(disc_id, str(disc_id)),
                        "component": component,
                        "control_score": float(c),
                        "sigma": float(s),
                    }
                )
    return pd.DataFrame(rows)


def build_sigma_spread_dataframe(
    curves: pd.DataFrame,
    *,
    reference_scope: str,
    reference_level: str | None,
) -> pd.DataFrame:
    if curves.empty:
        return pd.DataFrame()

    ref_level = reference_level or ELEMENT_RANKING_LEVEL_FILTER_ALL
    ref = curves.loc[
        (curves["competition_scope"] == reference_scope)
        & (curves["segment_level_preset"] == ref_level)
    ][
        [
            "discipline_type_id",
            "discipline",
            "component",
            "control_score",
            "sigma",
        ]
    ].rename(columns={"sigma": "sigma_reference"})

    rows: list[dict] = []
    group_cols = ["discipline_type_id", "discipline", "component", "control_score"]
    for key, grp in curves.groupby(group_cols, sort=False):
        disc_id, disc_name, component, control_score = key
        sigmas = grp["sigma"].astype(float)
        ref_row = ref.loc[
            (ref["discipline_type_id"] == disc_id)
            & (ref["component"] == component)
            & (ref["control_score"] == control_score)
        ]
        sigma_ref = (
            float(ref_row["sigma_reference"].iloc[0]) if not ref_row.empty else np.nan
        )
        rows.append(
            {
                "discipline_type_id": int(disc_id),
                "discipline": disc_name,
                "component": component,
                "control_score": float(control_score),
                "n_pools": int(len(sigmas)),
                "sigma_min": float(sigmas.min()),
                "sigma_max": float(sigmas.max()),
                "sigma_mean": float(sigmas.mean()),
                "sigma_std": float(sigmas.std(ddof=0)) if len(sigmas) > 1 else 0.0,
                "sigma_range": float(sigmas.max() - sigmas.min()),
                "sigma_cv_pct": float(100.0 * sigmas.std(ddof=0) / sigmas.mean())
                if len(sigmas) > 1 and sigmas.mean() > 0
                else 0.0,
                "sigma_reference": sigma_ref,
                "sigma_delta_vs_reference": float(sigmas.mean() - sigma_ref)
                if np.isfinite(sigma_ref)
                else np.nan,
                "sigma_max_abs_delta_vs_reference": float(
                    (grp["sigma"].astype(float) - sigma_ref).abs().max()
                )
                if np.isfinite(sigma_ref)
                else np.nan,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["discipline", "component", "control_score"], ascending=[True, True, True]
    )


_LEVEL_SHORT = {
    ELEMENT_RANKING_LEVEL_FILTER_ALL: "all levels",
    "novice_junior_senior": "N/J/S",
    "junior_senior": "J/S",
}


def _pool_legend_label(scope_label: str, level_preset: str) -> str:
    short = _LEVEL_SHORT.get(level_preset, level_preset)
    return f"{scope_label} ({short})"


def _pool_column_id(scope_key: str, level_preset: str | None) -> str:
    level = level_preset or ELEMENT_RANKING_LEVEL_FILTER_ALL
    return f"{scope_key}__{level}"


def _segment_level_preset_arg(preset: str) -> str | None:
    if preset == ELEMENT_RANKING_LEVEL_FILTER_ALL:
        return None
    return preset


MARKING_TIER_TOP = "top"
MARKING_TIER_MIDDLE_HIGH = "middle_high"
MARKING_TIER_MIDDLE_LOW = "middle_low"
MARKING_TIER_BOTTOM = "bottom"

DEFAULT_RANKING_SCENARIOS: tuple[tuple[str, str], ...] = (
    ("all", ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR),
    ("international", ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR),
)


@dataclass(frozen=True)
class JudgeDisciplineGroup:
    key: str
    label: str
    discipline_type_ids: tuple[int, ...]


JUDGE_DISCIPLINE_GROUPS: tuple[JudgeDisciplineGroup, ...] = (
    JudgeDisciplineGroup("singles_pairs", "Singles & Pairs", (1, 2)),
    JudgeDisciplineGroup("dance", "Ice Dance", (3,)),
    JudgeDisciplineGroup("synchro", "Synchronized", (5,)),
)


@dataclass(frozen=True)
class RankingScenario:
    competition_scope: str
    segment_level: str
    us_directory_only: bool = False

    @property
    def slug(self) -> str:
        parts = [_slug(self.competition_scope), _slug(self.segment_level)]
        if self.us_directory_only:
            parts.append("us_directory")
        return "_".join(parts)

    @property
    def label(self) -> str:
        base = (
            f"{_scope_label(self.competition_scope)} / "
            f"{_level_label(self.segment_level)}"
        )
        if self.us_directory_only:
            return f"{base} (US directory)"
        return base

    @property
    def tier_reference_pool_id(self) -> str:
        return _pool_column_id(self.competition_scope, self.segment_level)


def parse_ranking_scenario(spec: str) -> tuple[str, str]:
    if "/" not in spec:
        raise ValueError(
            f"Ranking scenario must be scope/level, got {spec!r} "
            "(e.g. all/junior_senior or international/junior_senior)."
        )
    scope, level = spec.split("/", 1)
    if scope not in PCS_DEVIATION_COMPETITION_SCOPES:
        raise ValueError(f"Unknown competition scope {scope!r}.")
    if level not in ELEMENT_RANKING_LEVEL_FILTER_PRESETS:
        raise ValueError(f"Unknown segment level preset {level!r}.")
    return scope, level


def ranking_scenarios_from_args(
    *,
    ranking_scenarios: list[str] | None,
    ranking_scope: str | None,
    ranking_segment_level: str | None,
    us_directory_only: bool,
) -> list[RankingScenario]:
    specs: list[tuple[str, str]] = []
    if ranking_scenarios:
        specs = [parse_ranking_scenario(s) for s in ranking_scenarios]
    elif ranking_scope is not None or ranking_segment_level is not None:
        specs = [
            (
                ranking_scope or "international",
                ranking_segment_level or ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
            )
        ]
    else:
        specs = list(DEFAULT_RANKING_SCENARIOS)

    scenarios = [
        RankingScenario(scope, level, us_directory_only=False)
        for scope, level in specs
    ]
    if us_directory_only:
        scenarios.extend(
            RankingScenario(scope, level, us_directory_only=True)
            for scope, level in specs
        )
    return scenarios


def filter_marks_for_discipline_group(
    marks: pd.DataFrame, group: JudgeDisciplineGroup
) -> pd.DataFrame:
    if marks.empty or "discipline_type_id" not in marks.columns:
        return marks.iloc[0:0].copy()
    allowed = {int(d) for d in group.discipline_type_ids}
    return marks.loc[marks["discipline_type_id"].isin(allowed)].copy()


def assign_marking_score_tiers(
    marking_scores: pd.Series,
    *,
    top_fraction: float = 0.30,
    bottom_fraction: float = 0.10,
) -> pd.Series:
    """
    Assign judges to four quality tiers (lower marking score = better).

    - ``top``: best ``top_fraction`` of judges (default 30%)
    - ``middle_high`` / ``middle_low``: remaining middle split by rank
    - ``bottom``: worst ``bottom_fraction`` of judges (default 10%)
    """
    scores = marking_scores.dropna().astype(float)
    tiers = pd.Series(MARKING_TIER_MIDDLE_LOW, index=marking_scores.index, dtype="object")
    if scores.empty:
        return tiers
    if len(scores) == 1:
        tiers.loc[scores.index] = MARKING_TIER_TOP
        return tiers

    ordered = scores.sort_values()
    n = len(ordered)
    n_top = max(1, int(np.ceil(n * float(top_fraction))))
    n_bottom = max(1, int(np.ceil(n * float(bottom_fraction))))
    if n_top + n_bottom >= n:
        n_bottom = max(0, min(n_bottom, n - 2))
        n_top = min(n_top, n - n_bottom - 1) if n - n_bottom > 1 else 1
    if n_top < 1:
        n_top = 1

    top_idx = ordered.index[:n_top]
    bottom_idx = ordered.index[-n_bottom:] if n_bottom > 0 else ordered.index[:0]
    middle_idx = ordered.index[n_top : n - n_bottom] if n_bottom > 0 else ordered.index[n_top:]

    tiers.loc[top_idx] = MARKING_TIER_TOP
    tiers.loc[bottom_idx] = MARKING_TIER_BOTTOM

    if len(middle_idx) > 0:
        mid_ordered = ordered.loc[middle_idx]
        n_mid_high = (len(mid_ordered) + 1) // 2
        tiers.loc[mid_ordered.index[:n_mid_high]] = MARKING_TIER_MIDDLE_HIGH
        tiers.loc[mid_ordered.index[n_mid_high:]] = MARKING_TIER_MIDDLE_LOW
    return tiers


def compute_judge_scores_across_pools(
    marks: pd.DataFrame,
    pools: list[BenchmarkSigmaPool],
    *,
    sigma_model: str,
    min_marks: int,
    tier_top_fraction: float = 0.30,
    tier_bottom_fraction: float = 0.10,
    us_directory_labels: frozenset[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
  Re-score the same mark pool under each benchmark σ̂ pool.

    Returns wide score table and long tier table.
    """
    if marks.empty:
        return pd.DataFrame(), pd.DataFrame()

    bin_width = pcs_sigma_bin_width_for_model(sigma_model)
    work = compute_errors(marks.copy(), bin_width=bin_width)
    if us_directory_labels is not None:
        work = work.loc[work["judge_name"].isin(us_directory_labels)].copy()
        if work.empty:
            return pd.DataFrame(), pd.DataFrame()

    score_parts: list[pd.DataFrame] = []
    tier_rows: list[dict] = []

    for pool in pools:
        pool_id = _pool_column_id(pool.competition_scope, pool.segment_level_preset)
        annotated = annotate_normalized_marks_for_sigma_model(
            work,
            pool.params,
            sigma_model=sigma_model,
            floor_sigma=pool.floor_sigma,
        )
        summary = compute_judge_summaries_pcs(annotated)
        summary = apply_min_marks_filter(summary, min_marks)
        if summary.empty:
            continue

        summary = summary.rename(
            columns={
                "marking_score": f"marking_score__{pool_id}",
                "rank": f"rank__{pool_id}",
                "n_marks": "n_marks",
            }
        )
        keep_cols = ["judge_name", "n_marks", f"marking_score__{pool_id}", f"rank__{pool_id}"]
        score_parts.append(summary[keep_cols])

        score_col = f"marking_score__{pool_id}"
        rank_col = f"rank__{pool_id}"
        tiers = assign_marking_score_tiers(
            summary.set_index("judge_name")[score_col],
            top_fraction=tier_top_fraction,
            bottom_fraction=tier_bottom_fraction,
        )
        for judge_name, tier in tiers.items():
            row = summary.loc[summary["judge_name"] == judge_name].iloc[0]
            tier_rows.append(
                {
                    "judge_name": judge_name,
                    "pool_id": pool_id,
                    "pool_label": _pool_label(
                        pool.competition_scope, pool.segment_level_preset
                    ),
                    "competition_scope": pool.competition_scope,
                    "segment_level_preset": pool.segment_level_preset
                    or ELEMENT_RANKING_LEVEL_FILTER_ALL,
                    "marking_score": float(row[score_col]),
                    "rank": int(row[rank_col]),
                    "n_marks": int(row["n_marks"]),
                    "tier": str(tier),
                }
            )

    if not score_parts:
        return pd.DataFrame(), pd.DataFrame()

    wide = score_parts[0]
    for part in score_parts[1:]:
        drop_cols = [c for c in part.columns if c == "n_marks"]
        wide = wide.merge(part.drop(columns=drop_cols), on="judge_name", how="outer")

    tier_df = pd.DataFrame(tier_rows)
    return wide.sort_values("judge_name").reset_index(drop=True), tier_df


def build_judge_tier_stability(
    tier_df: pd.DataFrame,
    *,
    reference_pool_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-pool tier agreement vs reference and transition counts."""
    if tier_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    ref = tier_df.loc[tier_df["pool_id"] == reference_pool_id][
        ["judge_name", "tier", "marking_score"]
    ].rename(
        columns={"tier": "tier_reference", "marking_score": "marking_score_reference"}
    )
    if ref.empty:
        return pd.DataFrame(), pd.DataFrame()

    tier_df = tier_df.merge(ref, on="judge_name", how="inner")
    tier_df["tier_changed"] = tier_df["tier"] != tier_df["tier_reference"]
    ref_ranks = tier_df.loc[tier_df["pool_id"] == reference_pool_id][
        ["judge_name", "rank"]
    ].rename(columns={"rank": "rank_reference"})
    tier_df = tier_df.merge(ref_ranks, on="judge_name", how="left")

    stability_rows: list[dict] = []
    transition_rows: list[dict] = []
    for pool_id, grp in tier_df.groupby("pool_id", sort=False):
        pool_label = str(grp["pool_label"].iloc[0])
        n = len(grp)
        n_changed = int(grp["tier_changed"].sum())
        rank_ref = grp["rank_reference"] if "rank_reference" in grp.columns else grp["rank"]
        if pool_id == reference_pool_id:
            mean_abs_rank_delta = 0.0
        else:
            mean_abs_rank_delta = float((grp["rank"] - rank_ref).abs().mean())
        stability_rows.append(
            {
                "pool_id": pool_id,
                "pool_label": pool_label,
                "n_judges": n,
                "n_tier_changes": n_changed,
                "tier_match_pct": float(100.0 * (n - n_changed) / n) if n else 0.0,
                "mean_abs_score_delta": float(
                    (grp["marking_score"] - grp["marking_score_reference"]).abs().mean()
                ),
                "max_abs_score_delta": float(
                    (grp["marking_score"] - grp["marking_score_reference"]).abs().max()
                ),
                "mean_abs_rank_delta": mean_abs_rank_delta,
            }
        )
        if pool_id == reference_pool_id:
            continue
        for (tier_ref, tier_new), sub in grp.groupby(
            ["tier_reference", "tier"], sort=False
        ):
            transition_rows.append(
                {
                    "pool_id": pool_id,
                    "pool_label": pool_label,
                    "tier_reference": tier_ref,
                    "tier_new": tier_new,
                    "n_judges": int(len(sub)),
                }
            )

    stability = pd.DataFrame(stability_rows)
    transitions = pd.DataFrame(transition_rows)
    return stability, transitions


def build_judge_score_delta_table(
    wide_scores: pd.DataFrame,
    pools: list[BenchmarkSigmaPool],
    *,
    reference_pool_id: str,
) -> pd.DataFrame:
    ref_col = f"marking_score__{reference_pool_id}"
    ref_rank_col = f"rank__{reference_pool_id}"
    if ref_col not in wide_scores.columns:
        return pd.DataFrame()

    rows: list[dict] = []
    for pool in pools:
        pool_id = _pool_column_id(pool.competition_scope, pool.segment_level_preset)
        score_col = f"marking_score__{pool_id}"
        rank_col = f"rank__{pool_id}"
        if score_col not in wide_scores.columns:
            continue
        for _, row in wide_scores.iterrows():
            score = row.get(score_col)
            score_ref = row.get(ref_col)
            if pd.isna(score) or pd.isna(score_ref):
                continue
            rank = row.get(rank_col)
            rank_ref = row.get(ref_rank_col)
            rows.append(
                {
                    "judge_name": row["judge_name"],
                    "n_marks": int(row["n_marks"]),
                    "pool_id": pool_id,
                    "pool_label": _pool_label(
                        pool.competition_scope, pool.segment_level_preset
                    ),
                    "marking_score": float(score),
                    "marking_score_reference": float(score_ref),
                    "score_delta": float(score) - float(score_ref),
                    "rank": int(rank) if pd.notna(rank) else None,
                    "rank_reference": int(rank_ref) if pd.notna(rank_ref) else None,
                    "rank_delta": int(rank) - int(rank_ref)
                    if pd.notna(rank) and pd.notna(rank_ref)
                    else None,
                }
            )
    return pd.DataFrame(rows)


def build_judge_tier_stability_figure(stability: pd.DataFrame, *, reference_label: str):
    import plotly.graph_objects as go

    if stability.empty:
        return None
    plot_df = stability.loc[stability["pool_label"] != reference_label].copy()
    if plot_df.empty:
        return None
    plot_df = plot_df.sort_values("tier_match_pct", ascending=True)
    fig = go.Figure(
        go.Bar(
            x=plot_df["tier_match_pct"],
            y=plot_df["pool_label"],
            orientation="h",
            marker_color="steelblue",
            hovertemplate="%{y}<br>tier match=%{x:.1f}%<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Tier stability vs reference ({reference_label})",
        xaxis_title="Tier match (%)",
        yaxis_title="Benchmark σ̂ pool",
        template="plotly_white",
        height=max(400, 28 * len(plot_df)),
        margin=dict(l=220, r=40, t=60),
    )
    return fig


def _load_ranking_marks(
    analytics: JudgeAnalytics,
    *,
    competition_scope: str,
    segment_level_preset: str | None,
    discipline_type_ids: list[int] | None,
) -> pd.DataFrame:
    marks = collect_marks_for_run(
        analytics,
        competition_scope=competition_scope,
        segment_level_preset=segment_level_preset,
        discipline_type_ids=discipline_type_ids,
        cache_only=True,
        persist_shards=False,
    )
    if marks is None or marks.empty:
        marks = collect_marks_for_run(
            analytics,
            competition_scope=competition_scope,
            segment_level_preset=segment_level_preset,
            discipline_type_ids=discipline_type_ids,
            cache_only=False,
            persist_shards=False,
        )
    if marks is None or marks.empty:
        return pd.DataFrame()
    if "judge_id" in marks.columns:
        marks = attach_judge_identities(marks, analytics)
    return marks


def run_judge_comparison_for_scenario(
    marks: pd.DataFrame,
    pools: list[BenchmarkSigmaPool],
    scenario: RankingScenario,
    discipline_group: JudgeDisciplineGroup,
    *,
    out_prefix: Path,
    sigma_model: str,
    min_marks: int,
    tier_top_fraction: float,
    tier_bottom_fraction: float,
    us_directory_labels: frozenset[str] | None,
) -> tuple[list[str], dict]:
    """Write judge comparison outputs for one ranking scenario and discipline group."""
    group_marks = filter_marks_for_discipline_group(marks, discipline_group)
    us_labels = us_directory_labels if scenario.us_directory_only else None
    wide_scores, tier_df = compute_judge_scores_across_pools(
        group_marks,
        pools,
        sigma_model=sigma_model,
        min_marks=min_marks,
        tier_top_fraction=tier_top_fraction,
        tier_bottom_fraction=tier_bottom_fraction,
        us_directory_labels=us_labels,
    )
    paths: list[str] = []
    scenario_meta: dict = {
        "scenario": scenario.slug,
        "scenario_label": scenario.label,
        "discipline_group": discipline_group.key,
        "discipline_group_label": discipline_group.label,
        "discipline_type_ids": list(discipline_group.discipline_type_ids),
        "competition_scope": scenario.competition_scope,
        "segment_level": scenario.segment_level,
        "us_directory_only": scenario.us_directory_only,
        "tier_reference_pool_id": scenario.tier_reference_pool_id,
    }
    if wide_scores.empty:
        scenario_meta["error"] = "No judges met min_marks after filters."
        scenario_meta["n_ranking_marks"] = len(group_marks)
        return paths, scenario_meta

    suffix = f"_{scenario.slug}_{discipline_group.key}"
    if not tier_df.empty:
        tier_df = tier_df.copy()
        tier_df.insert(0, "discipline_group", discipline_group.key)
        tier_df.insert(1, "discipline_group_label", discipline_group.label)
    wide_out = wide_scores.copy()
    wide_out.insert(0, "discipline_group", discipline_group.key)
    wide_out.insert(1, "discipline_group_label", discipline_group.label)

    judges_path = out_prefix.parent / f"{out_prefix.name}{suffix}_judges.csv"
    wide_out.to_csv(judges_path, index=False)
    paths.append(str(judges_path))

    ref_pool_id = scenario.tier_reference_pool_id
    deltas = build_judge_score_delta_table(
        wide_scores, pools, reference_pool_id=ref_pool_id
    )
    if not deltas.empty:
        deltas_path = out_prefix.parent / f"{out_prefix.name}{suffix}_judge_deltas.csv"
        deltas.to_csv(deltas_path, index=False)
        paths.append(str(deltas_path))

    stability = pd.DataFrame()
    transitions = pd.DataFrame()
    if not tier_df.empty:
        tiers_path = out_prefix.parent / f"{out_prefix.name}{suffix}_judge_tiers.csv"
        tier_df.to_csv(tiers_path, index=False)
        paths.append(str(tiers_path))

        stability, transitions = build_judge_tier_stability(
            tier_df, reference_pool_id=ref_pool_id
        )
        if not stability.empty:
            stability_path = (
                out_prefix.parent / f"{out_prefix.name}{suffix}_judge_tier_stability.csv"
            )
            stability.to_csv(stability_path, index=False)
            paths.append(str(stability_path))
        if not transitions.empty:
            trans_path = (
                out_prefix.parent
                / f"{out_prefix.name}{suffix}_judge_tier_transitions.csv"
            )
            transitions.to_csv(trans_path, index=False)
            paths.append(str(trans_path))

        tier_fig = build_judge_tier_stability_figure(
            stability,
            reference_label=f"{scenario.label} — {discipline_group.label}",
        )
        if tier_fig is not None:
            tier_plot = (
                out_prefix.parent / f"{out_prefix.name}{suffix}_judge_tier_stability.html"
            )
            tier_fig.write_html(str(tier_plot), include_plotlyjs="cdn")
            paths.append(str(tier_plot))

    scenario_meta.update(
        {
            "n_ranking_marks": len(group_marks),
            "n_judges": int(len(wide_scores)),
            "min_marks": int(min_marks),
            "tier_top_fraction": float(tier_top_fraction),
            "tier_bottom_fraction": float(tier_bottom_fraction),
        }
    )
    if not deltas.empty:
        other = deltas.loc[deltas["pool_id"] != ref_pool_id, "score_delta"]
        scenario_meta["mean_abs_score_delta"] = float(other.abs().mean())
        scenario_meta["max_abs_score_delta"] = float(other.abs().max())
    if not stability.empty:
        other = stability.loc[stability["pool_id"] != ref_pool_id]
        if not other.empty:
            scenario_meta["mean_tier_match_pct"] = float(other["tier_match_pct"].mean())
            scenario_meta["min_tier_match_pct"] = float(other["tier_match_pct"].min())
    if not tier_df.empty:
        ref_tiers = tier_df.loc[tier_df["pool_id"] == ref_pool_id]
        if not ref_tiers.empty:
            scenario_meta["reference_tier_counts"] = {
                str(k): int(v) for k, v in ref_tiers["tier"].value_counts().items()
            }
    return paths, scenario_meta


def build_pool_overlay_figure(
    curves: pd.DataFrame,
    *,
    discipline: str,
    component: str,
    title_suffix: str = "",
) -> "go.Figure | None":
    import plotly.graph_objects as go

    subset = curves.loc[
        (curves["discipline"] == discipline) & (curves["component"] == component)
    ]
    if subset.empty:
        return None

    fig = go.Figure()
    level_styles = {
        ELEMENT_RANKING_LEVEL_FILTER_ALL: "solid",
        "novice_junior_senior": "dash",
        "junior_senior": "dot",
    }
    for pool_label, grp in subset.groupby("pool_label", sort=False):
        grp = grp.sort_values("control_score")
        level_key = grp["segment_level_preset"].iloc[0]
        legend_name = _pool_legend_label(
            str(grp["competition_scope_label"].iloc[0]), str(level_key)
        )
        fig.add_trace(
            go.Scatter(
                x=grp["control_score"],
                y=grp["sigma"],
                mode="lines",
                name=legend_name,
                legendgroup=pool_label,
                line=dict(
                    dash=level_styles.get(level_key, "solid"),
                    width=2,
                ),
                hovertemplate=(
                    f"{pool_label}<br>c=%{{x:.2f}}<br>σ̂=%{{y:.3f}}<extra></extra>"
                ),
            )
        )

    title = f"{discipline} — {component}"
    if title_suffix:
        title = f"{title} ({title_suffix})"
    fig.update_layout(
        title=title,
        xaxis_title="Control score",
        yaxis_title="Benchmark σ̂(c)",
        template="plotly_white",
        width=1050,
        height=520,
        margin=dict(r=260, t=60),
        legend=dict(
            x=1.02,
            xanchor="left",
            y=1,
            yanchor="top",
            font=dict(size=10),
            tracegroupgap=2,
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="lightgray",
            borderwidth=1,
        ),
    )
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sigma-model",
        choices=[PCS_SIGMA_MODEL_DISCRETE, PCS_SIGMA_MODEL_QUADRATIC],
        default=PCS_SIGMA_MODEL_QUADRATIC,
        help="Which cached σ̂ model to compare (default: quadratic).",
    )
    parser.add_argument(
        "--scopes",
        nargs="+",
        choices=list(PCS_DEVIATION_COMPETITION_SCOPES),
        help="Limit to these competition scopes (default: all cached scopes).",
    )
    parser.add_argument(
        "--segment-levels",
        nargs="+",
        choices=list(ELEMENT_RANKING_LEVEL_FILTER_PRESETS),
        help="Limit to these segment level presets (default: all cached levels).",
    )
    parser.add_argument(
        "--discipline-type-id",
        type=int,
        action="append",
        dest="discipline_type_ids",
        help="Repeat to limit disciplines (default: all in cache).",
    )
    parser.add_argument(
        "--control-min",
        type=float,
        default=4.0,
        help="Minimum control score for curve grid (default: 4.0).",
    )
    parser.add_argument(
        "--control-max",
        type=float,
        default=9.5,
        help="Maximum control score for curve grid (default: 9.5).",
    )
    parser.add_argument(
        "--control-step",
        type=float,
        default=0.25,
        help="Control score step for curve grid (default: 0.25).",
    )
    parser.add_argument(
        "--reference-scope",
        default="all",
        choices=list(PCS_DEVIATION_COMPETITION_SCOPES),
        help="Reference pool scope for spread deltas (default: all).",
    )
    parser.add_argument(
        "--reference-level",
        default=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
        choices=list(ELEMENT_RANKING_LEVEL_FILTER_PRESETS),
        help="Reference segment level for spread/tier deltas (default: junior_senior).",
    )
    parser.add_argument(
        "--skip-judge-scores",
        action="store_true",
        help="Skip judge marking-score comparison (faster; curves only).",
    )
    parser.add_argument(
        "--ranking-scenarios",
        nargs="+",
        metavar="SCOPE/LEVEL",
        help=(
            "Judge mark pools to compare (repeatable). "
            "Default: all/junior_senior and international/junior_senior. "
            "Example: international/junior_senior"
        ),
    )
    parser.add_argument(
        "--ranking-scope",
        default=None,
        choices=list(PCS_DEVIATION_COMPETITION_SCOPES),
        help="Single ranking scope (use with --ranking-segment-level instead of --ranking-scenarios).",
    )
    parser.add_argument(
        "--ranking-segment-level",
        default=None,
        choices=list(ELEMENT_RANKING_LEVEL_FILTER_PRESETS),
        help="Segment level for a single ranking pool (pairs with --ranking-scope).",
    )
    parser.add_argument(
        "--us-directory-only",
        action="store_true",
        help=(
            "Also run each ranking scenario limited to judges linked to a USFS "
            "directory official (Admin → Judge ↔ directory matcher)."
        ),
    )
    parser.add_argument(
        "--min-marks",
        type=int,
        default=100,
        help="Minimum PCS marks for a judge to be included (default: 100).",
    )
    parser.add_argument(
        "--tier-top-fraction",
        type=float,
        default=0.30,
        help="Fraction of judges in the top tier (default: 0.30).",
    )
    parser.add_argument(
        "--tier-bottom-fraction",
        type=float,
        default=0.10,
        help="Fraction of judges in the bottom tier (default: 0.10).",
    )
    parser.add_argument(
        "-o",
        "--output-prefix",
        type=Path,
        default=Path("analysisTemp/pcs_benchmark_sigma_pools"),
        help="Output path prefix (.csv, .json, and _*.html plots).",
    )
    args = parser.parse_args()

    control_scores = np.arange(
        float(args.control_min),
        float(args.control_max) + 1e-9,
        float(args.control_step),
    )

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
        pools = load_cached_benchmark_sigma_pools(
            session,
            sigma_model=args.sigma_model,
            scopes=args.scopes,
            segment_levels=args.segment_levels,
        )
        ranking_scenarios = ranking_scenarios_from_args(
            ranking_scenarios=args.ranking_scenarios,
            ranking_scope=args.ranking_scope,
            ranking_segment_level=args.ranking_segment_level,
            us_directory_only=args.us_directory_only,
        )
        us_directory_labels = analytics.get_us_linked_identity_labels()
        marks_by_scenario: dict[str, pd.DataFrame] = {}
        if not args.skip_judge_scores and pools:
            loaded_scopes: set[tuple[str, str | None]] = set()
            for scenario in ranking_scenarios:
                key = (
                    scenario.competition_scope,
                    _segment_level_preset_arg(scenario.segment_level),
                )
                if key in loaded_scopes:
                    continue
                loaded_scopes.add(key)
                marks = _load_ranking_marks(
                    analytics,
                    competition_scope=scenario.competition_scope,
                    segment_level_preset=key[1],
                    discipline_type_ids=args.discipline_type_ids,
                )
                marks_by_scenario[f"{key[0]}__{scenario.segment_level}"] = marks
    finally:
        session.close()

    if not pools:
        print("No matching benchmark σ̂ cache rows found.", file=sys.stderr)
        raise SystemExit(1)

    curves = build_sigma_curves_dataframe(
        pools,
        discipline_type_ids=args.discipline_type_ids,
        discipline_id_to_name=disc_map,
        control_scores=control_scores,
    )
    if curves.empty:
        print("No σ̂ curves could be evaluated for the selected filters.", file=sys.stderr)
        raise SystemExit(1)

    spread = build_sigma_spread_dataframe(
        curves,
        reference_scope=args.reference_scope,
        reference_level=args.reference_level,
    )

    out_prefix: Path = args.output_prefix
    out_prefix.parent.mkdir(parents=True, exist_ok=True)
    curves.to_csv(out_prefix.with_suffix(".csv"), index=False)
    spread.to_csv(
        out_prefix.parent / f"{out_prefix.name}_spread.csv", index=False
    )

    meta = {
        "sigma_model": normalize_sigma_model(args.sigma_model),
        "n_pools": len(pools),
        "pools": [
            {
                "competition_scope": p.competition_scope,
                "competition_scope_label": _scope_label(p.competition_scope),
                "segment_level_preset": p.segment_level_preset,
                "segment_level_label": _level_label(p.segment_level_preset),
                "pool_label": _pool_label(p.competition_scope, p.segment_level_preset),
                "n_marks": p.n_marks,
                "sigma_key": p.sigma_key,
                "floor_sigma": p.floor_sigma,
                "min_bin_count": p.min_bin_count,
                "n_series": len(p.params),
            }
            for p in pools
        ],
        "reference_pool": {
            "competition_scope": args.reference_scope,
            "segment_level_preset": args.reference_level,
            "pool_label": _pool_label(args.reference_scope, args.reference_level),
        },
        "control_grid": {
            "min": float(args.control_min),
            "max": float(args.control_max),
            "step": float(args.control_step),
        },
        "spread_summary": {},
    }
    if not spread.empty:
        meta["spread_summary"] = {
            "mean_sigma_range": float(spread["sigma_range"].mean()),
            "max_sigma_range": float(spread["sigma_range"].max()),
            "mean_sigma_cv_pct": float(spread["sigma_cv_pct"].mean()),
            "max_sigma_cv_pct": float(spread["sigma_cv_pct"].max()),
        }
    out_prefix.with_suffix(".json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )

    reference_pool_id = _pool_column_id(args.reference_scope, args.reference_level)
    judge_paths: list[str] = []
    judge_scenario_meta: list[dict] = []
    if not args.skip_judge_scores:
        sigma_model = normalize_sigma_model(args.sigma_model)
        for scenario in ranking_scenarios:
            marks_key = f"{scenario.competition_scope}__{scenario.segment_level}"
            ranking_marks = marks_by_scenario.get(marks_key, pd.DataFrame())
            if ranking_marks.empty:
                print(
                    f"No ranking marks for {scenario.label}; skipping.",
                    file=sys.stderr,
                )
                continue
            if scenario.us_directory_only and not us_directory_labels:
                print(
                    "US directory filter requested but no linked judges found; skipping.",
                    file=sys.stderr,
                )
                continue
            print(f"\nJudge comparison: {scenario.label} ({len(ranking_marks):,} marks)")
            for discipline_group in JUDGE_DISCIPLINE_GROUPS:
                group_marks = filter_marks_for_discipline_group(
                    ranking_marks, discipline_group
                )
                if group_marks.empty:
                    print(f"  {discipline_group.label}: no marks; skipping.")
                    continue
                print(
                    f"  {discipline_group.label} ({len(group_marks):,} marks)"
                )
                paths, scenario_meta = run_judge_comparison_for_scenario(
                    ranking_marks,
                    pools,
                    scenario,
                    discipline_group,
                    out_prefix=out_prefix,
                    sigma_model=sigma_model,
                    min_marks=int(args.min_marks),
                    tier_top_fraction=float(args.tier_top_fraction),
                    tier_bottom_fraction=float(args.tier_bottom_fraction),
                    us_directory_labels=us_directory_labels,
                )
                judge_paths.extend(paths)
                judge_scenario_meta.append(scenario_meta)
                if scenario_meta.get("n_judges"):
                    print(
                        f"    {scenario_meta['n_judges']} judges "
                        f"(>={args.min_marks} marks in group)"
                    )
                    if scenario_meta.get("mean_abs_score_delta") is not None:
                        print(
                            f"    Mean |score delta| vs "
                            f"{scenario.tier_reference_pool_id}: "
                            f"{scenario_meta['mean_abs_score_delta']:.4f}"
                        )
                    if scenario_meta.get("mean_tier_match_pct") is not None:
                        print(
                            f"    Mean tier match: "
                            f"{scenario_meta['mean_tier_match_pct']:.1f}%"
                        )
                elif scenario_meta.get("error"):
                    print(f"    {scenario_meta['error']}")
        if judge_scenario_meta:
            meta["judge_scenarios"] = judge_scenario_meta
            out_prefix.with_suffix(".json").write_text(
                json.dumps(meta, indent=2), encoding="utf-8"
            )

    plot_paths: list[str] = []
    for (disc_name, component), _grp in curves.groupby(
        ["discipline", "component"], sort=False
    ):
        fig = build_pool_overlay_figure(
            curves,
            discipline=str(disc_name),
            component=str(component),
            title_suffix=normalize_sigma_model(args.sigma_model),
        )
        if fig is None:
            continue
        plot_path = (
            out_prefix.parent
            / f"{out_prefix.name}_{_slug(disc_name)}_{_slug(component)}.html"
        )
        fig.write_html(str(plot_path), include_plotlyjs="cdn")
        plot_paths.append(str(plot_path))

    print(f"Loaded {len(pools)} benchmark pool(s) from σ̂ cache ({args.sigma_model}).")
    for p in pools:
        print(
            f"  {_pool_label(p.competition_scope, p.segment_level_preset):45} "
            f"n_marks={p.n_marks:,}"
        )
    if meta["spread_summary"]:
        print(
            f"Mean σ̂ range across pools/grid: "
            f"{meta['spread_summary']['mean_sigma_range']:.4f} "
            f"(max {meta['spread_summary']['max_sigma_range']:.4f})"
        )
        print(
            f"Mean coefficient of variation: "
            f"{meta['spread_summary']['mean_sigma_cv_pct']:.2f}%"
        )
    print(f"Wrote curves CSV: {out_prefix.with_suffix('.csv').resolve()}")
    print(
        f"Wrote spread CSV: "
        f"{(out_prefix.parent / f'{out_prefix.name}_spread.csv').resolve()}"
    )
    print(f"Wrote metadata JSON: {out_prefix.with_suffix('.json').resolve()}")
    for p in judge_paths:
        print(f"Wrote judge output: {p}")
    for p in plot_paths:
        print(f"Wrote plot: {p}")


if __name__ == "__main__":
    main()
