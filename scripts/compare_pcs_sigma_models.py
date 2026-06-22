#!/usr/bin/env python3
"""
Compare discrete-bin vs quadratic σ̂ models for PCS deviation.

Loads PCS marks from the database, fits both models, writes per-series
heteroscedasticity plots (slide-style), and a judge-ranking comparison CSV
with **both** marking scores plus rank/score deltas.

Example::

    python scripts/compare_pcs_sigma_models.py \\
        --discipline-type-id 3 \\
        --start-season 2223 --end-season 2526 \\
        --competition-scope international \\
        -o analysisTemp/pcs_sigma_model_comparison
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from analytics import JudgeAnalytics  # noqa: E402
from database import get_db_session  # noqa: E402
from element_deviation_ranking import ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR  # noqa: E402
from element_deviation_ranking import attach_judge_identities  # noqa: E402
from pcs_deviation_analysis import (  # noqa: E402
    FLOOR_SIGMA,
    MIN_BIN_COUNT,
    PCS_SIGMA_MODEL_DISCRETE,
    PCS_SIGMA_MODEL_QUADRATIC,
    collect_sigma_per_competition_stats_pcs,
    collect_sigma_plot_stats_pcs,
    compare_pcs_sigma_model_rankings,
    compute_errors,
    fit_sigma_discrete_pcs,
    fit_sigma_quadratic_pcs,
    build_pcs_heteroscedasticity_figure,
    load_pcs_deviation_marks,
    min_bin_count_for_sigma_model,
)


def _slug(s: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in s).strip("_")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-season", default=None, help="e.g. 2223")
    parser.add_argument("--end-season", default=None, help="e.g. 2526")
    parser.add_argument(
        "--discipline-type-id",
        type=int,
        action="append",
        dest="discipline_type_ids",
        help="Repeat for multiple disciplines (default: all PCS disciplines)",
    )
    parser.add_argument(
        "--competition-scope",
        default="all",
        choices=[
            "all",
            "qualifying",
            "sectionals_and_championships",
            "championships_only",
            "international",
            "isu_event",
        ],
    )
    parser.add_argument(
        "--segment-level-preset",
        default=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
        help="Segment level preset (default: junior_senior)",
    )
    parser.add_argument(
        "--min-bin-count",
        type=int,
        default=None,
        help=(
            "Override min marks per 0.25 PCS σ̂ bin for both models "
            f"(default {MIN_BIN_COUNT})"
        ),
    )
    parser.add_argument("--floor-sigma", type=float, default=FLOOR_SIGMA)
    parser.add_argument(
        "-o",
        "--output-prefix",
        type=Path,
        default=Path("analysisTemp/pcs_sigma_model_comparison"),
        help="Output path prefix (writes .csv, .json, and _*.html plots)",
    )
    args = parser.parse_args()

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        disc_ids = args.discipline_type_ids
        if not disc_ids:
            from pcs_deviation_analysis import PCS_DEVIATION_DISCIPLINE_IDS

            disc_ids = sorted(PCS_DEVIATION_DISCIPLINE_IDS)

        df = load_pcs_deviation_marks(
            analytics,
            start_season_year=args.start_season,
            end_season_year=args.end_season,
            discipline_type_ids=disc_ids,
            competition_scope=args.competition_scope,
            segment_level_preset=args.segment_level_preset,
        )
        disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
        if not df.empty:
            df = attach_judge_identities(df, analytics)
    finally:
        session.close()

    if df.empty:
        print("No PCS marks for the selected filters.")
        raise SystemExit(1)

    work = compute_errors(df)
    disc_min = (
        int(args.min_bin_count)
        if args.min_bin_count is not None
        else min_bin_count_for_sigma_model(PCS_SIGMA_MODEL_DISCRETE)
    )
    quad_min = (
        int(args.min_bin_count)
        if args.min_bin_count is not None
        else min_bin_count_for_sigma_model(PCS_SIGMA_MODEL_QUADRATIC)
    )
    discrete_params = fit_sigma_discrete_pcs(work, min_bin_count=disc_min)
    quadratic_params = fit_sigma_quadratic_pcs(
        work, min_bin_count=quad_min, floor_sigma=args.floor_sigma
    )
    comparison, metrics = compare_pcs_sigma_model_rankings(
        work,
        discrete_params=discrete_params,
        quadratic_params=quadratic_params,
        floor_sigma=args.floor_sigma,
        min_bin_count=disc_min,
    )

    out_prefix: Path = args.output_prefix
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    comparison.to_csv(out_prefix.with_suffix(".csv"), index=False)
    meta = {
        "filters": {
            "start_season": args.start_season,
            "end_season": args.end_season,
            "discipline_type_ids": disc_ids,
            "competition_scope": args.competition_scope,
            "segment_level_preset": args.segment_level_preset,
        },
        "n_marks": len(work),
        "metrics": metrics,
        "quadratic_params": {
            f"{disc_map.get(d, d)}-{c}": p
            for (d, c), p in quadratic_params.items()
        },
    }
    out_prefix.with_suffix(".json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )

    stats_all = collect_sigma_plot_stats_pcs(work, min_bin_count=quad_min)
    per_comp_all = collect_sigma_per_competition_stats_pcs(work)
    plot_paths: list[str] = []
    for (disc_id, component), grp in stats_all.groupby(
        ["discipline_type_id", "component"], sort=False
    ):
        key = (int(disc_id), str(component))
        if key not in quadratic_params:
            continue
        disc_name = disc_map.get(int(disc_id), str(disc_id))
        title = f"{_slug(disc_name)}_{_slug(component)}"
        comp_mask = (
            per_comp_all["discipline_type_id"] == int(disc_id)
        ) & (per_comp_all["component"] == str(component))
        fig = build_pcs_heteroscedasticity_figure(
            grp,
            quadratic_params[key],
            title=f"{disc_name} — {component}",
            per_competition_stats=per_comp_all.loc[comp_mask],
        )
        plot_path = out_prefix.parent / f"{out_prefix.name}_{title}.html"
        fig.write_html(str(plot_path), include_plotlyjs="cdn")
        plot_paths.append(str(plot_path))

    print(f"PCS marks: {len(work):,}")
    print(f"Discrete σ̂ bins: {len(discrete_params):,}")
    print(f"Quadratic σ̂ series: {len(quadratic_params):,}")
    if metrics:
        print(
            f"Judge rank Spearman (discrete vs quadratic): "
            f"{metrics.get('rank_spearman', float('nan')):.3f}"
        )
        print(
            f"Mean |rank delta|: {metrics.get('mean_abs_rank_delta', float('nan')):.2f}"
        )
    print(f"Wrote comparison CSV: {out_prefix.with_suffix('.csv').resolve()}")
    print(f"Wrote metadata JSON: {out_prefix.with_suffix('.json').resolve()}")
    for p in plot_paths:
        print(f"Wrote plot: {p}")


if __name__ == "__main__":
    main()
