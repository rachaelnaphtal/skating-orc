#!/usr/bin/env python3
"""
Panel-level PCS σ heteroscedasticity plot (unbinned control score vs empirical σ).

Each point is one skater-segment × PCS component: x = panel median (control score),
y = sample stdev of judge−median errors on that panel (ddof=1). Fits a direct
quadratic in σ and overlays production binned models for comparison.

Example::

    python scripts/plot_pcs_panel_sigma_fit.py \\
        --discipline-type-id 3 --component SS \\
        --start-season 2223 --end-season 2526 \\
        --competition-scope isu_event \\
        -o analysisTemp/ice_dance_ss_isu_panel_sigma.html
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from analytics import JudgeAnalytics  # noqa: E402
from database import get_db_session  # noqa: E402
from element_deviation_ranking import ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR  # noqa: E402
from models import Competition  # noqa: E402
from pcs_deviation_analysis import (  # noqa: E402
    FLOOR_SIGMA,
    MIN_BIN_COUNT,
    PCS_SIGMA_BIN_WIDTH,
    collect_sigma_plot_stats_pcs,
    compute_errors,
    direct_sigma_quadratic_equation_str,
    direct_sigma_quadratic_eval,
    fit_direct_sigma_quadratic_from_stats,
    fit_sigma_quadratic_pcs,
    load_pcs_deviation_marks,
    quadratic_sigma_equation_str,
    quadratic_sigma_from_variance_eval,
    sample_error_stdev,
)


def _filter_work_by_competition_start_date(
    analytics: JudgeAnalytics,
    work: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Keep marks whose competition ``start_date`` falls in [start_date, end_date]."""
    if work.empty or "competition_id" not in work.columns:
        return work.iloc[0:0].copy()
    cids = sorted({int(c) for c in work["competition_id"].dropna().unique()})
    if not cids:
        return work.iloc[0:0].copy()
    rows = (
        analytics.session.query(Competition.id, Competition.start_date)
        .filter(Competition.id.in_(cids))
        .all()
    )
    allowed = {
        int(cid)
        for cid, sd in rows
        if sd is not None and start_date <= sd <= end_date
    }
    return work.loc[work["competition_id"].isin(allowed)].copy()


def collect_sigma_per_panel_stats_pcs(
    df: pd.DataFrame,
    *,
    min_judges: int = 2,
) -> pd.DataFrame:
    """One empirical σ per skater-segment × component (panel)."""
    work = df.dropna(subset=["skater_segment_id", "component", "control_score", "error"])
    if work.empty:
        return pd.DataFrame(
            columns=[
                "skater_segment_id",
                "discipline_type_id",
                "component",
                "competition_id",
                "control_score",
                "sigma_empirical",
                "count",
            ]
        )
    if "error" not in work.columns:
        work = compute_errors(work)

    rows: list[dict] = []
    group_cols = ["skater_segment_id", "discipline_type_id", "component"]
    for key, grp in work.groupby(group_cols, sort=False):
        n = int(len(grp))
        if n < int(min_judges):
            continue
        sd = sample_error_stdev(grp["error"])
        if not np.isfinite(sd) or sd <= 0:
            continue
        skater_segment_id, disc_id, component = key
        control = float(grp["control_score"].iloc[0])
        row = {
            "skater_segment_id": int(skater_segment_id),
            "discipline_type_id": int(disc_id),
            "component": str(component),
            "control_score": control,
            "sigma_empirical": float(sd),
            "count": n,
        }
        if "competition_id" in grp.columns:
            row["competition_id"] = int(grp["competition_id"].iloc[0])
        rows.append(row)
    return pd.DataFrame(rows)


def _rmse_sigma(
    x: np.ndarray, y: np.ndarray, y_hat: np.ndarray, weights: np.ndarray | None = None
) -> float:
    if weights is None:
        return float(np.sqrt(np.mean((y - y_hat) ** 2)))
    return float(np.sqrt(np.average((y - y_hat) ** 2, weights=weights)))


def build_panel_sigma_comparison_figure(
    variance_params: dict[str, float],
    *,
    bin_stats: pd.DataFrame | None = None,
    panel_stats: pd.DataFrame | None = None,
    panel_direct: dict[str, float] | None = None,
    bin_direct: dict[str, float] | None = None,
    reference_sigma: dict[str, float] | None = None,
    reference_label: str = "External reference",
    title: str,
    floor_sigma: float = FLOOR_SIGMA,
    bin_width: float = PCS_SIGMA_BIN_WIDTH,
    show_bin_dots: bool = True,
    show_panel_dots: bool = False,
    x_max: float = 10.0,
):
    import plotly.graph_objects as go

    has_bins = bin_stats is not None and not bin_stats.empty
    has_panels = panel_stats is not None and not panel_stats.empty
    if not has_bins and not has_panels and not variance_params:
        return None

    fig = go.Figure()
    if show_panel_dots and has_panels:
        panel_alpha = 0.35 if show_bin_dots else 0.45
        fig.add_trace(
            go.Scatter(
                x=panel_stats["control_score"],
                y=panel_stats["sigma_empirical"],
                mode="markers",
                name=f"Panels (n={len(panel_stats):,})",
                marker=dict(
                    size=5 if show_bin_dots else 6,
                    color=f"rgba(30,100,200,{panel_alpha})",
                    line=dict(width=0.3, color="rgba(20,60,140,0.5)"),
                ),
                hovertemplate="c=%{x:.2f}<br>σ=%{y:.3f}<extra>panel</extra>",
            )
        )
    if show_bin_dots and has_bins:
        fig.add_trace(
            go.Scatter(
                x=bin_stats["control_score_mean"],
                y=bin_stats["sigma_empirical"],
                mode="markers",
                name=f"σ bins ({bin_width:g} PCS, n={len(bin_stats)})",
                marker=dict(
                    size=11,
                    color=bin_stats["count"],
                    colorscale="RdYlGn_r",
                    cmin=float(bin_stats["count"].min()),
                    cmax=float(bin_stats["count"].max()),
                    showscale=True,
                    colorbar=dict(title="Marks"),
                    line=dict(width=0.5, color="rgba(0,0,0,0.3)"),
                ),
                hovertemplate="c=%{x:.2f}<br>σ=%{y:.3f}<extra>bin</extra>",
            )
        )

    if has_panels:
        x_ref = panel_stats["control_score"]
    elif has_bins:
        x_ref = bin_stats["control_score_mean"]
    else:
        x_ref = pd.Series([1.0, 9.0])
    x_min = max(0.5, float(x_ref.min()) - 0.5)
    xs = np.linspace(x_min, x_max, 200)

    if variance_params:
        ys_var = quadratic_sigma_from_variance_eval(
            xs,
            variance_params["a"],
            variance_params["b"],
            variance_params["c"],
            floor_sigma=floor_sigma,
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

    if bin_direct:
        ys_bin = direct_sigma_quadratic_eval(
            xs,
            bin_direct["direct_sigma_a"],
            bin_direct["direct_sigma_b"],
            bin_direct["direct_sigma_c0"],
            floor_sigma=floor_sigma,
        )
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys_bin,
                mode="lines",
                name="Direct σ fit (bins)",
                line=dict(color="gray", width=2, dash="dash"),
            )
        )

    if panel_direct:
        ys_panel = direct_sigma_quadratic_eval(
            xs,
            panel_direct["direct_sigma_a"],
            panel_direct["direct_sigma_b"],
            panel_direct["direct_sigma_c0"],
            floor_sigma=floor_sigma,
        )
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys_panel,
                mode="lines",
                name="Direct σ on panels",
                line=dict(color="crimson", width=2.5),
            )
        )

    if reference_sigma and all(
        k in reference_sigma for k in ("direct_sigma_a", "direct_sigma_b", "direct_sigma_c0")
    ):
        ys_ref = direct_sigma_quadratic_eval(
            xs,
            reference_sigma["direct_sigma_a"],
            reference_sigma["direct_sigma_b"],
            reference_sigma["direct_sigma_c0"],
            floor_sigma=floor_sigma,
        )
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys_ref,
                mode="lines",
                name=reference_label,
                line=dict(color="#2ca02c", width=2.5, dash="dot"),
            )
        )

    fig.update_layout(
        title=title,
        xaxis_title="Panel control score (median PCS)",
        yaxis_title="Empirical σ (judge − median, ddof=1)",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        template="plotly_white",
        height=560,
        width=900,
    )
    return fig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-season", default="2223", help="Fit pool: season from")
    parser.add_argument("--end-season", default="2526", help="Fit pool: season to")
    parser.add_argument(
        "--plot-start-season",
        default=None,
        help="Plot dots from this season (default: same as --start-season)",
    )
    parser.add_argument(
        "--plot-end-season",
        default=None,
        help="Plot dots through this season (default: same as --end-season)",
    )
    parser.add_argument("--discipline-type-id", type=int, default=3)
    parser.add_argument("--component", default="SS", help="PCS component code, e.g. SS")
    parser.add_argument(
        "--competition-scope",
        default="isu_event",
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
    )
    parser.add_argument("--min-judges", type=int, default=2)
    parser.add_argument("--min-bin-count", type=int, default=MIN_BIN_COUNT)
    parser.add_argument(
        "--bin-width",
        type=float,
        default=PCS_SIGMA_BIN_WIDTH,
        help="PCS bin width for binned fits/overlays (default 0.25)",
    )
    parser.add_argument("--floor-sigma", type=float, default=FLOOR_SIGMA)
    parser.add_argument(
        "--bins-only",
        action="store_true",
        help="Show binned dots only (no per-panel cloud)",
    )
    parser.add_argument(
        "--plot-panels",
        action="store_true",
        help="Include per-panel dots (with binned dots unless --panels-only)",
    )
    parser.add_argument(
        "--panels-only",
        action="store_true",
        help="Per-panel dots only (hide binned summary dots)",
    )
    parser.add_argument(
        "--show-slide-reference",
        action="store_true",
        help="Overlay external σ̂(c)=0.353+0.0429·c−0.00626·c²",
    )
    parser.add_argument(
        "--reference-sigma-c0",
        type=float,
        default=None,
        help="External direct-σ intercept (with --reference-sigma-b/a)",
    )
    parser.add_argument("--reference-sigma-b", type=float, default=None)
    parser.add_argument("--reference-sigma-a", type=float, default=None)
    parser.add_argument(
        "--plot-competition-start-from",
        default=None,
        help="Plot only competitions with start_date on/after this ISO date",
    )
    parser.add_argument(
        "--plot-competition-start-to",
        default=None,
        help="Plot only competitions with start_date on/before this ISO date",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("analysisTemp/pcs_panel_sigma_fit.html"),
    )
    args = parser.parse_args()
    plot_start = args.plot_start_season or args.start_season
    plot_end = args.plot_end_season or args.end_season
    plot_comp_start = (
        date.fromisoformat(args.plot_competition_start_from)
        if args.plot_competition_start_from
        else None
    )
    plot_comp_end = (
        date.fromisoformat(args.plot_competition_start_to)
        if args.plot_competition_start_to
        else None
    )

    def _load_component_marks(analytics, start_sy: str, end_sy: str) -> pd.DataFrame:
        df = load_pcs_deviation_marks(
            analytics,
            start_season_year=start_sy,
            end_season_year=end_sy,
            discipline_type_ids=[args.discipline_type_id],
            competition_scope=args.competition_scope,
            segment_level_preset=args.segment_level_preset,
        )
        if df.empty:
            return df
        work = compute_errors(df)
        return work.loc[work["component"] == str(args.component)].copy()

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        work_fit = _load_component_marks(analytics, args.start_season, args.end_season)
        if plot_start != args.start_season or plot_end != args.end_season:
            work_plot = _load_component_marks(analytics, plot_start, plot_end)
        else:
            work_plot = work_fit.copy()
        if plot_comp_start is not None or plot_comp_end is not None:
            comp_from = plot_comp_start or date.min
            comp_to = plot_comp_end or date.max
            work_plot = _filter_work_by_competition_start_date(
                analytics, work_plot, start_date=comp_from, end_date=comp_to
            )
        disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
    finally:
        session.close()

    if work_fit.empty:
        print("No PCS marks for the fit-season filters.")
        raise SystemExit(1)
    if work_plot.empty:
        print("No PCS marks for the plot-season filters.")
        raise SystemExit(1)

    panel_stats = collect_sigma_per_panel_stats_pcs(work_plot, min_judges=args.min_judges)
    bin_width = float(args.bin_width)
    bin_stats = collect_sigma_plot_stats_pcs(
        work_plot, bin_width=bin_width, min_bin_count=int(args.min_bin_count)
    )
    if args.bins_only and args.panels_only:
        print("Use only one of --bins-only and --panels-only.")
        raise SystemExit(2)
    if args.plot_panels:
        if panel_stats.empty:
            print("No per-panel σ points for the plot filters.")
            raise SystemExit(1)
    if not args.panels_only and bin_stats.empty:
        print("No σ bins meet min_bin_count for the plot-season filters.")
        raise SystemExit(1)
    quad_params = fit_sigma_quadratic_pcs(
        work_fit,
        min_bin_count=int(args.min_bin_count),
        floor_sigma=float(args.floor_sigma),
        bin_width=bin_width,
    )
    key = (int(args.discipline_type_id), str(args.component))
    variance_params = quad_params.get(key, {})
    bin_direct = (
        {k: variance_params[k] for k in ("direct_sigma_a", "direct_sigma_b", "direct_sigma_c0")}
        if all(k in variance_params for k in ("direct_sigma_a", "direct_sigma_b", "direct_sigma_c0"))
        else fit_direct_sigma_quadratic_from_stats(bin_stats)
    )
    panel_for_fit = panel_stats.rename(columns={"control_score": "control_score_mean"})
    panel_direct = (
        None
        if args.bins_only or args.plot_panels or args.panels_only
        else fit_direct_sigma_quadratic_from_stats(panel_for_fit)
    )

    reference_sigma = None
    reference_label = "External reference"
    if args.show_slide_reference:
        reference_sigma = {
            "direct_sigma_a": -0.00626,
            "direct_sigma_b": 0.0429,
            "direct_sigma_c0": 0.353,
        }
        reference_label = "Slide: 0.353+0.0429·c−0.00626·c²"
    elif (
        args.reference_sigma_c0 is not None
        and args.reference_sigma_b is not None
        and args.reference_sigma_a is not None
    ):
        reference_sigma = {
            "direct_sigma_a": float(args.reference_sigma_a),
            "direct_sigma_b": float(args.reference_sigma_b),
            "direct_sigma_c0": float(args.reference_sigma_c0),
        }
        reference_label = direct_sigma_quadratic_equation_str(reference_sigma)

    disc_name = disc_map.get(int(args.discipline_type_id), str(args.discipline_type_id))
    plot_label = (
        f"{plot_start}–{plot_end}"
        if plot_start != plot_end
        else str(plot_start)
    )
    fit_label = (
        f"{args.start_season}–{args.end_season}"
        if args.start_season != args.end_season
        else str(args.start_season)
    )
    if plot_label != fit_label:
        season_note = f"fit {fit_label}, plot {plot_label}"
    else:
        season_note = fit_label
    if plot_comp_start is not None or plot_comp_end is not None:
        comp_from_s = (plot_comp_start or date.min).isoformat()
        comp_to_s = (plot_comp_end or date.max).isoformat()
        season_note = f"{season_note}, comp start {comp_from_s}–{comp_to_s}"
    title = (
        f"{disc_name} — {args.component}: σ vs control "
        f"({season_note}, {args.competition_scope}, bins={bin_width:g})"
    )

    summary: dict = {
        "filters": {
            k: (str(v) if isinstance(v, Path) else v)
            for k, v in vars(args).items()
        },
        "fit_seasons": f"{args.start_season}–{args.end_season}",
        "plot_seasons": f"{plot_start}–{plot_end}",
        "plot_competition_start_from": (
            plot_comp_start.isoformat() if plot_comp_start else None
        ),
        "plot_competition_start_to": (
            plot_comp_end.isoformat() if plot_comp_end else None
        ),
        "n_competitions_plot": int(work_plot["competition_id"].nunique())
        if "competition_id" in work_plot.columns
        else None,
        "n_marks_fit": int(len(work_fit)),
        "n_marks_plot": int(len(work_plot)),
        "n_panels": int(len(panel_stats)),
        "n_bins_plotted": int(len(bin_stats)),
        "bin_width": bin_width,
    }

    if not args.bins_only and panel_direct:
        x = panel_stats["control_score"].astype(float).values
        y = panel_stats["sigma_empirical"].astype(float).values
        w = panel_stats["count"].astype(float).values
        y_hat = direct_sigma_quadratic_eval(
            x,
            panel_direct["direct_sigma_a"],
            panel_direct["direct_sigma_b"],
            panel_direct["direct_sigma_c0"],
            floor_sigma=float(args.floor_sigma),
        )
        summary["panel_direct_sigma"] = {
            "equation": direct_sigma_quadratic_equation_str(panel_direct),
            "rmse_on_panels": _rmse_sigma(x, y, np.asarray(y_hat), w),
            **panel_direct,
        }

    if bin_direct:
        xb = bin_stats["control_score_mean"].astype(float).values
        yb = bin_stats["sigma_empirical"].astype(float).values
        wb = bin_stats["count"].astype(float).values
        y_hat_bin = direct_sigma_quadratic_eval(
            xb,
            bin_direct["direct_sigma_a"],
            bin_direct["direct_sigma_b"],
            bin_direct["direct_sigma_c0"],
            floor_sigma=float(args.floor_sigma),
        )
        summary["bin_direct_sigma"] = {
            "equation": direct_sigma_quadratic_equation_str(bin_direct),
            "rmse_on_fit_bins": float(bin_direct.get("rmse_direct_sigma", np.nan)),
            "rmse_on_plot_bins": _rmse_sigma(xb, yb, np.asarray(y_hat_bin), wb),
            **bin_direct,
        }

    if variance_params:
        summary["production_variance_fit"] = {
            "equation": quadratic_sigma_equation_str(variance_params),
            "rmse_sigma_on_bins": variance_params.get("rmse_sigma"),
            "a": variance_params.get("a"),
            "b": variance_params.get("b"),
            "c": variance_params.get("c"),
        }
        if not args.bins_only and not args.plot_panels and not args.panels_only and panel_direct and not panel_stats.empty:
            x = panel_stats["control_score"].astype(float).values
            y = panel_stats["sigma_empirical"].astype(float).values
            w = panel_stats["count"].astype(float).values
            y_prod = quadratic_sigma_from_variance_eval(
                x,
                variance_params["a"],
                variance_params["b"],
                variance_params["c"],
                floor_sigma=float(args.floor_sigma),
            )
            summary["production_variance_fit"]["rmse_on_panels"] = _rmse_sigma(
                x, y, np.asarray(y_prod), w
            )
        xb = bin_stats["control_score_mean"].astype(float).values
        yb = bin_stats["sigma_empirical"].astype(float).values
        wb = bin_stats["count"].astype(float).values
        y_prod_bins = quadratic_sigma_from_variance_eval(
            xb,
            variance_params["a"],
            variance_params["b"],
            variance_params["c"],
            floor_sigma=float(args.floor_sigma),
        )
        summary["production_variance_fit"]["rmse_on_plot_bins"] = _rmse_sigma(
            xb, yb, np.asarray(y_prod_bins), wb
        )

    if reference_sigma and not panel_stats.empty:
        xp = panel_stats["control_score"].astype(float).values
        yp = panel_stats["sigma_empirical"].astype(float).values
        wp = panel_stats["count"].astype(float).values
        y_ref = direct_sigma_quadratic_eval(
            xp,
            reference_sigma["direct_sigma_a"],
            reference_sigma["direct_sigma_b"],
            reference_sigma["direct_sigma_c0"],
            floor_sigma=float(args.floor_sigma),
        )
        summary["reference_sigma"] = {
            "equation": direct_sigma_quadratic_equation_str(reference_sigma),
            "rmse_on_plot_panels": _rmse_sigma(xp, yp, np.asarray(y_ref), wp),
            **reference_sigma,
        }
    if variance_params and not panel_stats.empty and (args.plot_panels or args.panels_only):
        xp = panel_stats["control_score"].astype(float).values
        yp = panel_stats["sigma_empirical"].astype(float).values
        wp = panel_stats["count"].astype(float).values
        y_prod_p = quadratic_sigma_from_variance_eval(
            xp,
            variance_params["a"],
            variance_params["b"],
            variance_params["c"],
            floor_sigma=float(args.floor_sigma),
        )
        summary["production_variance_fit"]["rmse_on_plot_panels"] = _rmse_sigma(
            xp, yp, np.asarray(y_prod_p), wp
        )
        if bin_direct:
            y_dir_p = direct_sigma_quadratic_eval(
                xp,
                bin_direct["direct_sigma_a"],
                bin_direct["direct_sigma_b"],
                bin_direct["direct_sigma_c0"],
                floor_sigma=float(args.floor_sigma),
            )
            summary["bin_direct_sigma"]["rmse_on_plot_panels"] = _rmse_sigma(
                xp, yp, np.asarray(y_dir_p), wp
            )

    show_panel_dots = args.plot_panels or args.panels_only
    show_bin_dots = not args.panels_only

    fig = build_panel_sigma_comparison_figure(
        variance_params,
        bin_stats=bin_stats,
        panel_stats=panel_stats,
        panel_direct=panel_direct,
        bin_direct=bin_direct,
        reference_sigma=reference_sigma,
        reference_label=reference_label,
        title=title,
        floor_sigma=float(args.floor_sigma),
        bin_width=bin_width,
        show_bin_dots=show_bin_dots,
        show_panel_dots=show_panel_dots,
    )
    if fig is None:
        print("Could not build figure.")
        raise SystemExit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(args.output), include_plotlyjs="cdn")
    meta_path = args.output.with_suffix(".json")
    meta_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    comp_note = ""
    if plot_comp_start is not None or plot_comp_end is not None:
        comp_note = (
            f"  Plot competitions (start date): "
            f"{work_plot['competition_id'].nunique() if 'competition_id' in work_plot.columns else 0}"
        )
    print(
        f"Fit marks ({args.start_season}–{args.end_season}): {len(work_fit):,}  "
        f"Plot marks ({plot_start}–{plot_end}): {len(work_plot):,}{comp_note}  "
        f"Plot panels: {len(panel_stats)}  "
        f"Plot bins (≥{args.min_bin_count}): {len(bin_stats)}"
    )
    if bin_direct:
        print(summary["bin_direct_sigma"]["equation"])
        if np.isfinite(summary["bin_direct_sigma"].get("rmse_on_fit_bins", np.nan)):
            print(
                "Direct σ RMSE on fit-season bins: "
                f"{summary['bin_direct_sigma']['rmse_on_fit_bins']:.4f}"
            )
        print(
            "Direct σ RMSE on plot-season bins: "
            f"{summary['bin_direct_sigma']['rmse_on_plot_bins']:.4f}"
        )
    if variance_params:
        print(summary["production_variance_fit"]["equation"])
        print(
            "Variance fit RMSE on fit bins (σ): "
            f"{summary['production_variance_fit']['rmse_sigma_on_bins']:.4f}"
        )
        if "rmse_on_plot_bins" in summary["production_variance_fit"]:
            print(
                "Variance fit RMSE on plot bins: "
                f"{summary['production_variance_fit']['rmse_on_plot_bins']:.4f}"
            )
        if "rmse_on_plot_panels" in summary["production_variance_fit"]:
            print(
                "Variance fit RMSE on plot panels: "
                f"{summary['production_variance_fit']['rmse_on_plot_panels']:.4f}"
            )
    if summary.get("reference_sigma"):
        print(summary["reference_sigma"]["equation"])
        print(
            "Reference RMSE on plot panels: "
            f"{summary['reference_sigma']['rmse_on_plot_panels']:.4f}"
        )
    print(f"Wrote {args.output}")
    print(f"Wrote {meta_path}")


if __name__ == "__main__":
    main()
