#!/usr/bin/env python3
"""
Compare PCS and element deviation marking scores at US Championships (type id 4).

σ̂ is fit on **sectionals + championships** marks for Junior & Senior singles/pairs
(the same domestic qualifying pool used elsewhere). Each judge's marking score is
computed from marks **at that US Championships only**, so you can compare how the
same panel behaves year to year.

Also includes cross-judge rule-error / anomaly stats when the shard cache is populated.

Example::

    python scripts/report_us_championships_judge_deviations.py
    python scripts/report_us_championships_judge_deviations.py \\
        --start-season 2223 --end-season 2526 \\
        -o analysisTemp/us_champs_judge_deviations
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import select

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from activityAnalysis.load_activity_data import US_CHAMPIONSHIPS_COMPETITION_TYPE_ID  # noqa: E402
from analytics import JudgeAnalytics  # noqa: E402
from cross_judge_cache import (  # noqa: E402
    _load_shard_judge_aggregates_sql,
    shard_cache_populated,
)
from database import get_db_session  # noqa: E402
from element_deviation_ranking import (  # noqa: E402
    ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
    FLOOR_SIGMA as ELEMENT_FLOOR_SIGMA,
    MIN_BIN_COUNT as ELEMENT_MIN_BIN_COUNT,
    build_element_mark_filters,
    compute_control_scores,
    finish_element_deviation_rankings_from_marks,
    fit_sigma_discrete,
    segment_levels_for_ranking_preset,
)
from models import (  # noqa: E402
    Competition,
    Element,
    ElementScorePerJudge,
    Segment,
    SkaterSegment,
)
from officials_competition_types import COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS  # noqa: E402
from pcs_deviation_analysis import (  # noqa: E402
    FLOOR_SIGMA as PCS_FLOOR_SIGMA,
    PCS_SIGMA_MODEL_QUADRATIC,
    finish_pcs_deviation_rankings_from_marks,
    fit_sigma_params_from_marks,
    load_pcs_deviation_marks,
    min_bin_count_for_sigma_model,
    normalize_sigma_model,
)

DEFAULT_SEGMENT_DISCIPLINE_TYPE_IDS = (1, 2)  # Singles, Pairs


def _seasons_in_range(analytics: JudgeAnalytics, start: str | None, end: str | None) -> list[str]:
    years = sorted(str(y) for y in analytics.get_years())
    if start:
        years = [y for y in years if y >= start]
    if end:
        years = [y for y in years if y <= end]
    return years


def _us_championship_competitions(
    analytics: JudgeAnalytics,
    seasons: list[str],
) -> pd.DataFrame:
    rows = (
        analytics.session.query(
            Competition.id,
            Competition.year,
            Competition.name,
            Competition.start_date,
            Competition.end_date,
        )
        .filter(
            Competition.officials_analysis_competition_type_id
            == US_CHAMPIONSHIPS_COMPETITION_TYPE_ID
        )
        .filter(Competition.year.in_(seasons))
        .order_by(Competition.year, Competition.start_date, Competition.name)
        .all()
    )
    if not rows:
        return pd.DataFrame(
            columns=[
                "competition_id",
                "season",
                "competition_name",
                "start_date",
                "end_date",
            ]
        )
    return pd.DataFrame(
        [
            {
                "competition_id": int(cid),
                "season": str(year),
                "competition_name": (name or "").strip(),
                "start_date": start_date,
                "end_date": end_date,
            }
            for cid, year, name, start_date, end_date in rows
        ]
    )


def _load_element_marks_with_competition(
    session,
    analytics: JudgeAnalytics,
    *,
    start_season_year: str | None,
    end_season_year: str | None,
    discipline_type_ids: list[int],
    segment_level_preset: str,
    competition_scope: str,
) -> pd.DataFrame:
    segment_levels = segment_levels_for_ranking_preset(segment_level_preset)
    where_clause = build_element_mark_filters(
        start_season_year,
        end_season_year,
        discipline_type_ids,
        segment_levels,
    )
    stmt = (
        select(
            ElementScorePerJudge.element_id,
            ElementScorePerJudge.judge_id,
            ElementScorePerJudge.judge_score,
            Segment.discipline_type_id,
            Element.element_type_id,
            Competition.id.label("competition_id"),
            Competition.year.label("competition_year"),
        )
        .select_from(ElementScorePerJudge)
        .join(Element, ElementScorePerJudge.element_id == Element.id)
        .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
    )
    if where_clause is not None:
        stmt = stmt.where(where_clause)
    stmt = analytics._filter_select_competition_scope(stmt, competition_scope)
    from element_deviation_ranking import MIN_ELEMENT_MARKING_EVENT_DATE

    stmt = analytics._apply_competition_event_date_range(
        stmt, MIN_ELEMENT_MARKING_EVENT_DATE, None
    )
    df = pd.read_sql(stmt, session.bind)
    if df.empty:
        return df
    df["element_id"] = pd.to_numeric(df["element_id"], downcast="integer")
    df["judge_id"] = pd.to_numeric(df["judge_id"], downcast="integer")
    df["competition_id"] = pd.to_numeric(df["competition_id"], downcast="integer")
    for col in ("discipline_type_id", "element_type_id"):
        df[col] = pd.to_numeric(df[col], downcast="integer")
    df["judge_score"] = df["judge_score"].astype(np.float32)
    return df


def _judge_summary_rows(result: dict) -> pd.DataFrame:
    js = result.get("judge_summary_all")
    if not isinstance(js, pd.DataFrame) or js.empty:
        return pd.DataFrame()
    return js.copy()


def _fit_pcs_sigma(benchmark_marks: pd.DataFrame, sigma_model: str) -> dict:
    return fit_sigma_params_from_marks(
        benchmark_marks,
        min_bin_count=min_bin_count_for_sigma_model(sigma_model),
        sigma_model=sigma_model,
        floor_sigma=PCS_FLOOR_SIGMA,
    )


def _fit_element_sigma(benchmark_marks: pd.DataFrame) -> dict:
    work = compute_control_scores(benchmark_marks.copy())
    return fit_sigma_discrete(work, min_bin_count=ELEMENT_MIN_BIN_COUNT)


def _cross_judge_stats_by_competition(
    analytics: JudgeAnalytics,
    competition_ids: list[int],
    discipline_type_ids: list[int],
) -> dict[tuple[int, str], dict[str, int | float]]:
    """(competition_id, identity_label) -> scores, anomalies, rule_errors."""
    if not competition_ids or not shard_cache_populated(analytics.session):
        return {}

    judge_id_to_label = analytics.get_judge_id_to_identity_label()
    pcs_raw, elem_raw = _load_shard_judge_aggregates_sql(
        analytics.session,
        competition_ids,
        discipline_type_ids,
        by_competition=True,
    )
    pcs_merged = analytics._merge_competition_judge_stat_dicts_by_identity(
        pcs_raw, judge_id_to_label
    )
    elem_merged = analytics._merge_competition_judge_stat_dicts_by_identity(
        elem_raw, judge_id_to_label
    )

    out: dict[tuple[int, str], dict[str, int]] = {}
    for (comp_id, label), stats in pcs_merged.items():
        key = (int(comp_id), str(label))
        bucket = out.setdefault(
            key, {"scores": 0, "anomalies": 0, "rule_errors": 0}
        )
        bucket["scores"] += int(stats.get("total", 0))
        bucket["anomalies"] += int(stats.get("anomalies", 0))
        bucket["rule_errors"] += int(stats.get("rule_errors", 0))
    for (comp_id, label), stats in elem_merged.items():
        key = (int(comp_id), str(label))
        bucket = out.setdefault(
            key, {"scores": 0, "anomalies": 0, "rule_errors": 0}
        )
        bucket["scores"] += int(stats.get("total", 0))
        bucket["anomalies"] += int(stats.get("anomalies", 0))
        bucket["rule_errors"] += int(stats.get("rule_errors", 0))
    return out


def _anomaly_rate_pct(anomalies: int, scores: int) -> float | None:
    if scores <= 0:
        return None
    return round((anomalies / scores) * 100, 2)


def build_report(
    analytics: JudgeAnalytics,
    *,
    seasons: list[str],
    discipline_type_ids: list[int],
    segment_level_preset: str,
    sigma_model: str,
    min_marks: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    sigma_model = normalize_sigma_model(sigma_model)
    if not seasons:
        raise RuntimeError("No seasons in range.")

    comps = _us_championship_competitions(analytics, seasons)
    if comps.empty:
        raise RuntimeError(
            f"No US Championships (type {US_CHAMPIONSHIPS_COMPETITION_TYPE_ID}) "
            f"found for seasons {seasons[0]}–{seasons[-1]}."
        )

    start_season = min(seasons)
    end_season = max(seasons)
    scope_kw = dict(
        start_season_year=start_season,
        end_season_year=end_season,
        discipline_type_ids=discipline_type_ids,
        competition_scope=COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
        segment_level_preset=segment_level_preset,
    )

    print(
        f"Loading benchmark marks ({COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS}, "
        f"{segment_level_preset}) for σ̂ fit..."
    )
    pcs_benchmark = load_pcs_deviation_marks(analytics, **scope_kw)
    elem_benchmark = _load_element_marks_with_competition(
        analytics.session,
        analytics,
        start_season_year=start_season,
        end_season_year=end_season,
        discipline_type_ids=discipline_type_ids,
        segment_level_preset=segment_level_preset,
        competition_scope=COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    )
    print(
        f"  PCS benchmark marks: {len(pcs_benchmark):,}; "
        f"element benchmark marks: {len(elem_benchmark):,}"
    )

    pcs_params = _fit_pcs_sigma(pcs_benchmark, sigma_model)
    elem_params = _fit_element_sigma(elem_benchmark)

    comp_ids = comps["competition_id"].astype(int).tolist()
    cross_stats = _cross_judge_stats_by_competition(
        analytics, comp_ids, discipline_type_ids
    )
    if cross_stats:
        print(f"Cross-judge shard stats loaded for {len(comp_ids)} championships.")
    else:
        print("Cross-judge shard cache not populated; skipping anomaly/rule-error columns.")

    long_rows: list[dict] = []
    comp_summaries: list[dict] = []

    for comp in comps.itertuples(index=False):
        cid = int(comp.competition_id)
        pcs_marks = pcs_benchmark.loc[pcs_benchmark["competition_id"] == cid]
        elem_marks = elem_benchmark.loc[elem_benchmark["competition_id"] == cid]

        pcs_result = finish_pcs_deviation_rankings_from_marks(
            analytics,
            pcs_marks,
            min_marks=min_marks,
            floor_sigma=PCS_FLOOR_SIGMA,
            min_bin_count=min_bin_count_for_sigma_model(sigma_model),
            params=pcs_params,
            sigma_reference_df=pcs_benchmark,
            sigma_model=sigma_model,
        )
        elem_result = finish_element_deviation_rankings_from_marks(
            analytics,
            elem_marks,
            min_marks=min_marks,
            floor_sigma=ELEMENT_FLOOR_SIGMA,
            min_bin_count=ELEMENT_MIN_BIN_COUNT,
            include_judge_detail=False,
            params=elem_params,
            sigma_reference_df=elem_benchmark,
        )

        pcs_js = _judge_summary_rows(pcs_result)
        elem_js = _judge_summary_rows(elem_result)
        pcs_by_name = (
            pcs_js.set_index("judge_name").to_dict("index") if not pcs_js.empty else {}
        )
        elem_by_name = (
            elem_js.set_index("judge_name").to_dict("index")
            if not elem_js.empty
            else {}
        )
        judge_names = sorted(set(pcs_by_name) | set(elem_by_name), key=str.lower)

        for name in judge_names:
            p = pcs_by_name.get(name, {})
            e = elem_by_name.get(name, {})
            cj = cross_stats.get((cid, name), {})
            scores = int(cj.get("scores", 0))
            anomalies = int(cj.get("anomalies", 0))
            long_rows.append(
                {
                    "competition_id": cid,
                    "season": comp.season,
                    "competition_name": comp.competition_name,
                    "start_date": comp.start_date,
                    "judge_name": name,
                    "pcs_n_marks": int(p.get("n_marks", 0) or 0),
                    "pcs_marking_score": round(float(p["marking_score"]), 4)
                    if p.get("marking_score") is not None
                    else None,
                    "pcs_mean_abs_error": round(float(p["mean_abs_error"]), 4)
                    if p.get("mean_abs_error") is not None
                    else None,
                    "pcs_rank": int(p["rank"]) if p.get("rank") is not None else None,
                    "element_n_marks": int(e.get("n_marks", 0) or 0),
                    "element_marking_score": round(float(e["marking_score"]), 4)
                    if e.get("marking_score") is not None
                    else None,
                    "element_mean_abs_error": round(float(e["mean_abs_error"]), 4)
                    if e.get("mean_abs_error") is not None
                    else None,
                    "element_rank": int(e["rank"]) if e.get("rank") is not None else None,
                    "cross_judge_scores": scores or None,
                    "cross_judge_rule_errors": int(cj.get("rule_errors", 0)) or None,
                    "cross_judge_anomalies": anomalies or None,
                    "cross_judge_anomaly_rate_pct": _anomaly_rate_pct(anomalies, scores),
                }
            )

        def _comp_stats(js: pd.DataFrame, prefix: str) -> dict:
            if js.empty or "marking_score" not in js.columns:
                return {
                    f"{prefix}_judges": 0,
                    f"{prefix}_min": None,
                    f"{prefix}_median": None,
                    f"{prefix}_mean": None,
                    f"{prefix}_max": None,
                    f"{prefix}_std": None,
                }
            s = js["marking_score"].dropna()
            return {
                f"{prefix}_judges": len(s),
                f"{prefix}_min": round(float(s.min()), 4) if len(s) else None,
                f"{prefix}_median": round(float(s.median()), 4) if len(s) else None,
                f"{prefix}_mean": round(float(s.mean()), 4) if len(s) else None,
                f"{prefix}_max": round(float(s.max()), 4) if len(s) else None,
                f"{prefix}_std": round(float(s.std(ddof=0)), 4) if len(s) > 1 else None,
            }

        comp_summaries.append(
            {
                "competition_id": cid,
                "season": comp.season,
                "competition_name": comp.competition_name,
                "start_date": comp.start_date,
                "pcs_raw_marks": len(pcs_marks),
                "element_raw_marks": len(elem_marks),
                "judges_with_any_marks": len(judge_names),
                **_comp_stats(pcs_js, "pcs_marking_score"),
                **_comp_stats(elem_js, "element_marking_score"),
            }
        )
        print(
            f"  {comp.season} {comp.competition_name}: "
            f"{len(judge_names)} judges, {len(pcs_marks):,} PCS / {len(elem_marks):,} element marks"
        )

    long_df = pd.DataFrame(long_rows)
    if not long_df.empty:
        long_df = long_df.sort_values(
            ["season", "competition_name", "pcs_marking_score", "judge_name"],
            ascending=[True, True, True, True],
            na_position="last",
        )

    summary_df = pd.DataFrame(comp_summaries)
    summary_df = summary_df.rename(
        columns={
            "pcs_marking_score_judges": "pcs_judges",
            "element_marking_score_judges": "element_judges",
        }
    )

    return long_df, summary_df


def _write_pivot(long_df: pd.DataFrame, out_path: Path, value_col: str) -> None:
    if long_df.empty or value_col not in long_df.columns:
        return
    pivot = long_df.pivot_table(
        index="judge_name",
        columns="season",
        values=value_col,
        aggfunc="first",
    )
    pivot = pivot.sort_index(key=lambda s: s.str.lower())
    pivot.to_csv(out_path)
    print(f"Wrote pivot ({value_col}) to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-season",
        help="First season code inclusive (e.g. 2223). Default: earliest available.",
    )
    parser.add_argument(
        "--end-season",
        help="Last season code inclusive (e.g. 2526). Default: latest available.",
    )
    parser.add_argument(
        "--discipline-type-id",
        type=int,
        action="append",
        dest="discipline_type_ids",
        help="Segment discipline type id (repeat; default: Singles + Pairs).",
    )
    parser.add_argument(
        "--segment-level-preset",
        default=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
        choices=[ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR],
        help="Segment level filter (default: junior_senior).",
    )
    parser.add_argument(
        "--sigma-model",
        default=PCS_SIGMA_MODEL_QUADRATIC,
        choices=["discrete", "quadratic"],
        help="PCS σ model (default: quadratic, 0.125 bins).",
    )
    parser.add_argument(
        "--min-marks",
        type=int,
        default=0,
        help="Minimum marks per judge within a championship to include in ranking output.",
    )
    parser.add_argument(
        "-o",
        "--output-prefix",
        type=Path,
        default=_REPO / "analysisTemp" / "us_champs_judge_deviations",
        help="Output path prefix (writes _long.csv, _by_competition.csv, pivots).",
    )
    args = parser.parse_args()

    discipline_type_ids = args.discipline_type_ids or list(DEFAULT_SEGMENT_DISCIPLINE_TYPE_IDS)
    prefix: Path = args.output_prefix
    prefix.parent.mkdir(parents=True, exist_ok=True)

    with get_db_session() as session:
        analytics = JudgeAnalytics(session)
        seasons = _seasons_in_range(analytics, args.start_season, args.end_season)
        print(f"Seasons: {seasons[0]}–{seasons[-1]} ({len(seasons)} seasons)")
        long_df, summary_df = build_report(
            analytics,
            seasons=seasons,
            discipline_type_ids=discipline_type_ids,
            segment_level_preset=args.segment_level_preset,
            sigma_model=args.sigma_model,
            min_marks=args.min_marks,
        )

    long_path = prefix.with_name(prefix.name + "_long.csv")
    summary_path = prefix.with_name(prefix.name + "_by_competition.csv")
    long_df.to_csv(long_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    print(f"\nWrote {len(long_df)} judge×competition rows to {long_path}")
    print(f"Wrote {len(summary_df)} competition summaries to {summary_path}")

    _write_pivot(
        long_df,
        prefix.with_name(prefix.name + "_pcs_marking_pivot.csv"),
        "pcs_marking_score",
    )
    _write_pivot(
        long_df,
        prefix.with_name(prefix.name + "_element_marking_pivot.csv"),
        "element_marking_score",
    )


if __name__ == "__main__":
    main()
