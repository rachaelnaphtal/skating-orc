#!/usr/bin/env python3
"""
Precompute element deviation ranking shard caches (per season × discipline).

Shards are stored in ``element_deviation_ranking_shard_cache``. A UI run for a
multi-season or multi-discipline window concatenates matching shards, then fits σ̂
once. Re-run after loading new competitions (or rely on downloadResults invalidation).

Example::

    python scripts/precompute_element_ranking_cache.py
    python scripts/precompute_element_ranking_cache.py --all-scopes
    python scripts/precompute_element_ranking_cache.py --scope qualifying --season 2425
    python scripts/precompute_element_ranking_cache.py --all-scopes --sigma-benchmark
    python scripts/precompute_element_ranking_cache.py --all-scopes --sigma-benchmark --summaries
    python scripts/precompute_element_ranking_cache.py --all-scopes --all-segment-levels --sigma-benchmark --summaries
    python scripts/precompute_element_ranking_cache.py --segment-levels junior_senior novice_junior_senior
    python scripts/precompute_element_ranking_cache.py --scope international --segment-levels junior_senior --skip-unchanged
    python scripts/precompute_element_ranking_cache.py --scope international --segment-levels junior_senior --summaries-only --sigma-benchmark
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from analytics import JudgeAnalytics
from database import get_db_session
from element_deviation_ranking import (
    ELEMENT_RANKING_LEVEL_FILTER_ALL,
    ELEMENT_RANKING_LEVEL_FILTER_LABELS,
    ELEMENT_RANKING_LEVEL_FILTER_PRESETS,
    filter_element_ranking_season_years,
    memory_efficient_mode,
)
from element_ranking_cache import (
    build_precompute_element_ranking_run_params,
    precompute_element_ranking_shard_summaries,
    precompute_element_ranking_shards,
    precompute_element_ranking_sigma,
)
from officials_competition_types import (
    ALL_COMPETITION_SCOPES,
    COMPETITION_SCOPE_ALL,
)


def _season_years_for_run(
    analytics: JudgeAnalytics, season: str | None
) -> list[str]:
    years = filter_element_ranking_season_years(analytics.get_years())
    if season:
        if season not in years:
            raise ValueError(f"Season {season} not in database.")
        return [season]
    return years


def _segment_level_preset_arg(preset: str) -> str | None:
    if preset == ELEMENT_RANKING_LEVEL_FILTER_ALL:
        return None
    return preset


def _segment_level_presets_for_args(
    *,
    segment_levels: list[str] | None,
    all_segment_levels: bool,
) -> list[str]:
    if all_segment_levels:
        return list(ELEMENT_RANKING_LEVEL_FILTER_PRESETS)
    if segment_levels:
        return segment_levels
    return [ELEMENT_RANKING_LEVEL_FILTER_ALL]


def _precompute_scope(
    session,
    analytics: JudgeAnalytics,
    *,
    competition_scope: str,
    years: list[str],
    segment_level_preset: str | None,
    sigma_benchmark: bool,
    warm_summaries: bool,
    skip_unchanged: bool,
    summaries_only: bool,
) -> tuple[int, int, str | None, int]:
    """Warm shards (and optional σ̂ / summary rows) for one competition scope."""
    level_label = ELEMENT_RANKING_LEVEL_FILTER_LABELS.get(
        segment_level_preset or ELEMENT_RANKING_LEVEL_FILTER_ALL,
        segment_level_preset or "all",
    )
    print(
        f"\n=== scope={competition_scope}, levels={level_label} "
        f"({len(years)} season(s)) ==="
    )
    written = 0
    skipped = 0
    if not summaries_only:
        written, skipped = precompute_element_ranking_shards(
            session,
            analytics,
            competition_scope=competition_scope,
            season_years=years,
            segment_level_preset=segment_level_preset,
            skip_unchanged=skip_unchanged,
        )
        if skipped:
            print(f"  shards skipped (unchanged): {skipped}")
    else:
        print("  shard pass: skipped (--summaries-only)")
    sigma_key = None
    n_summaries = 0
    if sigma_benchmark:
        run_params = build_precompute_element_ranking_run_params(
            competition_scope,
            segment_level_preset=segment_level_preset,
        )
        if summaries_only:
            if warm_summaries:
                n_summaries = precompute_element_ranking_shard_summaries(
                    session,
                    analytics,
                    run_params,
                    summaries_only=True,
                )
                if n_summaries:
                    sigma_key = "cached"
                    print(f"  summary shard rows written: {n_summaries}")
                else:
                    print(
                        "  summary rebuild: skipped (missing σ̂ or mark shards)",
                        file=sys.stderr,
                    )
        else:
            sigma_key = precompute_element_ranking_sigma(session, analytics, run_params)
            if sigma_key:
                print(f"  σ̂ benchmark cache: {sigma_key}")
            else:
                print("  σ̂ benchmark cache: skipped (no marks)", file=sys.stderr)
            if warm_summaries and sigma_key:
                n_summaries = precompute_element_ranking_shard_summaries(
                    session, analytics, run_params
                )
                print(f"  summary shard rows written: {n_summaries}")
    return written, skipped, sigma_key, n_summaries


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Precompute element ranking shards (season × discipline)"
    )
    scope_group = parser.add_mutually_exclusive_group()
    scope_group.add_argument(
        "--scope",
        default=COMPETITION_SCOPE_ALL,
        help=f"Single competition scope key (default: {COMPETITION_SCOPE_ALL})",
    )
    scope_group.add_argument(
        "--all-scopes",
        action="store_true",
        help=(
            "Precompute every competition scope: "
            + ", ".join(ALL_COMPETITION_SCOPES)
        ),
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Single season year (e.g. 2425). Default: all seasons from 1819 onward.",
    )
    parser.add_argument(
        "--segment-levels",
        nargs="+",
        choices=list(ELEMENT_RANKING_LEVEL_FILTER_PRESETS),
        metavar="PRESET",
        help=(
            "Segment level preset(s) to warm. Choices: "
            + ", ".join(ELEMENT_RANKING_LEVEL_FILTER_PRESETS)
            + f" (default: {ELEMENT_RANKING_LEVEL_FILTER_ALL} only)."
        ),
    )
    parser.add_argument(
        "--all-segment-levels",
        action="store_true",
        help=(
            "Warm every segment level preset (all, novice_junior_senior, junior_senior). "
            "Overrides --segment-levels."
        ),
    )
    parser.add_argument(
        "--sigma-benchmark",
        action="store_true",
        help=(
            "Also warm σ̂ cache per scope (all seasons, same scope as benchmark pool)."
        ),
    )
    parser.add_argument(
        "--summaries",
        action="store_true",
        help=(
            "With --sigma-benchmark, also warm per-shard mergeable judge summaries "
            "(faster cache-only ranking loads)."
        ),
    )
    parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        help=(
            "Skip shard DB reloads when the cached fingerprint still matches "
            "(faster routine precompute)."
        ),
    )
    parser.add_argument(
        "--summaries-only",
        action="store_true",
        help=(
            "Rebuild per-shard summary cache from existing mark shards only; "
            "requires --sigma-benchmark (and usually --summaries). Does not "
            "reload mark shards from the database."
        ),
    )
    args = parser.parse_args(argv)

    scopes = list(ALL_COMPETITION_SCOPES) if args.all_scopes else [args.scope]
    if not args.all_scopes and args.scope not in ALL_COMPETITION_SCOPES:
        print(
            f"Unknown scope {args.scope!r}. Choose one of: {', '.join(ALL_COMPETITION_SCOPES)}",
            file=sys.stderr,
        )
        return 1

    if args.segment_levels and args.all_segment_levels:
        print("Use --segment-levels or --all-segment-levels, not both.", file=sys.stderr)
        return 1

    level_presets = _segment_level_presets_for_args(
        segment_levels=args.segment_levels,
        all_segment_levels=args.all_segment_levels,
    )

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        try:
            years = _season_years_for_run(analytics, args.season)
        except ValueError as exc:
            print(exc, file=sys.stderr)
            return 1

        if args.summaries and not args.sigma_benchmark:
            print("--summaries requires --sigma-benchmark", file=sys.stderr)
            return 1
        if args.summaries_only and not args.sigma_benchmark:
            print("--summaries-only requires --sigma-benchmark", file=sys.stderr)
            return 1
        if args.summaries_only and not args.summaries:
            print(
                "Note: --summaries-only implies summary rebuild; "
                "pass --summaries to silence this.",
                file=sys.stderr,
            )
        if args.summaries_only and args.skip_unchanged:
            print("Note: --skip-unchanged has no effect with --summaries-only.", file=sys.stderr)

        total_shards = 0
        total_skipped = 0
        total_summaries = 0
        sigma_keys: list[str] = []
        warm_summaries = args.summaries or args.summaries_only
        for scope in scopes:
            for preset in level_presets:
                written, skipped, sigma_key, n_summaries = _precompute_scope(
                    session,
                    analytics,
                    competition_scope=scope,
                    years=years,
                    segment_level_preset=_segment_level_preset_arg(preset),
                    sigma_benchmark=args.sigma_benchmark,
                    warm_summaries=warm_summaries,
                    skip_unchanged=args.skip_unchanged,
                    summaries_only=args.summaries_only,
                )
                total_shards += written
                total_skipped += skipped
                total_summaries += n_summaries
                if sigma_key:
                    sigma_keys.append(sigma_key)

        print(
            f"\nDone. {total_shards} shard(s) written"
            + (f", {total_skipped} skipped (unchanged)" if total_skipped else "")
            + f" across {len(scopes)} scope(s), "
            f"{len(level_presets)} level preset(s). "
            f"memory_efficient={memory_efficient_mode()}"
        )
        if sigma_keys:
            print(f"σ̂ keys: {', '.join(sigma_keys)}")
        if total_summaries:
            print(f"Summary rows written: {total_summaries}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
