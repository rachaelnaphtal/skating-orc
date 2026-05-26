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
    FLOOR_SIGMA,
    MIN_BIN_COUNT,
    filter_element_ranking_season_years,
    memory_efficient_mode,
)
from element_ranking_cache import (
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


def _precompute_scope(
    session,
    analytics: JudgeAnalytics,
    *,
    competition_scope: str,
    years: list[str],
    sigma_benchmark: bool,
) -> tuple[int, str | None]:
    """Warm shards (and optional σ̂ row) for one competition scope."""
    print(f"\n=== scope={competition_scope} ({len(years)} season(s)) ===")
    n = precompute_element_ranking_shards(
        session,
        analytics,
        competition_scope=competition_scope,
        season_years=years,
    )
    sigma_key = None
    if sigma_benchmark:
        run_params = (
            None,
            None,
            None,
            competition_scope,
            None,
            None,
            0,
            float(FLOOR_SIGMA),
            int(MIN_BIN_COUNT),
            None,
            None,
            competition_scope,
        )
        sigma_key = precompute_element_ranking_sigma(session, analytics, run_params)
        if sigma_key:
            print(f"  σ̂ benchmark cache: {sigma_key}")
        else:
            print("  σ̂ benchmark cache: skipped (no marks)", file=sys.stderr)
    return n, sigma_key


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
        "--sigma-benchmark",
        action="store_true",
        help=(
            "Also warm σ̂ cache per scope (all seasons, same scope as benchmark pool)."
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

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        try:
            years = _season_years_for_run(analytics, args.season)
        except ValueError as exc:
            print(exc, file=sys.stderr)
            return 1

        total_shards = 0
        sigma_keys: list[str] = []
        for scope in scopes:
            n, sigma_key = _precompute_scope(
                session,
                analytics,
                competition_scope=scope,
                years=years,
                sigma_benchmark=args.sigma_benchmark,
            )
            total_shards += n
            if sigma_key:
                sigma_keys.append(sigma_key)

        print(
            f"\nDone. {total_shards} shard(s) across {len(scopes)} scope(s). "
            f"memory_efficient={memory_efficient_mode()}"
        )
        if sigma_keys:
            print(f"σ̂ keys: {', '.join(sigma_keys)}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
