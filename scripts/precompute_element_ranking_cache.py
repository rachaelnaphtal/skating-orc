#!/usr/bin/env python3
"""
Precompute element deviation ranking shard caches (per season × discipline).

Shards are stored in ``element_deviation_ranking_shard_cache``. A UI run for a
multi-season or multi-discipline window concatenates matching shards, then fits σ̂
once. Re-run after loading new competitions (or rely on downloadResults invalidation).

Example::

    python scripts/precompute_element_ranking_cache.py
    python scripts/precompute_element_ranking_cache.py --scope qualifying --season 2425
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
from officials_competition_types import COMPETITION_SCOPE_ALL


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Precompute element ranking shards (season × discipline)"
    )
    parser.add_argument(
        "--scope",
        default=COMPETITION_SCOPE_ALL,
        help="Competition scope key (default: all)",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Single season year (e.g. 2425). Default: all seasons from 1819 onward.",
    )
    parser.add_argument(
        "--sigma-benchmark",
        action="store_true",
        help="Also warm σ̂ cache for the widest season window (all disciplines).",
    )
    args = parser.parse_args(argv)

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        years = filter_element_ranking_season_years(analytics.get_years())
        if args.season:
            if args.season not in years:
                print(f"Season {args.season} not in database.", file=sys.stderr)
                return 1
            years = [args.season]

        print(f"Precomputing {len(years)} season(s), scope={args.scope} …")
        n = precompute_element_ranking_shards(
            session,
            analytics,
            competition_scope=args.scope,
            season_years=years,
        )
        print(f"Done. {n} shard(s). memory_efficient={memory_efficient_mode()}")
        if args.sigma_benchmark:
            run_params = (
                None,
                None,
                None,
                args.scope,
                None,
                None,
                0,
                float(FLOOR_SIGMA),
                int(MIN_BIN_COUNT),
                None,
                None,
                COMPETITION_SCOPE_ALL,
            )
            key = precompute_element_ranking_sigma(session, analytics, run_params)
            if key:
                print(f"σ̂ benchmark cache: {key} (all seasons, scope=all)")
            else:
                print("σ̂ benchmark cache: skipped (no marks)", file=sys.stderr)
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
