#!/usr/bin/env python3
"""
Precompute PCS quality shard caches (per season × discipline).

Shards are stored in ``pcs_quality_shard_cache``. Re-run after loading new
competitions (or rely on downloadResults invalidation).

Example::

    python scripts/precompute_pcs_quality_cache.py
    python scripts/precompute_pcs_quality_cache.py --all-scopes
    python scripts/precompute_pcs_quality_cache.py --scope qualifying --season 2425
    python scripts/precompute_pcs_quality_cache.py --all-scopes --summaries
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
from officials_competition_types import (
    ALL_COMPETITION_SCOPES,
    COMPETITION_SCOPE_ALL,
)
from pcs_quality_analysis import filter_pcs_quality_season_years
from pcs_quality_cache import (
    precompute_pcs_quality_shards,
    precompute_pcs_quality_summaries,
)


def _season_years_for_run(
    analytics: JudgeAnalytics, season: str | None
) -> list[str]:
    years = filter_pcs_quality_season_years(analytics.get_years())
    if season:
        if season not in years:
            raise ValueError(f"Season {season} not in database.")
        return [season]
    return years


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Precompute PCS quality shards (season × discipline)"
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
        help="Precompute every competition scope: " + ", ".join(ALL_COMPETITION_SCOPES),
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Single season year (e.g. 2425). Default: all seasons from 2223 onward.",
    )
    parser.add_argument(
        "--summaries",
        action="store_true",
        help=(
            "Also warm per-shard mergeable component summaries "
            "(requires mark shards; faster cache-only PCS loads)."
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
        total_summaries = 0
        for scope in scopes:
            print(f"\n=== scope={scope} ({len(years)} season(s)) ===")
            n = precompute_pcs_quality_shards(
                session,
                analytics,
                competition_scope=scope,
                season_years=years,
            )
            total_shards += n
            if args.summaries:
                n_sum = precompute_pcs_quality_summaries(
                    session,
                    analytics,
                    competition_scope=scope,
                    season_years=years,
                )
                total_summaries += n_sum
                print(f"  summary shard rows written: {n_sum}")

        print(f"\nDone. {total_shards} shard(s) across {len(scopes)} scope(s).")
        if total_summaries:
            print(f"Summary rows written: {total_summaries}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
