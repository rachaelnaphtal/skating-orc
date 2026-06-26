#!/usr/bin/env python3
"""
Flag PCS fall rule errors from stored element.notes (no protocol re-fetch).

Uses set-based SQL (same logic as ``pcs_fall_rule_errors_examples.sql``) by default.
The old per-segment loop is available with ``--per-segment`` for debugging.

  python scripts/backfill_pcs_fall_rule_errors.py --dry-run
  python scripts/backfill_pcs_fall_rule_errors.py --competition-id 42
  python scripts/backfill_pcs_fall_rule_errors.py --segment-id 1001
  python scripts/backfill_pcs_fall_rule_errors.py --year 2526
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from database import ensure_database_for_streamlit, get_db_session
from database_loader import DatabaseLoader
from models import Competition, Segment
from rule_errors_policy import (
    MIN_PCS_FALL_RULE_ERROR_SEASON_YEAR,
    PCS_FALL_RULE_DISCIPLINE_TYPE_IDS,
)


def _build_query(session, args):
    q = (
        session.query(Segment, Competition)
        .join(Competition, Segment.competition_id == Competition.id)
        .filter(
            Segment.discipline_type_id.in_(sorted(PCS_FALL_RULE_DISCIPLINE_TYPE_IDS))
        )
    )
    if not args.all_seasons:
        q = q.filter(Competition.year >= MIN_PCS_FALL_RULE_ERROR_SEASON_YEAR)
    if args.competition_id is not None:
        q = q.filter(Competition.id == args.competition_id)
    if args.segment_id is not None:
        q = q.filter(Segment.id == args.segment_id)
    if args.year is not None:
        q = q.filter(Competition.year == str(args.year))
    return q.order_by(Competition.id, Segment.id)


def _run_per_segment(loader, session, rows, *, dry_run: bool) -> int:
    total_flagged = 0
    for segment, competition in rows:
        if dry_run:
            flagged = loader._count_pcs_fall_rule_errors_for_segment(segment.id)
        else:
            result = loader.refresh_pcs_fall_rule_errors_from_db(segment.id)
            flagged = int(result["flagged"])
        total_flagged += flagged
        if flagged:
            print(
                f"segment {segment.id} ({competition.name!r} / {segment.name!r}): "
                f"{flagged} PCS fall rule error(s)"
            )
    if not dry_run:
        session.commit()
    return total_flagged


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--per-segment", action="store_true", help="Slow debug path")
    parser.add_argument(
        "--all-seasons",
        action="store_true",
        help="Include pre-2425 seasons in per-segment mode only",
    )
    parser.add_argument("--competition-id", type=int, default=None)
    parser.add_argument("--segment-id", type=int, default=None)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()

    ensure_database_for_streamlit()
    session = get_db_session()
    loader = DatabaseLoader(session)

    if args.per_segment:
        q = _build_query(session, args)
        if args.offset:
            q = q.offset(args.offset)
        if args.limit is not None:
            q = q.limit(args.limit)
        rows = q.all()
        total_flagged = _run_per_segment(loader, session, rows, dry_run=args.dry_run)
        print(
            f"Done (per-segment). {total_flagged} PCS fall rule error(s) "
            f"across {len(rows)} segment(s)."
        )
        return 0

    if args.dry_run:
        flagged = loader.count_pcs_fall_rule_errors_bulk(
            competition_id=args.competition_id,
            segment_id=args.segment_id,
            season_year=args.year,
        )
        print(f"Dry run: {flagged} PCS fall violation(s) in scope.")
        return 0

    result = loader.refresh_pcs_fall_rule_errors_bulk(
        competition_id=args.competition_id,
        segment_id=args.segment_id,
        season_year=args.year,
        clear_pre_season=(
            args.competition_id is None
            and args.segment_id is None
            and args.year is None
        ),
    )
    print(
        "Done. "
        f"cleared={result['cleared']} "
        f"flagged={result['flagged']} "
        f"pre_season_cleared={result['pre_season_cleared']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
