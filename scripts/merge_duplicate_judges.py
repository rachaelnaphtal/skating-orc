#!/usr/bin/env python3
"""
Merge duplicate ``public.judge`` rows into each group's canonical id.

Uses the same canonical pick as ``report_duplicate_judges.py`` / ``_ensure_judges_by_name``
(most element+PCS scores, then lowest id).

Dry-run by default (counts only). Pass ``--execute`` to apply.

  python scripts/merge_duplicate_judges.py
  python scripts/merge_duplicate_judges.py --match-key "karen wolanchuk"
  python scripts/merge_duplicate_judges.py --keeper-id 341 --duplicate-id 2784 --execute
  python scripts/merge_duplicate_judges.py --execute
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from database import ensure_database_for_streamlit, get_database_url, get_db_session
from judge_merge import format_merge_stats, plan_duplicate_judge_merges


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply merges (default is dry-run).",
    )
    parser.add_argument(
        "--match-key",
        default="",
        help="Only merge this normalized name key (e.g. karen wolanchuk).",
    )
    parser.add_argument("--keeper-id", type=int, default=None)
    parser.add_argument("--duplicate-id", type=int, default=None)
    args = parser.parse_args()

    if (args.keeper_id is None) ^ (args.duplicate_id is None):
        print("Use both --keeper-id and --duplicate-id together.", file=sys.stderr)
        return 2

    ensure_database_for_streamlit()
    db_url = get_database_url()
    host_hint = db_url.split("@")[-1].split("/")[0] if "@" in db_url else "(local)"
    dry_run = not args.execute
    print(f"Database host: {host_hint}", flush=True)
    print(f"Mode: {'dry-run' if dry_run else 'EXECUTE'}", flush=True)

    session = get_db_session()
    try:
        plan = plan_duplicate_judge_merges(
            session,
            match_key=args.match_key or None,
            keeper_id=args.keeper_id,
            duplicate_id=args.duplicate_id,
            dry_run=dry_run,
        )
        if not plan.merges:
            print("No duplicate judge merges planned.", flush=True)
            return 0

        total_elem_repoint = 0
        total_elem_drop = 0
        total_pcs_repoint = 0
        total_pcs_drop = 0
        for stats in plan.merges:
            print(format_merge_stats(stats), flush=True)
            total_elem_repoint += stats.repointed_element_scores
            total_elem_drop += stats.deleted_element_scores
            total_pcs_repoint += stats.repointed_pcs_scores
            total_pcs_drop += stats.deleted_pcs_scores

        print(
            f"\nPlanned {plan.total_merges} merge(s): "
            f"elem repoint={total_elem_repoint} drop={total_elem_drop}, "
            f"pcs repoint={total_pcs_repoint} drop={total_pcs_drop}",
            flush=True,
        )
        if dry_run:
            print("Re-run with --execute to apply.", flush=True)
        else:
            session.commit()
            print("Committed.", flush=True)
        return 0
    except Exception:
        session.rollback()
        raise


if __name__ == "__main__":
    raise SystemExit(main())
