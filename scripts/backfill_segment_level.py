#!/usr/bin/env python3
"""
Backfill ``public.segment.level`` and ``level_source`` for existing rows.

  python scripts/backfill_segment_level.py
  python scripts/backfill_segment_level.py --dry-run
  python scripts/backfill_segment_level.py --competition-id 42
  python scripts/backfill_segment_level.py --only-unspecified
  python scripts/backfill_segment_level.py --only-unspecified --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import or_

from database import ensure_database_for_streamlit, get_database_url, get_db_session
from models import Competition, Segment
from segment_level import (
    LEVEL_UNSPECIFIED,
    SOURCE_COMPETITION_NAME,
    SOURCE_DEFAULT_INTERNATIONAL,
    classify_segment_level_for_row,
)

_BATCH_COMMIT = 500


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--competition-id", type=int, default=None)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_BATCH_COMMIT,
        help=f"Commit every N changed rows (default {_BATCH_COMMIT}).",
    )
    parser.add_argument(
        "--only-unspecified",
        action="store_true",
        help=f"Only rows where level is NULL, empty, or {LEVEL_UNSPECIFIED!r}.",
    )
    args = parser.parse_args()

    ensure_database_for_streamlit()
    db_url = get_database_url()
    host_hint = db_url.split("@")[-1].split("/")[0] if "@" in db_url else "(local)"
    print(f"Database host: {host_hint}", flush=True)

    session = get_db_session()
    try:
        q = session.query(Segment, Competition).join(
            Competition, Segment.competition_id == Competition.id
        )
        if args.competition_id is not None:
            q = q.filter(Segment.competition_id == args.competition_id)
        if args.only_unspecified:
            q = q.filter(
                or_(
                    Segment.level.is_(None),
                    Segment.level == "",
                    Segment.level == LEVEL_UNSPECIFIED,
                )
            )

        scope = "segments"
        if args.only_unspecified:
            scope = "segments with unspecified level"
        if args.competition_id is not None:
            scope = f"{scope} for competition_id={args.competition_id}"
        print(f"Loading {scope}…", flush=True)
        rows = q.all()
        print(f"Loaded {len(rows)} segment row(s).", flush=True)

        n = 0
        fallback = 0
        pending = 0
        for i, (seg, comp) in enumerate(rows, start=1):
            result = classify_segment_level_for_row(
                seg.name,
                comp.name,
                comp.international,
                discipline_type_id=seg.discipline_type_id,
            )
            if result.source in (SOURCE_COMPETITION_NAME, SOURCE_DEFAULT_INTERNATIONAL):
                fallback += 1
            if seg.level == result.level and seg.level_source == result.source:
                continue
            n += 1
            if not args.dry_run:
                seg.level = result.level
                seg.level_source = result.source
                pending += 1
                if pending >= args.batch_size:
                    print(f"  committing {pending} change(s)…", flush=True)
                    session.commit()
                    pending = 0

            if i % 5000 == 0:
                print(f"  processed {i}/{len(rows)}…", flush=True)

        if args.dry_run:
            print(
                f"Would update {n} segment(s); {fallback} use competition/default inference.",
                flush=True,
            )
        else:
            if pending:
                print(f"  committing final {pending} change(s)…", flush=True)
                session.commit()
            print(
                f"Updated {n} segment(s); {fallback} use competition/default inference.",
                flush=True,
            )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
