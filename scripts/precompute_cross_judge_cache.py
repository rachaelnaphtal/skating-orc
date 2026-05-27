#!/usr/bin/env python3
"""
Precompute per-competition cross-judge benchmarking shards.

Shards are stored in ``cross_judge_competition_shard``. By default, competitions
that already have shard rows are skipped; use ``--force`` to rebuild everything.
After bulk DB loads, run without ``--force`` to fill only missing competitions.

Example::

    python scripts/precompute_cross_judge_cache.py
    python scripts/precompute_cross_judge_cache.py --force
    python scripts/precompute_cross_judge_cache.py --competition-id 42
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from cross_judge_cache import (
    build_cross_judge_shards_for_competition,
    competition_has_shard_cache,
    ensure_cross_judge_cache_tables,
    iter_competitions_for_precompute,
    precompute_cross_judge_shards,
)
from database import get_db_session


def _print_progress(
    index: int,
    total: int,
    competition_id: int,
    name: str,
    year: str,
    n_rows: int,
    status: str,
) -> None:
    label = f"{name} ({year})"
    if status == "skipped":
        print(f"[{index}/{total}] skip id={competition_id} {label} (already cached)")
    else:
        print(f"[{index}/{total}] built id={competition_id} {label}: {n_rows} shard rows")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Precompute cross-judge competition shards."
    )
    parser.add_argument(
        "--competition-id",
        type=int,
        action="append",
        dest="competition_ids",
        help="Rebuild only these competition ids (repeatable).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild shards even when this competition is already cached.",
    )
    args = parser.parse_args()

    skip_cached = not args.force

    with get_db_session() as session:
        bind = session.get_bind()
        ensure_cross_judge_cache_tables(bind)

        if args.competition_ids:
            targets = iter_competitions_for_precompute(
                session, args.competition_ids
            )
            if len(targets) < len(args.competition_ids):
                found = {t[0] for t in targets}
                missing = [c for c in args.competition_ids if c not in found]
                print(
                    f"Warning: competition id(s) not in database: {missing}",
                    file=sys.stderr,
                )
            n = len(targets)
            print(
                f"Processing {n} competition(s)"
                + (" (skip if cached)" if skip_cached else " (force rebuild)")
            )
            built = skipped = rows_written = 0
            for i, (cid, name, year) in enumerate(targets, start=1):
                if skip_cached and competition_has_shard_cache(session, cid):
                    skipped += 1
                    _print_progress(i, n, cid, name, year, 0, "skipped")
                    continue
                n_rows = build_cross_judge_shards_for_competition(session, cid)
                session.commit()
                built += 1
                rows_written += n_rows
                _print_progress(i, n, cid, name, year, n_rows, "built")
            print(
                f"Done. {built} built, {skipped} skipped, "
                f"{rows_written} shard rows written."
            )
            return

        mode = "skip competitions already cached" if skip_cached else "force rebuild all"
        print(f"Precomputing cross-judge shards ({mode})…")
        rows_written, built, skipped = precompute_cross_judge_shards(
            session,
            skip_cached=skip_cached,
            on_progress=_print_progress,
        )
        print(
            f"Done. {built} built, {skipped} skipped, "
            f"{rows_written} shard rows written."
        )


if __name__ == "__main__":
    main()
