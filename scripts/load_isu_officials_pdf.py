#!/usr/bin/env python3
"""
Load ISU Communication official list PDF into ``officials_analysis.isu_official``.

Example:
    python scripts/load_isu_officials_pdf.py path/to/List-Officials-....pdf \\
        --season 2526 --communication-ref 2735

Requires migration 013 and DATABASE_URL.
"""

from __future__ import annotations

import argparse
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from sqlalchemy import text

from isu_official_pdf_parse import extract_lines_from_pdf, parse_isu_official_pdf_lines
import judge_official_link_core as jol_core


def _upsert_rows(
    engine,
    rows,
    *,
    season: str,
    communication_ref: str | None,
    dry_run: bool,
) -> tuple[int, int]:
    inserted = 0
    updated = 0
    sql = text(
        """
        INSERT INTO officials_analysis.isu_official (
            federation_code, full_name, first_name, last_name,
            name_normalized, season, communication_ref
        )
        VALUES (
            :fed, :full, :first, :last, :norm, :season, :cref
        )
        ON CONFLICT ON CONSTRAINT isu_official_roster_unique
        DO UPDATE SET
            full_name = EXCLUDED.full_name,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            communication_ref = COALESCE(EXCLUDED.communication_ref, isu_official.communication_ref),
            last_modified = NOW()
        RETURNING (xmax = 0) AS inserted
        """
    )
    if dry_run:
        return len(rows), 0

    eng = engine
    with eng.begin() as conn:
        for r in rows:
            result = conn.execute(
                sql,
                {
                    "fed": r.federation_code,
                    "full": r.full_name,
                    "first": r.first_name,
                    "last": r.last_name,
                    "norm": r.name_normalized,
                    "season": season,
                    "cref": communication_ref,
                },
            ).first()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
    return inserted, updated


def main() -> None:
    p = argparse.ArgumentParser(description="Load ISU officials PDF into isu_official.")
    p.add_argument("pdf_path", help="Path to ISU Communication PDF")
    p.add_argument("--season", required=True, help="Season key, e.g. 2526")
    p.add_argument("--communication-ref", default=None, help="e.g. 2735")
    p.add_argument("--dry-run", action="store_true", help="Parse only; do not write DB")
    args = p.parse_args()

    lines = extract_lines_from_pdf(args.pdf_path)
    rows = parse_isu_official_pdf_lines(lines)
    print(f"Parsed {len(rows)} unique officials from {args.pdf_path!r}")

    if args.dry_run:
        for r in rows[:15]:
            print(f"  {r.federation_code}: {r.full_name!r} ({r.name_normalized})")
        if len(rows) > 15:
            print(f"  ... and {len(rows) - 15} more")
        return

    engine = jol_core.make_engine()
    jol_core.ensure_table(engine)
    ins, upd = _upsert_rows(
        engine,
        rows,
        season=args.season.strip(),
        communication_ref=(args.communication_ref or "").strip() or None,
        dry_run=False,
    )
    print(f"Done: inserted={ins}, updated={upd}")


if __name__ == "__main__":
    main()
