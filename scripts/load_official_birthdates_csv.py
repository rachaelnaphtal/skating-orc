#!/usr/bin/env python3
"""
Load date of birth from a USFS ages / directory CSV into ``officials_analysis.officials``.

Only updates officials that already exist (matched on ``mbr_number`` / Member #).

Apply migration ``activityAnalysis/migrations/027_officials_date_of_birth.sql`` first.

    export DATABASE_URL='postgresql://...'
    python scripts/load_official_birthdates_csv.py "/path/to/ISU Judges Analysis - Ages.csv"
    python scripts/load_official_birthdates_csv.py ages.csv --dry-run
"""

from __future__ import annotations

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from activityAnalysis.official_birthdate_loader import load_official_birthdates_from_csv


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Load official date_of_birth from USFS ages CSV (existing officials only)."
    )
    parser.add_argument("csv_path", help="Path to ages CSV export")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report match/update counts without writing",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.csv_path):
        print(f"File not found: {args.csv_path}", file=sys.stderr)
        sys.exit(1)

    try:
        load_official_birthdates_from_csv(args.csv_path, dry_run=args.dry_run)
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
