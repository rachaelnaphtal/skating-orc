#!/usr/bin/env python3
"""
Load an international judge availability Excel export into ``officials_analysis``
international availability tables.

Apply migration ``activityAnalysis/migrations/041_international_availability_form.sql``
on PostgreSQL before the first run.

Example:

    python scripts/load_international_availability_workbook.py \\
        activityAnalysis/InternationalAvailability20262.xlsx
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from activityAnalysis.international_availability_store import (
    DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL,
    load_international_availability_form_workbook,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load international availability Excel into international_* tables."
    )
    parser.add_argument("excel_path", help="Path to .xlsx export")
    parser.add_argument(
        "--label",
        default=DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL,
        help="Form label stored in DB (default: 2026-27 International Judge Availability)",
    )
    parser.add_argument(
        "--sheet",
        default=None,
        help="Worksheet name (default: original, else first sheet)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not commit (session rolls back on exit)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.excel_path):
        print(f"File not found: {args.excel_path}", file=sys.stderr)
        sys.exit(1)

    summary = load_international_availability_form_workbook(
        args.excel_path,
        label=args.label,
        sheet_name=args.sheet,
        commit=not args.dry_run,
    )
    print(json.dumps(summary, indent=2, default=str))
    if args.dry_run:
        print("\nDry run: changes were not committed.", file=sys.stderr)


if __name__ == "__main__":
    main()
