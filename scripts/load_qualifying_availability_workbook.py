#!/usr/bin/env python3
"""
Load a qualifying-season availability Excel export into ``officials_analysis`` tables
``official_qualifying_availability`` and ``official_qualifying_supplemental``.

Uses the same ``DATABASE_URL`` as the activity tracker (see ``load_activity_data``).
Apply migration ``activityAnalysis/migrations/007_official_qualifying_tables.sql`` on
PostgreSQL before the first run.

Example:

    python scripts/load_qualifying_availability_workbook.py path/to/responses.xlsx

Optional flags:

    python scripts/load_qualifying_availability_workbook.py file.xlsx --sheet original
    python scripts/load_qualifying_availability_workbook.py file.xlsx --allow-missing-status
    python scripts/load_qualifying_availability_workbook.py file.xlsx --include-incomplete

``--include-incomplete`` loads all rows regardless of the *Status* / completion column.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from activityAnalysis.load_activity_data import load_qualifying_availability_workbook


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load qualifying availability Excel into official_qualifying_* tables."
    )
    parser.add_argument("excel_path", help="Path to .xlsx export")
    parser.add_argument(
        "--sheet",
        default=None,
        help="Worksheet name (default: original)",
    )
    parser.add_argument(
        "--allow-missing-status",
        action="store_true",
        help="If the workbook has no Status / completion column, load all rows instead of erroring",
    )
    parser.add_argument(
        "--include-incomplete",
        action="store_true",
        help="Do not filter to Complete responses only",
    )
    parser.add_argument(
        "--status-column",
        default=None,
        help="Explicit completion column name (overrides autodetection)",
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

    summary = load_qualifying_availability_workbook(
        args.excel_path,
        sheet_name=args.sheet,
        only_complete_responses=not args.include_incomplete,
        allow_missing_completion_status=args.allow_missing_status,
        completion_status_column=args.status_column,
        commit=not args.dry_run,
    )
    print(json.dumps(summary, indent=2))
    if summary.get("unmatched_member_numbers"):
        print(
            "\nWarning: some member numbers are not in the officials directory — "
            "those rows were skipped.",
            file=sys.stderr,
        )
    if args.dry_run:
        print("\nDry run: changes were not committed.", file=sys.stderr)


if __name__ == "__main__":
    main()
