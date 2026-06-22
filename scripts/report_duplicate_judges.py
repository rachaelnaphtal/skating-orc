#!/usr/bin/env python3
"""
List ``public.judge`` rows that share a normalized name key (case/spacing variants).

Canonical pick matches ``DatabaseLoader._ensure_judges_by_name``: most element+PCS
score references, then lowest id.

  python scripts/report_duplicate_judges.py
  python scripts/report_duplicate_judges.py --csv analysisTemp/duplicate_judges.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from database import ensure_database_for_streamlit, get_database_url, get_db_session
from database_loader import fetch_duplicate_judge_groups


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=str,
        default="",
        help="Optional path to write one row per duplicate judge.",
    )
    args = parser.parse_args()

    ensure_database_for_streamlit()
    db_url = get_database_url()
    host_hint = db_url.split("@")[-1].split("/")[0] if "@" in db_url else "(local)"
    print(f"Database host: {host_hint}", flush=True)

    session = get_db_session()
    groups = fetch_duplicate_judge_groups(session)
    if not groups:
        print("No duplicate judge name groups found.", flush=True)
        return 0

    print(f"Found {len(groups)} duplicate name group(s):\n", flush=True)
    csv_rows: list[dict] = []
    for group in groups:
        print(f"match_key={group['match_key']!r} canonical_id={group['canonical_id']}")
        for member in group["members"]:
            tag = "canonical" if member["is_canonical"] else "duplicate"
            print(
                f"  [{member['id']}] {member['name']!r} "
                f"({tag}; element={member['element_scores']}, "
                f"pcs={member['pcs_scores']}, total={member['total_scores']})"
            )
            csv_rows.append({
                "match_key": group["match_key"],
                "canonical_id": group["canonical_id"],
                "judge_id": member["id"],
                "judge_name": member["name"],
                "is_canonical": member["is_canonical"],
                "element_scores": member["element_scores"],
                "pcs_scores": member["pcs_scores"],
                "total_scores": member["total_scores"],
            })
        print(flush=True)

    if args.csv:
        out_path = Path(args.csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "match_key",
                    "canonical_id",
                    "judge_id",
                    "judge_name",
                    "is_canonical",
                    "element_scores",
                    "pcs_scores",
                    "total_scores",
                ],
            )
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"Wrote {len(csv_rows)} row(s) to {out_path}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
