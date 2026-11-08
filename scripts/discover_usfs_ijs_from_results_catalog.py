#!/usr/bin/env python3
"""
List IJS Live leaderboard URLs from the results.usfigureskating.org catalog.

One request to the public events index replaces probing
``/leaderboard/results/{year}/{id}/index.asp``. The CSV matches
``discover_usfs_ijs_competitions.py`` so ``load_discovered_ijs_competitions_csv.py``
can scrape it. ``season_year`` is filled (``2627`` for the 2026–27 season).

``http_status`` is 200 because the catalog published the link. This script does
not fetch each leaderboard page.

Default filter: the current season (July turnover) and ``status=final``.
On the site that season is ``2026–27``, which is the completed list behind
https://results.usfigureskating.org/ . Pass ``--season all`` or ``--status all``
to widen it.

Example::

    python scripts/discover_usfs_ijs_from_results_catalog.py -o discovered_2627.csv

    python scripts/discover_usfs_ijs_from_results_catalog.py \\
        --season 2026-27 --status final --skip-if-in-database -o discovered_2627.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys

import requests

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from discover_usfs_ijs_competitions import (  # noqa: E402
    _load_existing_competition_base_urls,
)
from ijs_results_urls import results_url_dedupe_key  # noqa: E402
from usfs_events_catalog import (  # noqa: E402
    current_season_label,
    fetch_events_index,
    normalize_season_label,
    select_catalog_rows,
)

CSV_FIELDS = [
    "year",
    "competition_id",
    "url",
    "http_status",
    "competition_name",
    "start_date",
    "end_date",
    "location",
    "fetched_at_utc",
    "probe_error",
    "season_year",
]


def _parse_token_set(raw: str, *, normalizer) -> set[str] | None:
    text = (raw or "").strip()
    if not text or text.lower() == "all":
        return None
    return {normalizer(part) for part in text.split(",") if part.strip()}


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--season",
        default=current_season_label(),
        help=(
            "Catalog season, comma-separated (2026-27 or 2627). "
            "'all' keeps every season. Default: the current season."
        ),
    )
    parser.add_argument(
        "--status",
        default="final",
        help="Event status, comma-separated (final, live, upcoming). 'all' keeps every status.",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="discovered_ijs_competitions.csv",
        help="CSV path. Overwrites the file.",
    )
    parser.add_argument(
        "--skip-if-in-database",
        action="store_true",
        help=(
            "Omit URLs already stored on public.competition.results_url. "
            "Requires DATABASE_URL."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        seasons = _parse_token_set(args.season, normalizer=normalize_season_label)
        statuses = _parse_token_set(args.status, normalizer=lambda part: part.strip().lower())
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    existing: set[str] = set()
    if args.skip_if_in_database:
        try:
            existing = _load_existing_competition_base_urls()
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        except Exception as exc:
            print(f"Failed loading existing competitions: {exc}", file=sys.stderr)
            return 3
        print(
            f"skip-if-in-database: {len(existing)} URL(s) loaded from competition table",
            file=sys.stderr,
        )

    try:
        events, generated_at = fetch_events_index(requests.Session())
    except Exception as exc:
        print(f"Could not read the results catalog: {exc}", file=sys.stderr)
        return 1

    rows, skipped_no_url = select_catalog_rows(
        events,
        seasons=seasons,
        statuses=statuses,
        fetched_at_utc=generated_at,
    )
    written = 0
    skipped_in_db = 0
    with open(args.output, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            if args.skip_if_in_database and results_url_dedupe_key(row["url"]) in existing:
                skipped_in_db += 1
                continue
            writer.writerow(row)
            written += 1

    season_note = args.season if seasons is not None else "all"
    status_note = args.status if statuses is not None else "all"
    print(
        f"Wrote {written} IJS Live link(s) to {args.output} "
        f"(season {season_note}, status {status_note}; "
        f"{skipped_no_url} matching event(s) have no IJS Live URL"
        + (
            f"; skipped {skipped_in_db} already in DB"
            if args.skip_if_in_database
            else ""
        )
        + ").",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
