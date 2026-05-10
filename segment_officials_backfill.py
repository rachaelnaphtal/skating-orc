#!/usr/bin/env python3
"""
Backfill public.segment_official for existing competitions without running scrape().

Loads the competition index (``index.asp`` and/or ``index.htm``), walks segment result links
(classic ``Final`` rows on ``index.asp``, or Swiss Timing FSM ``Panel of Judges`` rows on
``index.htm``). Use the ``--fsm`` flag when an event publishes both formats and you need the
Swiss Timing index only. Cover labels are normalized to ``public.segment.name`` via
``ijs_event_label_to_db_segment_name``.
"""

from __future__ import annotations

import argparse
import sys

from database import get_db_session
from database_loader import DatabaseLoader
from downloadResults import (
    get_page_contents,
    iter_ijs_index_final_href_and_cover_event,
    iter_fsm_leaderboard_panel_href_and_cover_event,
    parse_ijs_segment_officials,
)
from judgingParsing import ijs_event_label_to_db_segment_name
from models import Competition, Segment


def normalize_results_url(url: str) -> str:
    u = (url or "").strip().rstrip("/")
    for suffix in ("/index.asp", "/index.htm"):
        if u.lower().endswith(suffix):
            u = u[: -len(suffix)].rstrip("/")
            break
    return u


def find_segment_by_db_name(
    session, competition_id: int, db_segment_name: str
) -> Segment | None:
    name = (db_segment_name or "").strip()
    if not name:
        return None
    return (
        session.query(Segment)
        .filter_by(competition_id=competition_id, name=name)
        .first()
    )


def backfill_one_competition(
    session,
    loader: DatabaseLoader,
    results_url: str,
    *,
    dry_run: bool = False,
    fsm: bool = False,
) -> dict:
    """
    Returns keys: results_url, competition_id (or null), updated, skipped_no_segment,
    skipped_no_officials, index_fetched (which index URL was used, if any), errors (list of str).

    For USFS events that publish both ``index.asp`` (classic) and ``index.htm`` (Swiss Timing /
    FSM), pass ``fsm=True`` so only ``index.htm`` is used. Otherwise classic links are tried
    first on ``index.asp``, then FSM-style links on whichever index yields rows.
    """
    base_url = normalize_results_url(results_url)
    out: dict = {
        "results_url": base_url,
        "competition_id": None,
        "updated": 0,
        "skipped_no_segment": 0,
        "skipped_no_officials": 0,
        "index_fetched": None,
        "errors": [],
    }
    comp = (
        session.query(Competition)
        .filter(Competition.results_url == base_url)
        .first()
    )
    if not comp:
        out["errors"].append(f"No competition row with results_url={base_url!r}")
        return out
    out["competition_id"] = comp.id

    if fsm:
        index_candidates = [f"{base_url}/index.htm"]
    else:
        index_candidates = [f"{base_url}/index.asp", f"{base_url}/index.htm"]

    index_html = None
    index_url = None
    pairs: list[tuple[str, str]] = []
    for candidate in index_candidates:
        html = get_page_contents(candidate)
        if not html:
            continue
        pairs = list(iter_ijs_index_final_href_and_cover_event(html))
        if not pairs:
            pairs = list(iter_fsm_leaderboard_panel_href_and_cover_event(html))
        if pairs:
            index_html = html
            index_url = candidate
            break

    if not index_html:
        out["errors"].append(
            f"Failed to fetch any index in {', '.join(index_candidates)}"
        )
        return out
    out["index_fetched"] = index_url

    if not pairs:
        out["errors"].append(
            f"No segment links on {index_url} (no Final rows; no FSM panel rows). "
            f"For Swiss Timing events try --fsm to use index.htm only."
        )
        return out

    for href, cover_label in pairs:
        seg_url = f"{base_url}/{href}"
        seg_html = get_page_contents(seg_url)
        if not seg_html:
            out["errors"].append(f"Empty response: {seg_url}")
            continue
        db_segment_name = ijs_event_label_to_db_segment_name(cover_label)
        rows = parse_ijs_segment_officials(seg_html)
        if not rows:
            out["skipped_no_officials"] += 1
            if cover_label:
                out["errors"].append(
                    f"No Officials table for segment page {seg_url!r} "
                    f"(cover {cover_label!r} → db {db_segment_name!r})"
                )
            continue
        if not db_segment_name:
            out["skipped_no_segment"] += 1
            out["errors"].append(
                f"Empty segment name from index row for {href!r} ({seg_url})"
            )
            continue
        segment = find_segment_by_db_name(session, comp.id, db_segment_name)
        if not segment:
            out["skipped_no_segment"] += 1
            out["errors"].append(
                f"No DB segment for cover label {cover_label!r} "
                f"(normalized {db_segment_name!r}, from {href})"
            )
            continue
        if dry_run:
            out["updated"] += 1
            continue
        loader.replace_segment_officials(segment.id, rows)
        out["updated"] += 1

    return out


def main() -> int:
    p = argparse.ArgumentParser(
        description="Backfill segment_official from IJS results (no full scrape)"
    )
    p.add_argument(
        "--url",
        action="append",
        dest="urls",
        metavar="RESULTS_URL",
        help="Competition results base URL (optional /index.asp or /index.htm are stripped), "
        "e.g. https://ijs.../2025/35890. Repeatable.",
    )
    p.add_argument(
        "--from-db",
        action="store_true",
        help="Use every competition in public.competition that has at least one segment.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="With --from-db, max number of competitions to process.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and match segments only; do not write segment_official rows.",
    )
    p.add_argument(
        "--fsm",
        action="store_true",
        help="Use Swiss Timing index.htm only (Panel of Judges). Needed when the event also "
        "has index.asp with a different (classic) layout.",
    )
    args = p.parse_args()

    targets: list[str] = []
    if args.urls:
        targets.extend(args.urls)

    session = get_db_session()
    loader = DatabaseLoader(session)

    if args.from_db:
        q = (
            session.query(Competition)
            .join(Segment, Segment.competition_id == Competition.id)
            .distinct()
            .order_by(Competition.id)
        )
        if args.limit is not None:
            q = q.limit(args.limit)
        targets.extend(row.results_url for row in q)

    if not targets:
        p.print_help()
        print("\nProvide --url (one or more) and/or --from-db.", file=sys.stderr)
        return 2

    seen = set()
    exit_code = 0
    for raw in targets:
        url = normalize_results_url(raw)
        if url in seen:
            continue
        seen.add(url)
        print(f"=== {url} ===")
        stats = backfill_one_competition(
            session, loader, url, dry_run=args.dry_run, fsm=args.fsm
        )
        print(
            f"  competition_id={stats['competition_id']} "
            f"index={stats.get('index_fetched')} "
            f"updated={stats['updated']} "
            f"skipped_no_segment={stats['skipped_no_segment']} "
            f"skipped_no_officials={stats['skipped_no_officials']} "
            f"dry_run={args.dry_run}"
        )
        for err in stats["errors"]:
            print(f"  ! {err}", file=sys.stderr)
        if stats["errors"]:
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
