#!/usr/bin/env python3
"""
Probe USFS IJS leaderboard ``index.asp`` URLs and write hits to CSV.

URL pattern:

    https://ijs.usfigureskating.org/leaderboard/results/{year}/{numeric_id}/index.asp

A row is written when the response is HTTP 200 and the HTML looks like a real
competition index (same heuristic as scraping: at least one ``<a>Final</a>`` link).

The CSV is **opened at startup** and each matching row is written and **flushed immediately**,
so you keep hits (and misses if ``--include-misses``) after Ctrl+C or crash — not only when the run finishes.

Progress (current year / id) prints to **stderr** as it runs (line-buffered with flush),
so you can watch it in another terminal or the same one without waiting for the CSV.
Use ``--log-requests`` to print every full URL and HTTP status on stderr.

Example:

    python scripts/discover_usfs_ijs_competitions.py --years 2026 --start-id 36400 --end-id 36600 --output hits.csv

Skip IDs already present in ``public.competition`` (requires ``DATABASE_URL``; no GET, no CSV row):

    python scripts/discover_usfs_ijs_competitions.py --years 2025 --start-id 34000 --end-id 35000 \\
        --skip-if-in-database -o new_hits.csv

Full usage, flags, and workflow: scripts/README.md
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from ijs_index_parse import ijs_index_start_end_and_location  # noqa: E402
from ijs_results_urls import results_url_dedupe_key  # noqa: E402


def _load_existing_competition_base_urls() -> set[str]:
    """Dedupe keys for ``results_url`` values already in the judging DB."""
    import os

    from sqlalchemy import create_engine, text

    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        raise RuntimeError("DATABASE_URL is not set")
    if raw.startswith("postgres://"):
        raw = raw.replace("postgres://", "postgresql://", 1)
    engine = create_engine(raw, pool_pre_ping=True)
    stmts = (
        text(
            "SELECT results_url FROM public.competition "
            "WHERE results_url IS NOT NULL AND trim(results_url) <> ''"
        ),
        text(
            "SELECT results_url FROM competition "
            "WHERE results_url IS NOT NULL AND trim(results_url) <> ''"
        ),
    )
    last_err: Exception | None = None
    with engine.connect() as conn:
        for stmt in stmts:
            try:
                result = conn.execute(stmt)
                out: set[str] = set()
                for row in result:
                    ru = row[0]
                    if ru:
                        out.add(results_url_dedupe_key(str(ru)))
                return out
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"Could not read competition.results_url: {last_err}")


BASE = "https://ijs.usfigureskating.org/leaderboard/results"


def _session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
    s.mount("https://", adapter)
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
            )
        }
    )
    return s


def _looks_like_ijs_index(soup: BeautifulSoup) -> bool:
    """Index rows often wrap anchor text with whitespace; avoid exact ``string=`` match."""
    for a in soup.find_all("a", href=True):
        if a.get_text(strip=True) == "Final":
            return True
    return False


def _parse_index_row(html: str, page_url: str = "") -> dict[str, str]:
    """Extract title and dates/location for current and older IJS index templates."""
    soup = BeautifulSoup(html, "html.parser")
    title = ""

    # Current ijsLive template: <div class="eventTitle"><h2 class="title">...</h2>
    h2_title = soup.find("h2", class_="title")
    if h2_title:
        title = " ".join(h2_title.get_text().split())
    if not title:
        h1 = soup.find("h1", class_="header1")
        if not h1:
            h1 = soup.find("h1")
        if h1:
            title = " ".join(h1.get_text().split())
    if not title:
        t_el = soup.find("title")
        if t_el:
            title = " ".join(t_el.get_text().split())

    start_date, end_date, location = ijs_index_start_end_and_location(soup, page_url)

    return {
        "competition_name": title,
        "start_date": start_date,
        "end_date": end_date,
        "location": location,
    }


def probe_url(
    session: requests.Session, url: str, timeout: float
) -> tuple[int | None, str | None]:
    try:
        r = session.get(url, timeout=timeout)
    except requests.RequestException:
        return None, None
    if r.status_code != 200:
        return r.status_code, None
    return 200, r.text


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--years",
        type=str,
        default="2026",
        help="Comma-separated calendar years in the path (default: 2026)",
    )
    p.add_argument("--start-id", type=int, required=True)
    p.add_argument("--end-id", type=int, required=True)
    p.add_argument("--step", type=int, default=1, help="ID step (default: 1)")
    p.add_argument("--delay", type=float, default=0.75, help="Seconds between requests")
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument(
        "--output",
        "-o",
        type=str,
        default="discovered_ijs_competitions.csv",
    )
    p.add_argument(
        "--include-misses",
        action="store_true",
        help="Also append rows for non-hits (empty name, note in error column)",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print per-probe progress on stderr.",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=1,
        metavar="N",
        help="Print progress every N probes (default: 1). Use 10 or 50 for less noise.",
    )
    p.add_argument(
        "--skip-if-in-database",
        action="store_true",
        help=(
            "Do not HTTP-probe or write CSV rows for URLs whose base path already exists in "
            "``public.competition.results_url``. Requires DATABASE_URL (uses SQLAlchemy). "
            "Skips do not count toward --delay sleep."
        ),
    )
    p.add_argument(
        "--log-requests",
        action="store_true",
        help="Print every probed URL and HTTP status on stderr.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.end_id < args.start_id:
        print("end-id must be >= start-id", file=sys.stderr)
        return 2
    if args.step < 1:
        print("step must be >= 1", file=sys.stderr)
        return 2
    if args.progress_every < 1:
        print("progress-every must be >= 1", file=sys.stderr)
        return 2

    years = [y.strip() for y in args.years.split(",") if y.strip()]
    if not years:
        print("no years after parsing --years", file=sys.stderr)
        return 2

    fieldnames = [
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
    ]

    existing_urls: set[str] = set()
    if args.skip_if_in_database:
        try:
            existing_urls = _load_existing_competition_base_urls()
        except RuntimeError as e:
            print(str(e), file=sys.stderr)
            return 2
        except Exception as e:
            print(f"Failed loading existing competitions: {e}", file=sys.stderr)
            return 3
        print(
            f"skip-if-in-database: {len(existing_urls)} URL(s) loaded from competition table",
            file=sys.stderr,
        )

    sess = _session()
    row_count = 0
    skipped_in_db = 0

    span = (args.end_id - args.start_id) // args.step + 1
    total_probes = len(years) * span
    probe_num = 0

    with open(args.output, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()
        out_f.flush()

        for year in years:
            cid = args.start_id
            while cid <= args.end_id:
                url = f"{BASE}/{year}/{cid}/index.asp"

                if args.skip_if_in_database and results_url_dedupe_key(url) in existing_urls:
                    skipped_in_db += 1
                    probe_num += 1
                    if (
                        not args.quiet
                        and (
                            probe_num % args.progress_every == 0
                            or probe_num == total_probes
                        )
                    ):
                        print(
                            f"[{probe_num}/{total_probes}] year={year} id={cid} in_db_skip",
                            file=sys.stderr,
                            flush=True,
                        )
                    cid += args.step
                    continue

                status, body = probe_url(sess, url, args.timeout)
                if args.log_requests:
                    if status is None:
                        line = f"GET {url} -> failed (no response)"
                    else:
                        line = f"GET {url} -> HTTP {status}"
                    print(line, file=sys.stderr, flush=True)
                fetched = datetime.now(timezone.utc).isoformat(timespec="seconds")

                err = ""
                if status is None:
                    err = "request_failed"
                elif status != 200:
                    err = f"http_{status}"

                parsed: dict[str, str] = {}
                hit = False
                if body:
                    soup = BeautifulSoup(body, "html.parser")
                    if _looks_like_ijs_index(soup):
                        hit = True
                        parsed = _parse_index_row(body, url)

                if hit or args.include_misses:
                    writer.writerow(
                        {
                            "year": year,
                            "competition_id": cid,
                            "url": url,
                            "http_status": status if status is not None else "",
                            "competition_name": parsed.get("competition_name", ""),
                            "start_date": parsed.get("start_date", ""),
                            "end_date": parsed.get("end_date", ""),
                            "location": parsed.get("location", ""),
                            "fetched_at_utc": fetched,
                            "probe_error": err,
                        }
                    )
                    out_f.flush()
                    row_count += 1

                probe_num += 1
                if (
                    not args.quiet
                    and (probe_num % args.progress_every == 0 or probe_num == total_probes)
                ):
                    if hit:
                        tag = "hit"
                    elif err:
                        tag = err
                    else:
                        tag = "miss"
                    name_hint = (parsed.get("competition_name") or "")[:50]
                    extra = f" | {name_hint}" if name_hint else ""
                    print(
                        f"[{probe_num}/{total_probes}] year={year} id={cid} {tag}{extra}",
                        file=sys.stderr,
                        flush=True,
                    )

                if args.delay > 0:
                    time.sleep(args.delay)
                cid += args.step

    print(
        f"Wrote {row_count} row(s) to {args.output}"
        + (f"; skipped {skipped_in_db} already in DB" if args.skip_if_in_database else ""),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
