#!/usr/bin/env python3
"""
Discover ISU figure skating "Detailed Results" URLs and optionally load them.

The ISU events listing is backed by ``https://api.isu-skating.com/api/event/mobile-list``.
This script uses that API to enumerate figure skating ISU events by season, follows each
event detail page, extracts the "Detailed Results" URL, writes a CSV, and can run the
project's normal ``downloadResults.scrape`` path for every discovered results page.

Examples:

  # Discover the last two started seasons and write a CSV.
  python scripts/load_isu_figure_skating_results.py -o isu_figure_results.csv

  # Discover explicit seasons, but only print what would be loaded.
  python scripts/load_isu_figure_skating_results.py --seasons 2526,2425 --dry-run

  # Discover ISU + international competitions whose start date is in 2025.
  python scripts/load_isu_figure_skating_results.py --year 2025 \
      --event-levels ISU,International -o figure_results_2025.csv

  # Discover and load every found ISU competition into the database.
  python scripts/load_isu_figure_skating_results.py --load --quiet
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from officials_competition_types import (  # noqa: E402
    OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL,
    OFFICIALS_COMPETITION_TYPE_ID_ISU_OTHER,
    OFFICIALS_COMPETITION_TYPE_ID_ISU_WORLD_CHAMPIONSHIPS,
)


ISU_API_BASE = "https://api.isu-skating.com/api"
ISU_SITE_BASE = "https://isu.org"
DISCIPLINE_TITLE = "FIGURE SKATING"
DEFAULT_EVENT_LEVELS = ("ISU",)
EVENT_LEVEL_CHOICES = ("ISU", "International")

CSV_FIELDS = (
    "season",
    "season_year",
    "event_level",
    "isu_event_id",
    "event_name",
    "event_sub_type_name",
    "display_date",
    "start_date",
    "end_date",
    "location",
    "isu_event_url",
    "detailed_results_url",
    "normalized_results_url",
    "is_fsm",
    "officials_analysis_competition_type_id",
    "international",
    "status",
    "error",
    "fetched_at_utc",
)


@dataclass(frozen=True)
class ResultRow:
    season: str
    season_year: str
    event_level: str
    isu_event_id: str
    event_name: str
    event_sub_type_name: str
    display_date: str
    start_date: str
    end_date: str
    location: str
    isu_event_url: str
    detailed_results_url: str
    normalized_results_url: str
    is_fsm: bool
    officials_analysis_competition_type_id: int
    international: bool
    status: str
    error: str
    fetched_at_utc: str

    def as_csv_row(self) -> dict[str, str]:
        return {
            "season": self.season,
            "season_year": self.season_year,
            "event_level": self.event_level,
            "isu_event_id": self.isu_event_id,
            "event_name": self.event_name,
            "event_sub_type_name": self.event_sub_type_name,
            "display_date": self.display_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "location": self.location,
            "isu_event_url": self.isu_event_url,
            "detailed_results_url": self.detailed_results_url,
            "normalized_results_url": self.normalized_results_url,
            "is_fsm": "true" if self.is_fsm else "false",
            "officials_analysis_competition_type_id": str(
                self.officials_analysis_competition_type_id
            ),
            "international": "true" if self.international else "false",
            "status": self.status,
            "error": self.error,
            "fetched_at_utc": self.fetched_at_utc,
        }


def _session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
            )
        }
    )
    return s


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _date_from_iso(value: str | None) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return ""


def _parse_iso_date(value: str) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def season_year_from_title(season: str) -> str:
    """Convert ``2025/2026`` to the repo's compact season code, ``2526``."""
    parts = [p.strip() for p in (season or "").split("/")]
    if len(parts) != 2 or any(len(p) < 2 for p in parts):
        return ""
    return f"{parts[0][-2:]}{parts[1][-2:]}"


def season_title_from_compact_code(season: str) -> str:
    """Convert repo-style compact seasons like ``2526`` to ISU API labels."""
    s = (season or "").strip()
    if not (len(s) == 4 and s.isdigit()):
        return s
    start_yy = int(s[:2])
    end_yy = int(s[2:])
    start_year = 1900 + start_yy if start_yy > end_yy and start_yy >= 90 else 2000 + start_yy
    end_year = 2000 + end_yy
    return f"{start_year}/{end_year}"


def normalize_results_base_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    lower = u.lower()
    for suffix in ("/index.asp", "/index.htm", "/index.html"):
        if lower.endswith(suffix):
            u = u[: -len(suffix)]
            break
    return u.rstrip("/")


def is_fsm_results_url(url: str) -> bool:
    """Most ISU/Swiss Timing result pages use ``index.htm``; classic IJS uses ``index.asp``."""
    return not (url or "").strip().lower().endswith("/index.asp")


def _clean_embedded_url(url: str) -> str:
    return (
        (url or "")
        .replace("\\/", "/")
        .replace("\\u002F", "/")
        .replace("\\u0026", "&")
        .strip()
    )


def extract_detailed_results_url(html: str, page_url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        text = " ".join(a.get_text(" ", strip=True).split()).lower()
        if text == "detailed results":
            return urljoin(page_url, a["href"])

    # Fallback for Next.js flight data when the rendered anchor is not easy to parse.
    for marker in ('"detail_result_url":"', 'detail_result_url\\":\\"'):
        start = html.find(marker)
        if start == -1:
            continue
        start += len(marker)
        end_markers = ('"', '\\"')
        ends = [html.find(end_marker, start) for end_marker in end_markers]
        ends = [idx for idx in ends if idx != -1]
        if not ends:
            continue
        return _clean_embedded_url(html[start : min(ends)])
    return ""


def button_detailed_results_url(event: dict) -> str:
    for idx in (1, 2, 3):
        title = str(event.get(f"button_{idx}_title") or "").strip().lower()
        url = str(event.get(f"button_{idx}_url") or "").strip()
        if title == "detailed results" and url:
            return url
    return ""


def fetch_available_seasons(session: requests.Session, timeout: float) -> list[dict]:
    r = session.get(f"{ISU_API_BASE}/common/get-season", timeout=timeout)
    r.raise_for_status()
    data = r.json().get("data") or []
    return [item for item in data if str(item.get("title") or "").strip()]


def choose_default_seasons(seasons: Iterable[dict], today: date | None = None) -> list[str]:
    """Pick the two latest seasons that have started, avoiding future listing-only seasons."""
    today = today or date.today()
    started = []
    for item in seasons:
        title = str(item.get("title") or "").strip()
        start = _date_from_iso(item.get("from"))
        if not title or not start:
            continue
        try:
            start_date = date.fromisoformat(start)
        except ValueError:
            continue
        if start_date <= today:
            started.append((start_date, title))
    started.sort(reverse=True)
    return [title for _, title in started[:2]]


def seasons_for_calendar_year(seasons: Iterable[dict], year: int) -> list[str]:
    """Return ISU season labels likely to contain events starting in a calendar year."""
    candidates = {f"{year - 1}/{year}", f"{year}/{year + 1}"}
    available = [str(item.get("title") or "").strip() for item in seasons]
    selected = [title for title in available if title in candidates]
    if selected:
        return selected
    return sorted(candidates)


def parse_seasons_arg(
    raw: str | None,
    session: requests.Session,
    timeout: float,
    year: int | None = None,
) -> list[str]:
    if raw and raw.strip():
        return [
            season_title_from_compact_code(part)
            for part in (p.strip() for p in raw.split(","))
            if part
        ]
    if year is not None:
        return seasons_for_calendar_year(fetch_available_seasons(session, timeout), year)
    seasons = choose_default_seasons(fetch_available_seasons(session, timeout))
    if not seasons:
        raise RuntimeError("No started ISU seasons found; pass --seasons explicitly.")
    return seasons


def parse_event_levels_arg(raw: str | None) -> tuple[str, ...]:
    if not raw or not raw.strip():
        return DEFAULT_EVENT_LEVELS
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if any(part.lower() == "all" for part in parts):
        return EVENT_LEVEL_CHOICES
    invalid = [part for part in parts if part not in EVENT_LEVEL_CHOICES]
    if invalid:
        raise SystemExit(
            f"Invalid --event-levels value(s): {invalid!r}; "
            f"allowed: {', '.join(EVENT_LEVEL_CHOICES)} or All"
        )
    return tuple(dict.fromkeys(parts))


def is_world_championship_event(event_name: str) -> bool:
    return "world championships" in (event_name or "").lower()


def inferred_competition_type_id(event_level: str, event_name: str) -> int:
    if event_level == "International":
        return OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL
    if is_world_championship_event(event_name):
        return OFFICIALS_COMPETITION_TYPE_ID_ISU_WORLD_CHAMPIONSHIPS
    return OFFICIALS_COMPETITION_TYPE_ID_ISU_OTHER


def competition_type_id_for_row(
    row: ResultRow, override_competition_type_id: int | None = None
) -> int:
    if override_competition_type_id is not None:
        return override_competition_type_id
    return row.officials_analysis_competition_type_id


def fetch_events_for_season(
    session: requests.Session,
    season: str,
    event_level: str,
    pagesize: int,
    timeout: float,
) -> list[dict]:
    payload = {
        "pagesize": pagesize,
        "discipline_title": DISCIPLINE_TITLE,
        "season": season,
        "event_level": event_level,
    }
    r = session.post(f"{ISU_API_BASE}/event/mobile-list", json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json().get("data") or []
    if not isinstance(data, list):
        return []
    return data


def event_location(event: dict) -> str:
    city = str(event.get("city") or "").strip()
    country = str(event.get("country_code") or event.get("country_name") or "").strip()
    if city and country:
        return f"{city} / {country}"
    return city or country


def event_detail_page_url(event: dict) -> str:
    return f"{ISU_SITE_BASE}/events/{event['slug']}/"


def detailed_results_url_for_event(
    session: requests.Session, event: dict, timeout: float
) -> tuple[str, str]:
    direct = button_detailed_results_url(event)
    if direct:
        return direct, ""

    url = event_detail_page_url(event)
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
    except requests.RequestException as exc:
        return "", f"event detail fetch failed: {exc}"

    result_url = extract_detailed_results_url(r.text, url)
    if result_url:
        return result_url, ""
    return "", "no Detailed Results URL found"


def collect_result_rows(
    session: requests.Session,
    seasons: Iterable[str],
    *,
    event_levels: Iterable[str],
    year: int | None,
    pagesize: int,
    timeout: float,
    delay: float,
    include_missing: bool,
    limit: int | None,
    start_offset: int,
    quiet: bool,
) -> list[ResultRow]:
    rows: list[ResultRow] = []
    seen = 0
    fetched_at = _utc_now()
    for season in seasons:
        for event_level in event_levels:
            events = fetch_events_for_season(
                session, season, event_level, pagesize, timeout
            )
            if not quiet:
                print(
                    f"{season} {event_level}: {len(events)} figure skating events",
                    file=sys.stderr,
                )
            for event in events:
                start_date = _date_from_iso(event.get("from_date"))
                parsed_start = _parse_iso_date(start_date)
                if year is not None:
                    if parsed_start is None or parsed_start.year != year:
                        continue
                if seen < start_offset:
                    seen += 1
                    continue
                if limit is not None and len(rows) >= limit:
                    return rows
                seen += 1

                name = str(event.get("name") or "").strip()
                detail_url = event_detail_page_url(event)
                results_url, error = detailed_results_url_for_event(
                    session, event, timeout
                )
                status = "found" if results_url else "missing"
                normalized = normalize_results_base_url(results_url)
                if not quiet:
                    print(f"{status}: {season} {event_level} | {name}", file=sys.stderr)

                if results_url or include_missing:
                    competition_type_id = inferred_competition_type_id(event_level, name)
                    rows.append(
                        ResultRow(
                            season=season,
                            season_year=season_year_from_title(season),
                            event_level=event_level,
                            isu_event_id=str(event.get("event_id") or ""),
                            event_name=name,
                            event_sub_type_name=str(
                                event.get("event_sub_type_name") or ""
                            ).strip(),
                            display_date=str(event.get("display_date") or "").strip(),
                            start_date=start_date,
                            end_date=_date_from_iso(event.get("to_date")),
                            location=event_location(event),
                            isu_event_url=detail_url,
                            detailed_results_url=results_url,
                            normalized_results_url=normalized,
                            is_fsm=is_fsm_results_url(results_url),
                            officials_analysis_competition_type_id=competition_type_id,
                            international=True,
                            status=status,
                            error=error,
                            fetched_at_utc=fetched_at,
                        )
                    )
                if delay:
                    time.sleep(delay)
    return rows


def write_csv(path: str, rows: Iterable[ResultRow]) -> None:
    if path == "-":
        out = sys.stdout
        close = False
    else:
        out = open(path, "w", newline="", encoding="utf-8")
        close = True
    try:
        writer = csv.DictWriter(out, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())
        out.flush()
    finally:
        if close:
            out.close()


def _load_existing_competition_base_urls() -> set[str]:
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
                return {
                    normalize_results_base_url(str(row[0]))
                    for row in conn.execute(stmt)
                    if row[0]
                }
            except Exception as exc:
                last_err = exc
    raise RuntimeError(f"Could not read competition.results_url: {last_err}")


def load_rows(
    rows: Iterable[ResultRow],
    *,
    dry_run: bool,
    metadata_only: bool,
    skip_if_in_database: bool,
    quiet: bool,
    verbose: bool,
    log_file: str | None,
    default_competition_type_id: int | None,
    delay: float,
) -> tuple[int, int]:
    eligible = [row for row in rows if row.normalized_results_url and row.status == "found"]
    existing = _load_existing_competition_base_urls() if skip_if_in_database else set()

    planned = [
        row
        for row in eligible
        if normalize_results_base_url(row.normalized_results_url) not in existing
    ]

    if dry_run:
        for row in planned:
            type_id = competition_type_id_for_row(row, default_competition_type_id)
            print(
                f"DRY RUN load {row.season_year} | "
                f"{row.event_level} | "
                f"type={type_id} | international=true | "
                f"{'FSM' if row.is_fsm else 'classic'} | "
                f"{row.normalized_results_url} | {row.event_name}"
            )
        return 0, len(planned)

    from database import get_db_session
    from database_loader import DatabaseLoader
    from downloadResults import scrape
    from ijs_scrape_log import configure as configure_scrape_logging
    from ijs_scrape_log import print_batch_summary
    from officials_competition_types import competition_load_flags_from_officials_type_id

    configure_scrape_logging(quiet=quiet, verbose=verbose, log_file=log_file)
    db_session = get_db_session()
    db_loader = DatabaseLoader(db_session, defer_commits=True)
    http_session = _session()
    loaded = 0
    try:
        for row in planned:
            if not quiet:
                print(f"load: {row.event_name}", file=sys.stderr)
            qualifying = None
            nqs = None
            start_date = _parse_iso_date(row.start_date)
            end_date = _parse_iso_date(row.end_date)
            type_id = competition_type_id_for_row(row, default_competition_type_id)
            qualifying, nqs = competition_load_flags_from_officials_type_id(type_id)
            if metadata_only:
                db_loader.insert_competition(
                    row.event_name,
                    row.normalized_results_url,
                    row.season_year,
                    qualifying=qualifying,
                    nqs=nqs,
                    officials_analysis_competition_type_id=type_id,
                    international=row.international,
                )
                db_loader.updateCompetition(
                    row.normalized_results_url,
                    location=row.location,
                    start_date=start_date,
                    end_date=end_date,
                    name=row.event_name,
                    qualifying=qualifying,
                    nqs=nqs,
                    officials_analysis_competition_type_id=type_id,
                    update_officials_competition_type=True,
                    international=row.international,
                )
                db_session.commit()
            else:
                scrape(
                    row.normalized_results_url,
                    row.event_name,
                    write_to_database=True,
                    write_excel=False,
                    year=row.season_year,
                    use_html=True,
                    isFSM=row.is_fsm,
                    qualifying=qualifying,
                    nqs=nqs,
                    officials_analysis_competition_type_id=type_id,
                    update_officials_competition_type=True,
                    international=row.international,
                    http_session=http_session,
                    db_session=db_session,
                    database_loader=db_loader,
                    competition_metadata={
                        "start_date": start_date,
                        "end_date": end_date,
                        "location": row.location,
                    },
                    commit_per_segment=False,
                    quiet=quiet,
                    verbose=verbose,
                    log_file=log_file,
                    configure_logging=False,
                )
                db_session.commit()
                print_batch_summary()
            loaded += 1
            if delay:
                time.sleep(delay)
    finally:
        http_session.close()
        db_session.close()
    return loaded, len(planned)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--seasons",
        help=(
            "Comma-separated seasons, e.g. 2526,2425 or 2025/2026,2024/2025. "
            "Default: latest two seasons whose start date has passed, or seasons "
            "overlapping --year when --year is passed."
        ),
    )
    p.add_argument(
        "--year",
        type=int,
        default=None,
        help="Calendar year to collect; filters events by start date.",
    )
    p.add_argument(
        "--event-levels",
        default=",".join(DEFAULT_EVENT_LEVELS),
        help="Comma-separated event levels: ISU, International, or All (default: ISU).",
    )
    p.add_argument(
        "--output",
        "-o",
        default="isu_figure_skating_detailed_results.csv",
        help="CSV output path, or '-' for stdout.",
    )
    p.add_argument("--pagesize", type=int, default=200)
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--delay", type=float, default=0.1, help="Delay between event detail fetches.")
    p.add_argument("--limit", type=int, default=None, help="Limit rows for testing.")
    p.add_argument("--start-offset", type=int, default=0)
    p.add_argument(
        "--include-missing",
        action="store_true",
        help="Also write events with no Detailed Results URL.",
    )
    p.add_argument(
        "--load",
        action="store_true",
        help="After discovery, load found result URLs into the database.",
    )
    p.add_argument(
        "--metadata-only",
        action="store_true",
        help="With --load, only upsert competition rows; do not scrape segments.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned load actions; implies --load but does not write to the database.",
    )
    p.add_argument(
        "--skip-if-in-database",
        action="store_true",
        help="With --load/--dry-run, skip normalized URLs already in public.competition.",
    )
    p.add_argument(
        "--officials-analysis-competition-type-id",
        type=int,
        default=None,
        help=(
            "Override inferred officials_analysis.competition_type id for loaded rows. "
            "Default inference: International=17, ISU World Championships=15, other ISU=16."
        ),
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce progress output.",
    )
    p.add_argument("--verbose", action="store_true", help="Verbose scraper logs with --load.")
    p.add_argument("--log-file", default=None, help="Optional DEBUG log file for --load.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    session = _session()
    try:
        seasons = parse_seasons_arg(args.seasons, session, args.timeout, args.year)
        event_levels = parse_event_levels_arg(args.event_levels)
        rows = collect_result_rows(
            session,
            seasons,
            event_levels=event_levels,
            year=args.year,
            pagesize=args.pagesize,
            timeout=args.timeout,
            delay=args.delay,
            include_missing=args.include_missing,
            limit=args.limit,
            start_offset=args.start_offset,
            quiet=args.quiet,
        )
    finally:
        session.close()

    write_csv(args.output, rows)
    found = sum(1 for row in rows if row.status == "found")
    missing = sum(1 for row in rows if row.status == "missing")
    if not args.quiet:
        print(
            f"wrote {len(rows)} rows to {args.output} ({found} found, {missing} missing)",
            file=sys.stderr,
        )

    if args.load or args.dry_run:
        loaded, planned = load_rows(
            rows,
            dry_run=args.dry_run,
            metadata_only=args.metadata_only,
            skip_if_in_database=args.skip_if_in_database,
            quiet=args.quiet,
            verbose=args.verbose,
            log_file=args.log_file,
            default_competition_type_id=args.officials_analysis_competition_type_id,
            delay=args.delay,
        )
        if not args.quiet:
            action = "planned" if args.dry_run else "loaded"
            print(f"{action} {loaded or planned} competitions", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
