#!/usr/bin/env python3
"""
Discover ISU skating "Detailed Results" URLs and optionally load them.

The ISU events listing is backed by ``https://api.isu-skating.com/api/event/mobile-list``.
This script enumerates events by discipline (figure and/or synchronized skating), season,
and event level, extracts each event's Detailed Results URL, writes a CSV, and can load
via ``downloadResults.scrape``.

Examples:

  # Figure skating only (default).
  python scripts/load_isu_figure_skating_results.py -o isu_results.csv

  # Figure + synchronized skating.
  python scripts/load_isu_figure_skating_results.py --disciplines All -o isu_results.csv

  # Synchronized only.
  python scripts/load_isu_figure_skating_results.py \\
      --disciplines "SYNCHRONIZED SKATING" -o isu_synchro.csv

  # Write failed competitions to a file (see --write-failures / --failures-output).
  python scripts/load_isu_figure_skating_results.py --load --write-failures -o isu_results.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable, TextIO
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from officials_competition_types import (  # noqa: E402
    OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL_COMPETITION,
    OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP,
    OFFICIALS_COMPETITION_TYPE_ID_ISU_COMPETITION,
)


ISU_API_BASE = "https://api.isu-skating.com/api"
ISU_SITE_BASE = "https://isu.org"
DISCIPLINE_FIGURE_SKATING = "FIGURE SKATING"
DISCIPLINE_SYNCHRONIZED_SKATING = "SYNCHRONIZED SKATING"
DEFAULT_DISCIPLINES = (DISCIPLINE_FIGURE_SKATING,)
DISCIPLINE_CHOICES = (DISCIPLINE_FIGURE_SKATING, DISCIPLINE_SYNCHRONIZED_SKATING)
DEFAULT_EVENT_LEVELS = ("ISU",)
EVENT_LEVEL_CHOICES = ("ISU", "International")

CSV_FIELDS = (
    "discipline_title",
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
    discipline_title: str
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
            "discipline_title": self.discipline_title,
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
    """
    Results entry URL for scrape and ``competition.results_url``.

    Classic pages keep ``/index.asp``. All other URLs (ISU / Swiss Timing) get
    ``/index.htm`` when no index file is present.
    """
    u = (url or "").strip().rstrip("/")
    if not u:
        return ""
    lower = u.lower()
    if lower.endswith("/index.asp"):
        return u
    if lower.endswith("/index.htm") or lower.endswith("/index.html"):
        return u
    return f"{u}/index.htm"


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


def parse_disciplines_arg(raw: str | None) -> tuple[str, ...]:
    if not raw or not raw.strip():
        return DEFAULT_DISCIPLINES
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if len(parts) == 1 and parts[0].lower() == "all":
        return DISCIPLINE_CHOICES
    normalized: list[str] = []
    aliases = {
        "figure": DISCIPLINE_FIGURE_SKATING,
        "figure skating": DISCIPLINE_FIGURE_SKATING,
        "synchro": DISCIPLINE_SYNCHRONIZED_SKATING,
        "synchronized": DISCIPLINE_SYNCHRONIZED_SKATING,
        "synchronized skating": DISCIPLINE_SYNCHRONIZED_SKATING,
    }
    for part in parts:
        key = part.lower()
        if key in aliases:
            normalized.append(aliases[key])
            continue
        upper = part.upper()
        if upper not in DISCIPLINE_CHOICES:
            raise SystemExit(
                f"Invalid --disciplines value {part!r}; "
                f"use {', '.join(DISCIPLINE_CHOICES)}, All, or aliases "
                f"(figure, synchronized)."
            )
        normalized.append(upper)
    return tuple(dict.fromkeys(normalized))


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


# ISU Championship subtypes: long labels (official PDF) and short labels (mobile-list API).
ISU_CHAMPIONSHIP_EVENT_SUB_TYPES = frozenset(
    {
        # Official Communication / PDF wording
        "isu world figure skating championships",
        "isu world junior figure skating championships",
        "isu european figure skating championships",
        "isu four continents figure skating championships",
        "isu world synchronized skating championships",
        # api.isu-skating.com ``event_sub_type_name`` (see analysisTemp/figure_2526.csv)
        "world championships",
        "world junior championships",
        "european championships",
        "four continents championships",
        "world synchronized skating championships",
        "isu synchronized skating combined world & junior world championships",
        "synchronized skating world championships",
        "world synchronized championships",
        "olympic games",
    }
)

FAILURE_CSV_FIELDS = (
    "failure_stage",
    "discipline_title",
    "season",
    "season_year",
    "event_level",
    "event_name",
    "event_sub_type_name",
    "isu_event_url",
    "detailed_results_url",
    "normalized_results_url",
    "error",
)


def _normalize_event_label(value: str) -> str:
    return " ".join((value or "").lower().split())


def _name_indicates_isu_championship(name: str) -> bool:
    if not name:
        return False
    if any(
        phrase in name
        for phrase in (
            "four continents championships",
            "four continents figure skating championships",
            "european championships",
            "european figure skating championships",
            "world synchronized skating championships",
            "world junior figure skating championships",
            "world junior championships",
            "junior world championships",
            "world figure skating championships",
            "olympic winter games",
            "olympic games",
            "synchronized skating world championships",
            "synchronized skating junior world championships",
            "synchronized skating combined world",
        )
    ):
        return True
    if "world championships" in name:
        if "junior" not in name:
            return True
        return "junior world" in name or "world junior" in name
    return False


def is_isu_championship_event(
    event_name: str,
    event_sub_type_name: str = "",
) -> bool:
    """
    True for ISU Championship-tier events (type id 15).

    Uses ``event_sub_type_name`` when it matches known championship subtypes, and also
    checks ``event_name`` (the API often sends short subtypes like ``World Championships``
    rather than the long PDF labels).
    """
    sub_norm = _normalize_event_label(event_sub_type_name)
    if sub_norm and sub_norm in ISU_CHAMPIONSHIP_EVENT_SUB_TYPES:
        return True
    return _name_indicates_isu_championship(_normalize_event_label(event_name))


def is_world_championship_event(event_name: str) -> bool:
    """Backward-compatible alias; name-only check."""
    return is_isu_championship_event(event_name, "")


def inferred_competition_type_id(
    event_level: str,
    event_name: str,
    event_sub_type_name: str = "",
) -> int:
    if event_level == "International":
        return OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL_COMPETITION
    if is_isu_championship_event(event_name, event_sub_type_name):
        return OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP
    return OFFICIALS_COMPETITION_TYPE_ID_ISU_COMPETITION


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
    discipline_title: str,
    pagesize: int,
    timeout: float,
) -> list[dict]:
    payload = {
        "pagesize": pagesize,
        "discipline_title": discipline_title,
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
    disciplines: Iterable[str],
    event_levels: Iterable[str],
    year: int | None,
    pagesize: int,
    timeout: float,
    delay: float,
    limit: int | None,
    start_offset: int,
    quiet: bool,
) -> list[ResultRow]:
    rows: list[ResultRow] = []
    seen = 0
    fetched_at = _utc_now()
    for discipline_title in disciplines:
        for season in seasons:
            for event_level in event_levels:
                events = fetch_events_for_season(
                    session,
                    season,
                    event_level,
                    discipline_title,
                    pagesize,
                    timeout,
                )
                if not quiet:
                    print(
                        f"{discipline_title} | {season} {event_level}: "
                        f"{len(events)} events",
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
                    sub_type = str(event.get("event_sub_type_name") or "").strip()
                    detail_url = event_detail_page_url(event)
                    results_url, error = detailed_results_url_for_event(
                        session, event, timeout
                    )
                    status = "found" if results_url else "missing"
                    normalized = normalize_results_base_url(results_url)
                    if not quiet:
                        print(
                            f"{status}: {discipline_title} | {season} {event_level} | "
                            f"{name}",
                            file=sys.stderr,
                        )

                    competition_type_id = inferred_competition_type_id(
                        event_level, name, sub_type
                    )
                    rows.append(
                        ResultRow(
                            discipline_title=discipline_title,
                            season=season,
                            season_year=season_year_from_title(season),
                            event_level=event_level,
                            isu_event_id=str(event.get("event_id") or ""),
                            event_name=name,
                            event_sub_type_name=sub_type,
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


def failures_output_path(output_path: str, explicit: str | None = None) -> str:
    """Default failures CSV beside ``-o`` path, or a generic name for stdout output."""
    if explicit:
        return explicit
    if output_path and output_path != "-":
        base, ext = os.path.splitext(output_path)
        suffix = ext or ".csv"
        return f"{base}_failures{suffix}"
    return "isu_competition_failures.csv"


def resolve_failures_output_path(args: argparse.Namespace) -> str | None:
    """Return path when user asked for a failures file; else None (stderr summary only)."""
    if args.no_failures_file:
        return None
    if args.failures_output:
        return args.failures_output
    if args.write_failures:
        return failures_output_path(args.output)
    return None


def discovery_failures(rows: Iterable[ResultRow]) -> list[dict[str, str]]:
    """Events with no Detailed Results URL (``status == missing``)."""
    out: list[dict[str, str]] = []
    for row in rows:
        if row.status != "missing":
            continue
        out.append(
            {
                "failure_stage": "discover_missing_results",
                "discipline_title": row.discipline_title,
                "season": row.season,
                "season_year": row.season_year,
                "event_level": row.event_level,
                "event_name": row.event_name,
                "event_sub_type_name": row.event_sub_type_name,
                "isu_event_url": row.isu_event_url,
                "detailed_results_url": row.detailed_results_url,
                "normalized_results_url": row.normalized_results_url,
                "error": row.error or "no Detailed Results URL found",
            }
        )
    return out


def failure_record_from_row(row: ResultRow, *, stage: str, error: str) -> dict[str, str]:
    return {
        "failure_stage": stage,
        "discipline_title": row.discipline_title,
        "season": row.season,
        "season_year": row.season_year,
        "event_level": row.event_level,
        "event_name": row.event_name,
        "event_sub_type_name": row.event_sub_type_name,
        "isu_event_url": row.isu_event_url,
        "detailed_results_url": row.detailed_results_url,
        "normalized_results_url": row.normalized_results_url,
        "error": error,
    }


def write_failures_csv(path: str, failures: Iterable[dict[str, str]]) -> int:
    items = list(failures)
    if not items:
        return 0
    with open(path, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=FAILURE_CSV_FIELDS)
        writer.writeheader()
        for item in items:
            writer.writerow(item)
    return len(items)


def print_failures_summary(
    failures: list[dict[str, str]],
    *,
    failures_path: str | None = None,
    stream: TextIO | None = None,
) -> None:
    out = stream or sys.stderr
    if not failures:
        print("\nNo failed competitions.", file=out)
        return
    by_stage: dict[str, list[dict[str, str]]] = {}
    for f in failures:
        by_stage.setdefault(f["failure_stage"], []).append(f)
    print(f"\n=== Failed competitions ({len(failures)} total) ===", file=out)
    for stage, items in sorted(by_stage.items()):
        print(f"\n{stage} ({len(items)}):", file=out)
        for item in items:
            disc = item.get("discipline_title") or ""
            print(
                f"  - {item['event_name']} [{disc} | {item['event_level']}] "
                f"({item['season']})",
                file=out,
            )
            err = (item.get("error") or "").strip()
            if err:
                print(f"      {err}", file=out)
            if item.get("isu_event_url"):
                print(f"      {item['isu_event_url']}", file=out)
    if failures_path:
        print(f"\nFailures written to {failures_path}", file=out)


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
) -> tuple[int, int, list[dict[str, str]]]:
    eligible = [row for row in rows if row.normalized_results_url and row.status == "found"]
    existing = _load_existing_competition_base_urls() if skip_if_in_database else set()

    planned = [
        row
        for row in eligible
        if normalize_results_base_url(row.normalized_results_url) not in existing
    ]
    load_failures: list[dict[str, str]] = []

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
        return 0, len(planned), load_failures

    from database import get_db_session
    from database_loader import DatabaseLoader
    from downloadResults import scrape
    from ijs_scrape_log import configure as configure_scrape_logging
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
            start_date = _parse_iso_date(row.start_date)
            end_date = _parse_iso_date(row.end_date)
            type_id = competition_type_id_for_row(row, default_competition_type_id)
            qualifying, nqs, international = competition_load_flags_from_officials_type_id(
                type_id
            )
            try:
                if metadata_only:
                    db_loader.insert_competition(
                        row.event_name,
                        row.normalized_results_url,
                        row.season_year,
                        qualifying=qualifying,
                        nqs=nqs,
                        officials_analysis_competition_type_id=type_id,
                        international=international,
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
                        international=international,
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
                        international=international,
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
                loaded += 1
            except Exception as exc:
                db_session.rollback()
                load_failures.append(
                    failure_record_from_row(
                        row,
                        stage="load_error",
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
                if not quiet:
                    print(
                        f"FAILED load: {row.event_name}: {exc}",
                        file=sys.stderr,
                    )
            if delay:
                time.sleep(delay)
    finally:
        http_session.close()
        db_session.close()
    return loaded, len(planned), load_failures


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
        "--disciplines",
        default=None,
        help=(
            "Comma-separated ISU API discipline_title values. "
            f"Default: {DISCIPLINE_FIGURE_SKATING!r}. "
            f"Use All for {DISCIPLINE_FIGURE_SKATING!r} + {DISCIPLINE_SYNCHRONIZED_SKATING!r}, "
            "or aliases figure / synchronized."
        ),
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
        help=(
            "Include events with no Detailed Results URL in the main -o CSV. "
            "Missing events are always tracked for the failures summary/CSV."
        ),
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
            "Default inference: International=17, ISU Championships/Olympics=15, other ISU=16."
        ),
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce progress output.",
    )
    p.add_argument("--verbose", action="store_true", help="Verbose scraper logs with --load.")
    p.add_argument("--log-file", default=None, help="Optional DEBUG log file for --load.")
    p.add_argument(
        "--write-failures",
        action="store_true",
        help=(
            "Write failed competitions to a CSV file. "
            "Default path: <output-stem>_failures.csv (or isu_competition_failures.csv)."
        ),
    )
    p.add_argument(
        "--failures-output",
        default=None,
        metavar="PATH",
        help=(
            "CSV path for failed competitions (implies --write-failures). "
            "Discovery misses and load errors are included."
        ),
    )
    p.add_argument(
        "--no-failures-file",
        action="store_true",
        help="Do not write a failures CSV (stderr summary still printed).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    session = _session()
    try:
        seasons = parse_seasons_arg(args.seasons, session, args.timeout, args.year)
        disciplines = parse_disciplines_arg(args.disciplines)
        event_levels = parse_event_levels_arg(args.event_levels)
        rows = collect_result_rows(
            session,
            seasons,
            disciplines=disciplines,
            event_levels=event_levels,
            year=args.year,
            pagesize=args.pagesize,
            timeout=args.timeout,
            delay=args.delay,
            limit=args.limit,
            start_offset=args.start_offset,
            quiet=args.quiet,
        )
    finally:
        session.close()

    csv_rows = rows if args.include_missing else [r for r in rows if r.status == "found"]
    write_csv(args.output, csv_rows)
    found = sum(1 for row in rows if row.status == "found")
    missing = sum(1 for row in rows if row.status == "missing")
    if not args.quiet:
        print(
            f"wrote {len(rows)} rows to {args.output} ({found} found, {missing} missing)",
            file=sys.stderr,
        )

    all_failures: list[dict[str, str]] = discovery_failures(rows)
    load_failures: list[dict[str, str]] = []

    if args.load or args.dry_run:
        loaded, planned, load_failures = load_rows(
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
        all_failures.extend(load_failures)
        if not args.quiet:
            action = "planned" if args.dry_run else "loaded"
            print(f"{action} {loaded or planned} competitions", file=sys.stderr)
            if load_failures:
                print(
                    f"load failures: {len(load_failures)} of {planned} planned",
                    file=sys.stderr,
                )

    failures_path = resolve_failures_output_path(args)
    if failures_path is not None:
        n = write_failures_csv(failures_path, all_failures)
        if not args.quiet:
            print(
                f"wrote {n} failure row(s) to {failures_path}",
                file=sys.stderr,
            )

    print_failures_summary(all_failures, failures_path=failures_path)

    return 1 if all_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
