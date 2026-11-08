"""
Completed competitions listed on results.usfigureskating.org.

The homepage is a Nuxt app over one public catalog:

    https://resultsprodsa.blob.core.windows.net/results/events/index.json

Each event that is published on IJS Live has a ``url`` field. That URL is the
leaderboard link to scrape. The year in a "View results" path
(``/events/2026/37256-fall-madison-open``) matches the IJS Live year for recent
events, and it does not for a lot of older ones. The catalog ``url`` is the
link USFS actually published, including slug paths the numeric probe cannot see.

Directory URLs (no ``index.asp``) are Swiss Timing / FSM pages. Those are stored
as ``index.htm``. Appending ``index.asp`` 404s.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

import requests

EVENTS_INDEX_URL = (
    "https://resultsprodsa.blob.core.windows.net/results/events/index.json"
)
_HTTP_TIMEOUT = 60
_USER_AGENT = "JudgingAnalysis/usfs-events-catalog"
_IJS_ENTRY_RE = re.compile(
    r"^https?://ijs\.usfigureskating\.org/leaderboard/results/"
    r"(?P<year>\d{4})/(?P<competition_id>[^/?#]+)"
    r"(?:/index\.(?P<ext>asp|htm|html))?/?$",
    re.IGNORECASE,
)
_SEASON_LABEL_RE = re.compile(r"^(?P<start>\d{4})-(?P<end>\d{2})$")
_SEASON_CODE_RE = re.compile(r"^(?P<start>\d{2})(?P<end>\d{2})$")


def current_season_label(today: date | None = None) -> str:
    """Catalog season label (``2026-27``). The season turns over in July."""
    today = today or date.today()
    start = today.year if today.month >= 7 else today.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def normalize_season_label(season: str) -> str:
    """
    Canonical ``YYYY-YY`` label.

    Accepts the catalog's en dash (``2026–27``), a hyphen, or a four-digit
    season code (``2627``).
    """
    text = (season or "").strip().replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", "", text)
    label = _SEASON_LABEL_RE.fullmatch(text)
    if label:
        return f"{label.group('start')}-{label.group('end')}"
    code = _SEASON_CODE_RE.fullmatch(text)
    if code and not text.startswith(("19", "20")):
        return f"20{code.group('start')}-{code.group('end')}"
    raise ValueError(
        f"Season must look like 2026-27 or 2627, got {season!r}."
    )


def season_code_from_label(season: str) -> str:
    """``2026-27`` -> ``2627`` for ``competition.year``."""
    label = normalize_season_label(season)
    match = _SEASON_LABEL_RE.fullmatch(label)
    assert match is not None
    return f"{int(match.group('start')) % 100:02d}{match.group('end')}"


def ijs_leaderboard_entry(url: str) -> tuple[str, str, str] | None:
    """
    ``(path year, competition id, entry URL)`` for an IJS Live leaderboard link.

    A bare directory becomes ``index.htm``. ``index.asp`` and ``index.htm``
    links are kept.
    """
    match = _IJS_ENTRY_RE.fullmatch((url or "").strip())
    if not match:
        return None
    year = match.group("year")
    competition_id = match.group("competition_id")
    ext = (match.group("ext") or "").lower()
    if ext == "html":
        ext = "htm"
    if not ext:
        ext = "htm"
    entry = (
        "https://ijs.usfigureskating.org/leaderboard/results/"
        f"{year}/{competition_id}/index.{ext}"
    )
    return year, competition_id, entry


def _location(event: dict[str, Any]) -> str:
    city = str(event.get("city") or "").strip()
    region = str(event.get("region") or "").strip()
    if city and region and region.lower() not in city.lower():
        return f"{city}, {region}"
    return city or region


def catalog_row(event: dict[str, Any], *, fetched_at_utc: str = "") -> dict[str, str] | None:
    """
    One discover-CSV row, or ``None`` when the event has no IJS Live URL.

    ``http_status`` is ``200`` because the catalog published the link. This
    does not fetch the leaderboard page.
    """
    parsed = ijs_leaderboard_entry(str(event.get("url") or ""))
    if parsed is None:
        return None
    year, competition_id, entry = parsed
    season = str(event.get("season") or "").strip()
    try:
        season_year = season_code_from_label(season) if season else ""
    except ValueError:
        season_year = ""
    return {
        "year": year,
        "competition_id": competition_id,
        "url": entry,
        "http_status": "200",
        "competition_name": " ".join(str(event.get("name") or "").split()),
        "start_date": str(event.get("start") or "")[:10],
        "end_date": str(event.get("end") or "")[:10],
        "location": _location(event),
        "fetched_at_utc": fetched_at_utc,
        "probe_error": "",
        "season_year": season_year,
        "status": str(event.get("status") or "").strip().lower(),
    }


def select_catalog_rows(
    events: list[dict[str, Any]],
    *,
    seasons: set[str] | None = None,
    statuses: set[str] | None = None,
    fetched_at_utc: str = "",
) -> tuple[list[dict[str, str]], int]:
    """
    Rows with an IJS Live URL, filtered by season label and status.

    ``seasons`` and ``statuses`` are normalized labels (``2026-27``, ``final``).
    ``None`` keeps every value. Returns ``(rows, skipped_without_ijs_url)``.
    """
    wanted_seasons = (
        {normalize_season_label(season) for season in seasons}
        if seasons is not None
        else None
    )
    wanted_status = (
        {status.strip().lower() for status in statuses} if statuses is not None else None
    )
    rows: list[dict[str, str]] = []
    skipped = 0
    for event in events:
        if not isinstance(event, dict):
            continue
        if wanted_status is not None:
            status = str(event.get("status") or "").strip().lower()
            if status not in wanted_status:
                continue
        if wanted_seasons is not None:
            try:
                label = normalize_season_label(str(event.get("season") or ""))
            except ValueError:
                continue
            if label not in wanted_seasons:
                continue
        row = catalog_row(event, fetched_at_utc=fetched_at_utc)
        if row is None:
            skipped += 1
            continue
        rows.append(row)
    rows.sort(key=lambda row: (row["start_date"], row["competition_name"]), reverse=True)
    return rows, skipped


def parse_events_index(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """Events plus ``generatedAt`` from an ``events/index.json`` document."""
    events = payload.get("events")
    if not isinstance(events, list):
        raise ValueError("Events catalog has no events list.")
    generated = str(payload.get("generatedAt") or "").strip()
    return events, generated


def fetch_events_index(
    session: requests.Session | None = None,
    url: str = EVENTS_INDEX_URL,
) -> tuple[list[dict[str, Any]], str]:
    """Download the public results catalog. The blob is gzip-encoded JSON."""
    http = session or requests.Session()
    response = http.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={"User-Agent": _USER_AGENT, "Accept-Encoding": "gzip"},
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except json.JSONDecodeError:
        payload = json.loads(response.content)
    if not isinstance(payload, dict):
        raise ValueError("Events catalog was not a JSON object.")
    return parse_events_index(payload)
