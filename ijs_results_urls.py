"""
USFS IJS competition ``results_url`` helpers.

**Storage:** ``public.competition.results_url`` is the canonical results entry URL:

- Classic USFS IJS: ``…/index.asp`` (legacy base paths without a suffix get ``/index.asp``).
- FSM / Swiss Timing / ISU: ``…/index.htm`` (directory URLs without a suffix get ``/index.htm``).

Run migration ``011_competition_results_url_full_index.sql`` to append ``/index.asp`` to
legacy base-only classic rows already in the database.
"""

from __future__ import annotations

import re

_INDEX_ASP = "/index.asp"
_INDEX_HTM = "/index.htm"
_INDEX_HTML = "/index.html"
_LEGACY_BASE_RE = re.compile(r"/leaderboard/results/\d{4}/\d+$", re.IGNORECASE)
_USFS_RESULTS_EVENT_RE = re.compile(
    r"^https?://results\.usfigureskating\.org/events/(?P<year>\d{4})/(?P<slug>\d+-[a-z0-9-]+)",
    re.IGNORECASE,
)


def usfs_results_event_url(url: str) -> str | None:
    """
    Canonical event URL on results.usfigureskating.org.

    Schedule, category, and officials links collapse to
    ``https://results.usfigureskating.org/events/{year}/{id}-{slug}``.
    """
    raw = (url or "").strip().split("#", 1)[0].split("?", 1)[0]
    match = _USFS_RESULTS_EVENT_RE.match(raw)
    if not match:
        return None
    year = match.group("year")
    slug = match.group("slug").lower()
    return f"https://results.usfigureskating.org/events/{year}/{slug}"


def _strip_trailing_slash(url: str) -> str:
    return (url or "").strip().rstrip("/")


def results_url_for_storage(url: str) -> str:
    """Normalize a URL before persisting on ``competition.results_url``."""
    u = _strip_trailing_slash(url)
    if not u:
        return ""
    event_url = usfs_results_event_url(u)
    if event_url:
        return event_url
    lower = u.lower()
    if lower.endswith(_INDEX_ASP):
        return u
    if lower.endswith(_INDEX_HTM) or lower.endswith(_INDEX_HTML):
        return u
    if is_legacy_base_results_url(u):
        return f"{u}{_INDEX_ASP}"
    return f"{u}{_INDEX_HTM}"


def is_fsm_results_url(url: str) -> bool:
    """True when the URL is not a classic ``…/index.asp`` entry (FSM / Swiss Timing)."""
    u = (url or "").strip()
    if usfs_results_event_url(u):
        return False
    return not u.lower().endswith(_INDEX_ASP)


def is_legacy_base_results_url(url: str) -> bool:
    """True for unmigrated rows: base path without ``/index.asp`` or ``/index.htm``."""
    u = _strip_trailing_slash(url)
    if not u:
        return False
    lower = u.lower()
    if lower.endswith(_INDEX_ASP) or lower.endswith(_INDEX_HTM):
        return False
    return bool(_LEGACY_BASE_RE.search(u))


def results_url_dedupe_key(url: str) -> str:
    """
    Case-folded key for set membership (discover skip-if-in-database).

    Canonicalizes via :func:`results_url_for_storage` so ``wc2007`` and
    ``wc2007/index.htm`` dedupe together.
    """
    return results_url_for_storage(url).lower()


def results_page_url(stored: str | None) -> str | None:
    """Browser link target — stored URL as-is."""
    if not stored or not str(stored).strip():
        return None
    return str(stored).strip()


def competition_index_fetch_url(stored: str) -> str:
    """First HTML page to fetch when scraping or reading competition metadata."""
    return results_url_for_storage(stored)


def scrape_join_base(stored: str) -> str:
    """
    Base path for resolving relative segment links during scrape/backfill.

    Strips a trailing ``index.asp`` / ``index.htm`` when present; otherwise returns the
    stored path (FSM entry URL or legacy base before migration).
    """
    u = _strip_trailing_slash(results_url_for_storage(stored))
    lower = u.lower()
    for suffix in (_INDEX_ASP, _INDEX_HTM, _INDEX_HTML):
        if lower.endswith(suffix):
            return u[: -len(suffix)]
    return u


def results_url_lookup_keys(url: str) -> list[str]:
    """``results_url`` values to try when matching an existing competition row."""
    canonical = results_url_for_storage(url)
    keys: list[str] = [canonical]
    base = scrape_join_base(canonical)
    if base and base != canonical:
        keys.append(base)
    raw = _strip_trailing_slash(url)
    if raw and raw != canonical and raw != base:
        keys.append(raw)
    if is_legacy_base_results_url(raw):
        keys.append(f"{raw}{_INDEX_ASP}")
    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


# Backward-compatible name used by discover/load scripts.
def normalize_ijs_results_base_url(url: str) -> str:
    """Deprecated alias: use :func:`results_url_for_storage` for new code."""
    return results_url_for_storage(url)
