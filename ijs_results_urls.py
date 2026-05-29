"""
USFS IJS competition ``results_url`` helpers.

**Storage:** ``public.competition.results_url`` is the full results entry URL when known
(e.g. ends with ``/index.asp`` for classic events). FSM / Swiss Timing events are any URL
that does **not** end with ``/index.asp`` — there is no assumed ``/index.htm`` suffix.

Run migration ``011_competition_results_url_full_index.sql`` to append ``/index.asp`` to
legacy base-only classic rows already in the database.
"""

from __future__ import annotations

import re

_INDEX_ASP = "/index.asp"
_INDEX_HTM = "/index.htm"
_LEGACY_BASE_RE = re.compile(r"/leaderboard/results/\d{4}/\d+$", re.IGNORECASE)


def _strip_trailing_slash(url: str) -> str:
    return (url or "").strip().rstrip("/")


def results_url_for_storage(url: str) -> str:
    """Normalize a URL before persisting on ``competition.results_url``."""
    return _strip_trailing_slash(url)


def is_fsm_results_url(url: str) -> bool:
    """True when the URL is not a classic ``…/index.asp`` entry (FSM / Swiss Timing)."""
    u = (url or "").strip()
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

    Treats legacy base paths as equivalent to their ``/index.asp`` form so rows match
    before and after migration ``011``.
    """
    u = results_url_for_storage(url)
    lower = u.lower()
    if lower.endswith(_INDEX_ASP):
        return lower
    if is_legacy_base_results_url(u):
        return f"{u}{_INDEX_ASP}".lower()
    return lower


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
    for suffix in (_INDEX_ASP, _INDEX_HTM):
        if lower.endswith(suffix):
            return u[: -len(suffix)]
    return u


def results_url_lookup_keys(url: str) -> list[str]:
    """``results_url`` values to try when matching an existing competition row."""
    u = results_url_for_storage(url)
    keys: list[str] = [u]
    if is_legacy_base_results_url(u):
        keys.append(f"{u}{_INDEX_ASP}")
    else:
        base = scrape_join_base(u)
        if base and base != u:
            keys.append(base)
    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


# Backward-compatible name used by discover/load scripts.
def normalize_ijs_results_base_url(url: str) -> str:
    """Deprecated alias: use :func:`results_url_for_storage` for new code."""
    return results_url_for_storage(url)
