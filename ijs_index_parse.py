"""Parse competition dates and location from ijsLive ``index.asp`` HTML (current and legacy)."""

from __future__ import annotations

import re
from datetime import datetime

from bs4 import BeautifulSoup

_SLASH_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")


def ijs_index_h3_text_looks_like_slash_date_line(text: str) -> bool:
    """True when ``text`` contains at least one US ``MM/DD/YYYY`` (ijsLive header dates)."""
    return bool(_SLASH_DATE_RE.search(text or ""))


def parse_ijs_index_slash_date_h3_line(h3_text: str) -> tuple[str, str]:
    """
    One ``<h3>`` line: ``MM/DD/YYYY (TZ)``, ``start - end``, or ``start - end (TZ)``.
    Returns ``(start, end)`` as ``MM/DD/YYYY`` strings, or ``("", "")`` if not a slash-date line.
    """
    if not ijs_index_h3_text_looks_like_slash_date_line(h3_text):
        return "", ""
    raw = " ".join(str(h3_text).split())
    before_tz = raw.split("(", 1)[0].strip()
    if " - " in before_tz:
        left, right = before_tz.split(" - ", 1)
        l, r = left.strip(), right.strip()
        if ijs_index_h3_text_looks_like_slash_date_line(
            l
        ) and ijs_index_h3_text_looks_like_slash_date_line(r):
            return l, r
        return "", ""
    if before_tz:
        return before_tz, before_tz
    return "", ""


def ijs_index_location_from_h3_texts(h3_texts: list[str]) -> str:
    """Join non-date ``<h3>`` lines (venue, city, etc.) for header blocks without slash dates."""
    parts: list[str] = []
    for t in h3_texts:
        s = " ".join(str(t).split())
        if not s or ijs_index_h3_text_looks_like_slash_date_line(s):
            continue
        parts.append(s)
    return ", ".join(parts)


def _year_hint_from_ijs_footer(soup: BeautifulSoup) -> int | None:
    """Legacy footers often include ``Tuesday, Nov 19, 2013, 03:56 PM`` (local stamp)."""
    for el in soup.find_all(["p", "font"]):
        t = " ".join(el.get_text().split())
        m = re.search(
            r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
            r"[A-Za-z]+\s+\d{1,2},\s*(\d{4})\b",
            t,
        )
        if m:
            return int(m.group(1))
        m = re.search(r"\b[A-Za-z]+\s+\d{1,2},\s*(\d{4})\b", t)
        if m:
            return int(m.group(1))
    return None


def _year_hint_from_title(soup: BeautifulSoup) -> int | None:
    h2 = soup.find("h2", class_="title")
    if not h2:
        h2 = soup.find("h2")
    if h2:
        m = re.search(r"\b(19|20)\d{2}\b", h2.get_text())
        if m:
            return int(m.group(0))
    tit = soup.find("title")
    if tit:
        m = re.search(r"\b(19|20)\d{2}\b", tit.get_text())
        if m:
            return int(m.group(0))
    return None


def _year_hint_from_results_url(url: str) -> int | None:
    m = re.search(r"/leaderboard/results/(\d{4})/", url or "")
    if m:
        return int(m.group(1))
    return None


def _parse_day_sort_cell(text: str, year: int) -> datetime | None:
    raw = " ".join(text.split())
    if not raw:
        return None
    for fmt in ("%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(f"{raw} {year}", fmt)
        except ValueError:
            continue
    return None


def infer_ijs_index_dates_from_day_sort_table(
    soup: BeautifulSoup, page_url: str = ""
) -> tuple[str, str]:
    """
    Older templates omit header dates; ``table#daySort`` has weekday cells like ``November 20``.
    Year: footer timestamp (preferred), then title/title tag, then ``…/results/YYYY/`` in URL.
    """
    table = soup.select_one("table#daySort tbody")
    if table is None:
        return "", ""
    seen: set[str] = set()
    day_strings: list[str] = []
    for td in table.select("tr td.date"):
        raw = " ".join(td.get_text().split())
        if raw and raw not in seen:
            seen.add(raw)
            day_strings.append(raw)
    if not day_strings:
        return "", ""
    year = (
        _year_hint_from_ijs_footer(soup)
        or _year_hint_from_title(soup)
        or _year_hint_from_results_url(page_url)
    )
    if not year:
        return "", ""
    parsed: list[datetime] = []
    for ds in day_strings:
        dt = _parse_day_sort_cell(ds, year)
        if dt:
            parsed.append(dt)
    if not parsed:
        return "", ""
    mn, mx = min(parsed), max(parsed)
    return mn.strftime("%m/%d/%Y"), mx.strftime("%m/%d/%Y")


def ijs_index_start_end_and_location(
    soup: BeautifulSoup, page_url: str = ""
) -> tuple[str, str, str]:
    """
    Dates from the first slash-date ``<h3>``, else from ``#daySort`` schedule cells.
    Location: all non-slash-date ``<h3>`` texts joined (venue, city, …).
    """
    h3_texts = [h.get_text() for h in soup.find_all("h3")]
    start_date, end_date = "", ""
    for t in h3_texts:
        s, e = parse_ijs_index_slash_date_h3_line(t)
        if s and e:
            start_date, end_date = s, e
            break
    if not start_date:
        start_date, end_date = infer_ijs_index_dates_from_day_sort_table(soup, page_url)
    location = ijs_index_location_from_h3_texts(h3_texts)
    return start_date, end_date, location
