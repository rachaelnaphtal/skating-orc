"""
Normalize and parse names from ISU Communication official lists.

ISU PDF lines often look like: ``Lynch Susan M., Ms.`` (family name first, then given).
"""

from __future__ import annotations

import re

_TITLE_SUFFIX_RE = re.compile(r",\s*(Mr\.|Ms\.|Mrs\.)\s*$", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_person_name(s: str | None) -> str:
    if not s:
        return ""
    return _WHITESPACE_RE.sub(" ", s.lower().strip())


def full_name_from_first_last(
    first_name: str | None,
    last_name: str | None,
    *,
    raw_fallback: str = "",
) -> str:
    """Western display order: given name(s), then family name."""
    first = (first_name or "").strip()
    last = (last_name or "").strip()
    if first and last:
        return f"{first} {last}"
    if first:
        return first
    if last:
        return last
    return (raw_fallback or "").strip()


def parse_isu_list_name(raw: str) -> tuple[str, str | None, str | None]:
    """
    Parse an ISU list name into ``(full_name, first_name, last_name)``.

    ``full_name`` uses Western order (given … family) for display and fuzzy matching.
    """
    text = (raw or "").strip()
    if not text:
        return "", None, None
    text = _TITLE_SUFFIX_RE.sub("", text).strip().rstrip(",").strip()
    if not text:
        return "", None, None

    parts = text.split()
    if len(parts) < 2:
        return text, text, None

    last_name = parts[0]
    first_name = " ".join(parts[1:])
    full_name = full_name_from_first_last(first_name, last_name, raw_fallback=text)
    return full_name, first_name or None, last_name or None
