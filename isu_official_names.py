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
    full_name = f"{first_name} {last_name}".strip()
    return full_name, first_name or None, last_name or None
