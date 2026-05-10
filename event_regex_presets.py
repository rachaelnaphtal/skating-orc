"""Presets for segment/event name filtering when scraping (``downloadResults.scrape`` / ``event_regex``)."""

from __future__ import annotations

import re
from typing import Iterable

LEVEL_CHOICES = ("Novice", "Junior", "Senior")
DISCIPLINE_CHOICES = ("Singles", "Pairs", "Dance")


def _level_branches(levels: tuple[str, ...]) -> str:
    """OR'd branches for the level lookahead; Senior also matches *Championship* (common senior-level naming)."""
    parts: list[str] = []
    for x in levels:
        if x == "Senior":
            parts.append(
                f"(?:{re.escape('Senior')}|{re.escape('Championship')})"
            )
        else:
            parts.append(re.escape(x))
    return "|".join(parts)


def build_event_regex_from_presets(
    levels: Iterable[str],
    disciplines: Iterable[str],
) -> str:
    """Build a regex for ``re.match`` on each parsed event / segment label.

    - Preset patterns are **case-insensitive** (``(?i)``), so labels like
      ``CHAMPIONSHIP_ICE_DANCE_FREE_DANCE`` match the same tokens as mixed-case names.
    - Chosen levels are OR'd. **Senior** also matches **Championship** (synonym on many protocols).
    - Chosen disciplines are OR'd, using the same loose tokens as typical scrape examples
      (``Women`` / ``Men`` / … for singles-style labels; ``Pairs``; ``Dance`` for ice/rhythm/etc.).
    - If both level and discipline presets are set, the name must match both (AND), in any order.

    Custom regex from ``effective_event_regex`` is passed through unchanged (add ``(?i)`` yourself if needed).
    """
    lookaheads: list[str] = []
    levels = tuple(levels or ())
    disciplines = tuple(disciplines or ())
    if levels:
        alts = _level_branches(levels)
        lookaheads.append(f"(?=.*(?:{alts}))")
    if disciplines:
        d_alts: list[str] = []
        for d in disciplines:
            if d == "Singles":
                d_alts.append(r"Women|Men|Girls|Boys|Ladies|Lady|Singles?")
            elif d == "Pairs":
                d_alts.append(r"Pairs?|Pair")
            elif d == "Dance":
                d_alts.append(r"Dance")
        inner = "|".join(f"(?:{x})" for x in d_alts)
        lookaheads.append(f"(?=.*(?:{inner}))")
    if not lookaheads:
        return ""
    return "(?i)" + "".join(lookaheads) + ".*"


def effective_event_regex(
    custom: str,
    levels: Iterable[str],
    disciplines: Iterable[str],
) -> str:
    """If ``custom`` is non-empty, return it verbatim; otherwise return the composed preset regex.

    Preset output includes ``(?i)`` and Senior/Championship handling. Custom patterns are not modified
    (prefix with ``(?i)`` when you need case-insensitivity).
    """
    c = (custom or "").strip()
    if c:
        return c
    return build_event_regex_from_presets(levels, disciplines)
