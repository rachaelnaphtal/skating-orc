"""When to flag element rule errors during protocol scrape / DB load."""

from __future__ import annotations

import re
from datetime import date, datetime

# Competitions that start before this date use legacy marking rules; do not flag.
MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS = date(2018, 7, 1)


def _coerce_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def should_flag_rule_errors(
    competition_start_date: date | datetime | str | None = None,
    competition_end_date: date | datetime | str | None = None,
) -> bool:
    """
    Return True when scraped rule-error detection should run.

    Uses competition start date when present; otherwise end date. If both are
    missing, rule errors are still flagged (unknown metadata — keep prior behavior).
    """
    start = _coerce_date(competition_start_date)
    if start is not None:
        return start >= MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS
    end = _coerce_date(competition_end_date)
    if end is not None:
        return end >= MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS
    return True


_SINGLES_SEGMENT_TOKENS = frozenset({"WOMEN", "MEN", "BOYS", "GIRLS", "LADIES", "LADY"})
_PAIRS_SEGMENT_TOKENS = frozenset({"PAIR", "PAIRS"})


def _segment_name_tokens(event_name: str) -> set[str]:
    normalized = (event_name or "").upper().replace("-", "_")
    return {t for t in re.split(r"_+", normalized) if t}


def segment_supports_element_rule_errors(event_name: str) -> bool:
    """
    True when element rule-error detection applies to this segment name.

    Covers USFS-style labels (``Junior Women Free Skate``) and ISU FSM names
    (``MEN_SHORT_PROGRAM``, ``Men_Single_Skating___Short_Program``,
    ``Pair_Skating___Free_Skating``). Ice dance and other disciplines are excluded.
    """
    tokens = _segment_name_tokens(event_name)
    if tokens & _SINGLES_SEGMENT_TOKENS:
        return True
    if tokens & _PAIRS_SEGMENT_TOKENS:
        return True
    upper = (event_name or "").upper()
    if "SINGLE" in upper and ("MEN" in upper or "WOMEN" in upper):
        return True
    if "PAIR" in upper and "SKATING" in upper:
        return True
    return False


def segment_is_pairs_for_rule_errors(event_name: str) -> bool:
    """True when pairs marking rules should be used (vs singles)."""
    tokens = _segment_name_tokens(event_name)
    if tokens & _PAIRS_SEGMENT_TOKENS:
        return True
    upper = (event_name or "").upper()
    return "PAIR" in upper and "SKATING" in upper
