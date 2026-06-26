"""When to flag element rule errors during protocol scrape / DB load."""

from __future__ import annotations

import re
from datetime import date, datetime

# Competitions that start before this date use legacy marking rules; do not flag.
MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS = date(2018, 7, 1)

# PCS fall caps apply from 2024-25 onward (``competition.year`` season code >= 2425).
MIN_PCS_FALL_RULE_ERROR_SEASON_YEAR = "2425"
MIN_PCS_FALL_RULE_ERROR_START_DATE = date(2024, 7, 1)


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


def should_flag_pcs_fall_rule_errors(
    competition_year: str | int | None = None,
    competition_start_date: date | datetime | str | None = None,
    competition_end_date: date | datetime | str | None = None,
) -> bool:
    """
    Return True when PCS fall-cap rule errors should run.

    Uses ``competition.year`` (season code, e.g. ``2425``) when present; otherwise
    falls back to competition start/end dates (>= 2024-07-01).
    """
    year_text = str(competition_year).strip() if competition_year is not None else ""
    if year_text:
        return year_text >= MIN_PCS_FALL_RULE_ERROR_SEASON_YEAR
    start = _coerce_date(competition_start_date)
    if start is not None:
        return start >= MIN_PCS_FALL_RULE_ERROR_START_DATE
    end = _coerce_date(competition_end_date)
    if end is not None:
        return end >= MIN_PCS_FALL_RULE_ERROR_START_DATE
    return True


# ``public.discipline_type.id``: Singles, Pairs, Ice Dance, Solo Dance, Synchronized.
PCS_FALL_RULE_DISCIPLINE_TYPE_IDS = frozenset({1, 2, 3, 4, 5})


_SINGLES_SEGMENT_TOKENS = frozenset({"WOMEN", "MEN", "BOYS", "GIRLS", "LADIES", "LADY","GIRL","BOY"})
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


def segment_supports_pcs_fall_rule_errors(event_name: str) -> bool:
    """
    True when PCS fall-cap rule errors apply to this segment name.

    Covers singles, pairs, ice dance, solo dance, and synchronized skating.
    """
    if segment_supports_element_rule_errors(event_name):
        return True
    upper = (event_name or "").upper()
    if "SYNCHRO" in upper or "SYNCHRONIZED" in upper:
        return True
    if "SOLO" in upper and "DANCE" in upper:
        return True
    if "ICE" in upper and "DANCE" in upper:
        return True
    tokens = _segment_name_tokens(event_name)
    if "DANCE" in upper and tokens & {"RHYTHM", "PATTERN", "FREE"}:
        return True
    return False
