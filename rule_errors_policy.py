"""When to flag element rule errors during protocol scrape / DB load."""

from __future__ import annotations

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
