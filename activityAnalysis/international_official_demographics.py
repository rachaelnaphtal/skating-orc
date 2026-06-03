"""Age and time-in-grade for international officials listing reports."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

try:
    from activityAnalysis.international_listing_seasons import (
        format_listing_reference_july1,
        listing_reference_july1,
        listing_season_codes_for_projection,
    )
    from activityAnalysis.international_officials_data import (
        INTERNATIONAL_APPOINTMENT_TYPE_IDS,
        INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
        _nullable_int_for_sql,
    )
    from activityAnalysis.load_activity_data import get_engine
    from activityAnalysis.officials_analysis_models import Appointments, Officials
except ModuleNotFoundError:
    from international_listing_seasons import (
        format_listing_reference_july1,
        listing_reference_july1,
        listing_season_codes_for_projection,
    )
    from international_officials_data import (
        INTERNATIONAL_APPOINTMENT_TYPE_IDS,
        INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
        _nullable_int_for_sql,
    )
    from load_activity_data import get_engine
    from officials_analysis_models import Appointments, Officials

# Directory appointment_type.id (international roles).
APPOINTMENT_TYPE_ID_JUDGE = 12
APPOINTMENT_TYPE_ID_REFEREE = 13
APPOINTMENT_TYPE_ID_TECHNICAL_SPECIALIST = 14
APPOINTMENT_TYPE_ID_TECHNICAL_CONTROLLER = 15

# Directory discipline.id
DISCIPLINE_ID_SINGLES = 1
DISCIPLINE_ID_PAIRS = 8
DISCIPLINE_ID_SINGLES_PAIRS = 9
DISCIPLINE_ID_ICE_DANCE = 4
DISCIPLINE_ID_SYNCHRONIZED = 2


def grade_date_for_time_in_grade(
    achieved_date: date | None,
    appointed_date: date | None,
) -> date | None:
    """Prefer achieved date; fall back to appointed date."""
    return achieved_date or appointed_date


def first_year_credit_july1(grade_date: date) -> date:
    """
    First July 1 on which a full year in grade is credited.

    Achievements before July credit from July 1 of that calendar year.
    Achievements in July–December credit from the following July 1 (the July 1
    after achievement). Achievement on a listing reference July 1 is handled in
    :func:`years_in_grade_at_listing`.
    """
    if grade_date.month < 7:
        return date(grade_date.year, 7, 1)
    return date(grade_date.year + 1, 7, 1)


def years_in_grade_at_listing(
    grade_date: date | None,
    *,
    listing_season_code: int,
) -> int | None:
    """
    Whole years in grade as of the listing reference July 1.

    Example: achieved between Jul 1, 2025 and Jun 30, 2026 → 1 year for listing
    2627 (Jul 1, 2026) and 2 years for 2728 (Jul 1, 2027).
    """
    if grade_date is None:
        return None
    reference = listing_reference_july1(listing_season_code)
    if grade_date == reference and grade_date.month == 7 and grade_date.day == 1:
        return 1
    first = first_year_credit_july1(grade_date)
    if first > reference:
        return 0
    return reference.year - first.year + 1


def age_as_of_listing(
    date_of_birth: date | None,
    *,
    listing_season_code: int,
) -> int | None:
    """Age in whole years on the listing reference July 1."""
    if date_of_birth is None:
        return None
    reference = listing_reference_july1(listing_season_code)
    years = reference.year - date_of_birth.year
    if (reference.month, reference.day) < (date_of_birth.month, date_of_birth.day):
        years -= 1
    return years


def related_discipline_ids_for_tc_prerequisite(directory_discipline_id: Any) -> list[int]:
    """
    Directory disciplines used when counting International Judge / Referee years
    for TC promote (Singles/Pairs appointments apply to Singles or Pairs TC).
    """
    disc = _nullable_int_for_sql(directory_discipline_id)
    if disc is None:
        return []
    if disc == DISCIPLINE_ID_PAIRS:
        return [DISCIPLINE_ID_SINGLES, DISCIPLINE_ID_PAIRS, DISCIPLINE_ID_SINGLES_PAIRS]
    if disc == DISCIPLINE_ID_SINGLES_PAIRS:
        return [DISCIPLINE_ID_SINGLES, DISCIPLINE_ID_SINGLES_PAIRS]
    if disc == DISCIPLINE_ID_SYNCHRONIZED:
        return [DISCIPLINE_ID_SYNCHRONIZED]
    return [disc]


def load_official_international_appointment_rows(
    official_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    """All directory international appointments (types 12–16) per official."""
    ids = sorted({int(x) for x in official_ids if x is not None})
    out: dict[int, list[dict[str, Any]]] = {i: [] for i in ids}
    if not ids:
        return out
    with Session(get_engine()) as session:
        stmt = (
            select(
                Appointments.official_id,
                Appointments.appointment_type_id,
                Appointments.discipline_id,
                Appointments.level_id,
                Appointments.achieved_date,
                Appointments.appointed_date,
                Appointments.active,
            )
            .where(
                Appointments.official_id.in_(ids),
                Appointments.appointment_type_id.in_(list(INTERNATIONAL_APPOINTMENT_TYPE_IDS)),
            )
            .order_by(
                Appointments.official_id,
                Appointments.appointment_type_id,
                Appointments.discipline_id,
            )
        )
        for row in session.execute(stmt).mappings().all():
            oid = int(row["official_id"])
            out.setdefault(oid, []).append(dict(row))
    return out


def first_listing_season_meeting_min_years_on_grade_date(
    grade_date: date | None,
    min_years: int,
) -> int | None:
    """First listing season when ``min_years`` are credited on a single grade date."""
    if grade_date is None:
        return None
    for code in listing_season_codes_for_projection():
        if (years_in_grade_at_listing(grade_date, listing_season_code=code) or 0) >= int(
            min_years
        ):
            return int(code)
    return None


def first_listing_season_meeting_appointment_criteria(
    rows: list[dict[str, Any]],
    min_years: int,
    *,
    appointment_type_id: int,
    level_id: int | None = None,
    discipline_ids: list[int] | None = None,
) -> int | None:
    """First listing season when appointment-criteria years reach ``min_years``."""
    for code in listing_season_codes_for_projection():
        if (
            max_years_for_appointment_criteria(
                rows,
                listing_season_code=code,
                appointment_type_id=appointment_type_id,
                level_id=level_id,
                discipline_ids=discipline_ids,
            )
            >= int(min_years)
        ):
            return int(code)
    return None


def max_years_for_appointment_criteria(
    rows: list[dict[str, Any]],
    *,
    listing_season_code: int,
    appointment_type_id: int,
    level_id: int | None = None,
    discipline_ids: list[int] | None = None,
) -> int:
    """
    Whole years in grade (July 1 listing reference) for matching directory appointments.

    Uses the earliest achieved/appointed date among matching rows (longest tenure).
    """
    grade_dates: list[date] = []
    allowed_disc = (
        {_nullable_int_for_sql(d) for d in discipline_ids}
        if discipline_ids is not None
        else None
    )
    for row in rows:
        if int(row["appointment_type_id"]) != int(appointment_type_id):
            continue
        row_level = _nullable_int_for_sql(row.get("level_id"))
        if level_id is not None and row_level != int(level_id):
            continue
        row_disc = _nullable_int_for_sql(row.get("discipline_id"))
        if allowed_disc is not None and row_disc not in allowed_disc:
            continue
        gd = grade_date_for_time_in_grade(
            row.get("achieved_date"),
            row.get("appointed_date"),
        )
        if gd is not None:
            grade_dates.append(gd)
    if not grade_dates:
        return 0
    earliest = min(grade_dates)
    return years_in_grade_at_listing(earliest, listing_season_code=listing_season_code) or 0


def years_detail_label(*, actual: int, required: int, listing_season_code: int) -> str:
    as_of = format_listing_reference_july1(listing_season_code)
    return f"{actual}/{required} years ({as_of})"


def load_official_birthdates(official_ids: list[int]) -> dict[int, date | None]:
    ids = sorted({int(x) for x in official_ids if x is not None})
    if not ids:
        return {}
    out: dict[int, date | None] = {i: None for i in ids}
    with Session(get_engine()) as session:
        stmt = select(Officials.id, Officials.date_of_birth).where(Officials.id.in_(ids))
        for oid, dob in session.execute(stmt).all():
            out[int(oid)] = dob
    return out


def grade_date_from_appointment_contexts(
    contexts: dict[tuple[int, int, int | None], dict[str, Any]],
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
) -> date | None:
    """
    Grade date for one directory appointment row.

    International Data / Video Operator rows are collapsed with ``discipline_id``
    NULL in the summary; use the earliest achieved/appointed date across all
    per-discipline IDVO directory appointments for that official.
    """
    key = (
        int(official_id),
        int(appointment_type_id),
        _nullable_int_for_sql(discipline_id),
    )
    if key in contexts:
        ctx = contexts[key]
        return grade_date_for_time_in_grade(
            ctx.get("achieved_date"),
            ctx.get("appointed_date"),
        )

    if (
        int(appointment_type_id) == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID
        and _nullable_int_for_sql(discipline_id) is None
    ):
        dates: list[date] = []
        for (oid, atid, _disc), ctx in contexts.items():
            if oid != int(official_id) or atid != int(appointment_type_id):
                continue
            gd = grade_date_for_time_in_grade(
                ctx.get("achieved_date"),
                ctx.get("appointed_date"),
            )
            if gd is not None:
                dates.append(gd)
        return min(dates) if dates else None

    return None


def load_grade_dates_for_appointments(
    keys: list[tuple[int, int, int | None]],
) -> dict[tuple[int, int, int | None], date | None]:
    """Grade date per (official_id, appointment_type_id, discipline_id)."""
    try:
        from activityAnalysis.international_requirements import _batch_appointment_contexts
    except ModuleNotFoundError:
        from international_requirements import _batch_appointment_contexts

    official_ids = sorted({int(k[0]) for k in keys})
    contexts = _batch_appointment_contexts(official_ids)
    return {
        key: grade_date_from_appointment_contexts(
            contexts,
            official_id=key[0],
            appointment_type_id=key[1],
            discipline_id=key[2],
        )
        for key in keys
    }


def appointment_demographics_row(
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
    listing_season_code: int,
    date_of_birth: date | None = None,
    grade_date: date | None = None,
    birthdates: dict[int, date | None] | None = None,
    grade_dates: dict[tuple[int, int, int | None], date | None] | None = None,
) -> dict[str, int | None]:
    """Age and years in grade for one appointment at a listing season."""
    disc = _nullable_int_for_sql(discipline_id)
    key = (int(official_id), int(appointment_type_id), disc)

    if date_of_birth is None and birthdates is not None:
        date_of_birth = birthdates.get(int(official_id))
    if grade_date is None and grade_dates is not None:
        grade_date = grade_dates.get(key)

    return {
        "age_as_of_listing": age_as_of_listing(
            date_of_birth, listing_season_code=listing_season_code
        ),
        "years_in_grade": years_in_grade_at_listing(
            grade_date, listing_season_code=listing_season_code
        ),
    }


def enrich_summary_with_listing_demographics(
    summary: pd.DataFrame,
    *,
    listing_season_code: int,
) -> pd.DataFrame:
    """Add ``age_as_of_listing`` and ``years_in_grade`` columns to a summary frame."""
    if summary.empty:
        return summary
    out = summary.copy()
    official_ids = out["official_id"].astype(int).unique().tolist()
    birthdates = load_official_birthdates(official_ids)
    keys = [
        (
            int(r["official_id"]),
            int(r["appointment_type_id"]),
            _nullable_int_for_sql(r.get("discipline_id")),
        )
        for _, r in out.iterrows()
    ]
    grade_dates = load_grade_dates_for_appointments(keys)
    ages: list[int | None] = []
    years: list[int | None] = []
    for _, row in out.iterrows():
        demo = appointment_demographics_row(
            official_id=int(row["official_id"]),
            appointment_type_id=int(row["appointment_type_id"]),
            discipline_id=row.get("discipline_id"),
            listing_season_code=listing_season_code,
            birthdates=birthdates,
            grade_dates=grade_dates,
        )
        ages.append(demo["age_as_of_listing"])
        years.append(demo["years_in_grade"])
    out["age_as_of_listing"] = ages
    out["years_in_grade"] = years
    return out
