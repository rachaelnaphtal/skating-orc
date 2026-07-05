"""
International judge availability workbook ingest and reporting.

Loads a Google Form export (one row per respondent, columns per competition event)
and joins to directory **International Judge** appointments at International or ISU
Championship level in Singles/Pairs or Ice Dance.
"""

from __future__ import annotations

import os
import re
from typing import Any

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

try:
    from activityAnalysis.international_officials_data import (
        get_international_officials_for_filters,
    )
    from activityAnalysis.international_official_demographics import (
        OFFICIAL_AGE_OUT_ON_JULY1,
        age_as_of_listing,
        load_official_birthdates,
    )
    from activityAnalysis.international_listing_seasons import (
        REPORT_LISTING_SEASON_DEFAULT,
    )
    from activityAnalysis.international_requirements import (
        DIRECTORY_LEVEL_ID_INTERNATIONAL,
        DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
    )
    from activityAnalysis.load_activity_data import (
        DISC_DANCE_ID,
        DISC_SINGLES_PAIRS_ID,
        get_engine,
    )
    from activityAnalysis.officials_analysis_models import Officials
    from activityAnalysis.qualifying_availability_ingest import (
        normalize_qualifying_availability_cell,
        resolve_workbook_sheet_name,
    )
except ModuleNotFoundError:
    from international_officials_data import get_international_officials_for_filters
    from international_official_demographics import (
        OFFICIAL_AGE_OUT_ON_JULY1,
        age_as_of_listing,
        load_official_birthdates,
    )
    from international_listing_seasons import REPORT_LISTING_SEASON_DEFAULT
    from international_requirements import (
        DIRECTORY_LEVEL_ID_INTERNATIONAL,
        DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
    )
    from load_activity_data import DISC_DANCE_ID, DISC_SINGLES_PAIRS_ID, get_engine
    from officials_analysis_models import Officials
    from qualifying_availability_ingest import (
        normalize_qualifying_availability_cell,
        resolve_workbook_sheet_name,
    )

INTERNATIONAL_JUDGE_APPOINTMENT_TYPE_ID = 12

_DEFAULT_WORKBOOK = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "InternationalAvailability20262.xlsx",
)

_FIRST_NAME_HEADERS = frozenset({"first name"})
_LAST_NAME_HEADERS = frozenset({"last name"})
_EMAIL_HEADERS = frozenset({"email address", "email"})

_SP_LEVEL_HEADER = "international judging level - singles/pairs"
_DANCE_LEVEL_HEADER = "international judging level - ice dance"

_SUPPLEMENTAL_HEADER_PREFIXES = (
    "are you able to attend more than one competition",
    "do you have a valid passport",
    "if someone is needed for back-to-back",
    "judges, do you need any specific activity",
    "please provide us with any other information",
)

DISCIPLINE_FILTER_ALL = "(All)"
DISCIPLINE_FILTER_SINGLES_PAIRS = "Singles/Pairs"
DISCIPLINE_FILTER_DANCE = "Dance"

LEVEL_FILTER_ALL = "(All)"
LEVEL_FILTER_INTERNATIONAL = "International"
LEVEL_FILTER_ISU = "ISU"

_AVAILABILITY_DISPLAY = {
    "available": "Available",
    "not_available": "Not available",
    "no_response": "No response",
    "does_not_apply": "Does not apply",
    "unknown": "Unknown",
}

_REPORT_IDENTITY_COLUMNS = ("Official", "Discipline", "Level")

# Age-out reference for this report: July 1, 2026 (listing season 2627).
AVAILABILITY_AGE_LISTING_SEASON_CODE = REPORT_LISTING_SEASON_DEFAULT


def official_is_eligible_for_availability_report(
    date_of_birth: object,
    *,
    listing_season_code: int = AVAILABILITY_AGE_LISTING_SEASON_CODE,
    age_out_at: int = OFFICIAL_AGE_OUT_ON_JULY1,
) -> bool:
    """
    True when the official should appear on the availability report.

    Officials aged out (≥ ``age_out_at`` on listing July 1) are excluded.
    Unknown birthdates are kept.
    """
    if date_of_birth is None or (isinstance(date_of_birth, float) and pd.isna(date_of_birth)):
        return True
    age = age_as_of_listing(date_of_birth, listing_season_code=listing_season_code)
    if age is None:
        return True
    return int(age) < int(age_out_at)


def _filter_appointments_below_age_out(
    appointments: pd.DataFrame,
    *,
    birthdates: dict[int, object] | None = None,
    engine=None,
) -> tuple[pd.DataFrame, int]:
    if appointments.empty:
        return appointments, 0
    official_ids = appointments["official_id"].astype(int).unique().tolist()
    if birthdates is None:
        birthdates = load_official_birthdates(official_ids)
    eligible_ids = {
        oid
        for oid in official_ids
        if official_is_eligible_for_availability_report(birthdates.get(int(oid)))
    }
    filtered = appointments.loc[
        appointments["official_id"].astype(int).isin(eligible_ids)
    ].copy()
    excluded = int(appointments["official_id"].astype(int).nunique()) - len(eligible_ids)
    return filtered, excluded


def default_availability_workbook_path() -> str:
    return _DEFAULT_WORKBOOK


def _normalize_header(header: object) -> str:
    return re.sub(r"\s+", " ", str(header or "").strip()).lower()


def _classify_workbook_headers(headers: list[str]) -> dict[str, Any]:
    first_name_col: str | None = None
    last_name_col: str | None = None
    email_col: str | None = None
    sp_level_col: str | None = None
    dance_level_col: str | None = None
    event_cols: list[str] = []
    supplemental_cols: list[str] = []

    for header in headers:
        norm = _normalize_header(header)
        if norm in _FIRST_NAME_HEADERS:
            first_name_col = header
            continue
        if norm in _LAST_NAME_HEADERS:
            last_name_col = header
            continue
        if norm in _EMAIL_HEADERS:
            email_col = header
            continue
        if norm == _SP_LEVEL_HEADER:
            sp_level_col = header
            continue
        if norm == _DANCE_LEVEL_HEADER:
            dance_level_col = header
            continue
        if any(norm.startswith(prefix) for prefix in _SUPPLEMENTAL_HEADER_PREFIXES):
            supplemental_cols.append(header)
            continue
        event_cols.append(header)

    return {
        "first_name_col": first_name_col,
        "last_name_col": last_name_col,
        "email_col": email_col,
        "sp_level_col": sp_level_col,
        "dance_level_col": dance_level_col,
        "event_cols": event_cols,
        "supplemental_cols": supplemental_cols,
    }


def event_column_short_label(header: str) -> str:
    """Short display label for a wide event column header."""
    text = re.sub(r"\s+", " ", str(header).strip())
    if " (" in text:
        text = text.split(" (", 1)[0].strip()
    return text


def supplemental_column_short_label(header: str) -> str:
    norm = _normalize_header(header)
    if norm.startswith("are you able to attend more than one"):
        return "Multiple comps OK"
    if norm.startswith("do you have a valid passport"):
        return "Valid passport"
    if norm.startswith("if someone is needed for back-to-back"):
        return "Back-to-back OK"
    if norm.startswith("judges, do you need any specific activity"):
        return "Activity notes"
    if norm.startswith("please provide us with any other information"):
        return "Other notes"
    return str(header).strip()


def availability_cell_display(value: object) -> str:
    code = normalize_qualifying_availability_cell(value)
    if code == "unknown":
        text = str(value).strip() if value is not None and not pd.isna(value) else ""
        return text or _AVAILABILITY_DISPLAY["no_response"]
    return _AVAILABILITY_DISPLAY.get(code, _AVAILABILITY_DISPLAY["unknown"])


def availability_cell_code(value: object) -> str:
    return normalize_qualifying_availability_cell(value)


def _normalize_email(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().lower()


def _normalize_person_name(first: object, last: object) -> str:
    parts = []
    for val in (first, last):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        text = str(val).strip()
        if text:
            parts.append(text)
    return " ".join(parts).casefold()


def _normalize_official_name(name: object) -> str:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    return re.sub(r"\s+", " ", str(name).strip()).casefold()


def load_international_availability_workbook(
    path: str,
    *,
    sheet_name: str | None = None,
) -> pd.DataFrame:
    """Load the availability form export; first row is column headers."""
    sheet = resolve_workbook_sheet_name(path, sheet_name)
    df = pd.read_excel(path, sheet_name=sheet, dtype=object)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def parse_workbook_structure(df: pd.DataFrame) -> dict[str, Any]:
    """Classify columns and build short labels for events and supplemental fields."""
    layout = _classify_workbook_headers(list(df.columns))
    event_cols: list[str] = layout["event_cols"]
    layout["event_short_labels"] = {col: event_column_short_label(col) for col in event_cols}
    layout["supplemental_short_labels"] = {
        col: supplemental_column_short_label(col) for col in layout["supplemental_cols"]
    }
    return layout


def build_form_response_index(
    df: pd.DataFrame,
    layout: dict[str, Any],
) -> tuple[dict[str, pd.Series], dict[str, pd.Series]]:
    """
    Map normalized email and fallback name → form row.

    Email matches take precedence; name index is used only when email is blank.
    """
    by_email: dict[str, pd.Series] = {}
    by_name: dict[str, pd.Series] = {}

    email_col = layout.get("email_col")
    first_col = layout.get("first_name_col")
    last_col = layout.get("last_name_col")

    for _, row in df.iterrows():
        email = _normalize_email(row.get(email_col)) if email_col else ""
        if email and email not in by_email:
            by_email[email] = row
        name_key = _normalize_person_name(
            row.get(first_col) if first_col else None,
            row.get(last_col) if last_col else None,
        )
        if name_key and name_key not in by_name:
            by_name[name_key] = row

    return by_email, by_name


def _resolve_discipline_filter_id(discipline_filter: str) -> int | None:
    if discipline_filter == DISCIPLINE_FILTER_SINGLES_PAIRS:
        return DISC_SINGLES_PAIRS_ID
    if discipline_filter == DISCIPLINE_FILTER_DANCE:
        return DISC_DANCE_ID
    return None


def _resolve_level_filter_id(level_filter: str) -> int | None:
    if level_filter == LEVEL_FILTER_INTERNATIONAL:
        return DIRECTORY_LEVEL_ID_INTERNATIONAL
    if level_filter == LEVEL_FILTER_ISU:
        return DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP
    return None


def _load_official_emails(official_ids: list[int], *, engine=None) -> dict[int, str]:
    if not official_ids:
        return {}
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        rows = session.execute(
            select(Officials.id, Officials.email).where(Officials.id.in_(official_ids))
        ).all()
    return {
        int(oid): _normalize_email(email)
        for oid, email in rows
        if email and str(email).strip()
    }


def _lookup_form_row(
    *,
    official_id: int,
    official_name: str,
    official_emails: dict[int, str],
    by_email: dict[str, pd.Series],
    by_name: dict[str, pd.Series],
) -> pd.Series | None:
    email = official_emails.get(int(official_id), "")
    if email and email in by_email:
        return by_email[email]
    name_key = _normalize_official_name(official_name)
    if name_key and name_key in by_name:
        return by_name[name_key]
    return None


def build_international_availability_report(
    form_df: pd.DataFrame,
    *,
    discipline_filter: str = DISCIPLINE_FILTER_ALL,
    level_filter: str = LEVEL_FILTER_ALL,
    active_appointments_only: bool = True,
    appointments: pd.DataFrame | None = None,
    layout: dict[str, Any] | None = None,
    birthdates: dict[int, object] | None = None,
    engine=None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Matrix report: directory judge appointments × event availability from the form.

    Returns ``(display_df, meta)`` where ``meta`` includes event column keys and codes
    for styling.
    """
    meta: dict[str, Any] = {
        "discipline_filter": discipline_filter,
        "level_filter": level_filter,
        "event_cols": [],
        "supplemental_cols": [],
        "availability_codes": [],
        "excluded_age_out": 0,
        "age_reference_listing_season": AVAILABILITY_AGE_LISTING_SEASON_CODE,
    }
    if not len(form_df.columns):
        return pd.DataFrame(), meta

    layout = layout or parse_workbook_structure(form_df)
    event_cols: list[str] = layout["event_cols"]
    supplemental_cols: list[str] = layout["supplemental_cols"]
    event_labels: dict[str, str] = layout["event_short_labels"]
    supplemental_labels: dict[str, str] = layout["supplemental_short_labels"]
    meta["event_cols"] = event_cols
    meta["supplemental_cols"] = supplemental_cols

    disc_id = _resolve_discipline_filter_id(discipline_filter)
    level_id = _resolve_level_filter_id(level_filter)

    if appointments is None:
        appointments = get_international_officials_for_filters(
            appointment_type_id=INTERNATIONAL_JUDGE_APPOINTMENT_TYPE_ID,
            discipline_id=disc_id,
            level_id=level_id,
            active_appointments_only=active_appointments_only,
        )
    else:
        appointments = appointments.copy()
        appointments = appointments.loc[
            appointments["appointment_type_id"] == INTERNATIONAL_JUDGE_APPOINTMENT_TYPE_ID
        ]
        if disc_id is not None:
            appointments = appointments.loc[
                appointments["discipline_id"].astype("Int64") == disc_id
            ]
        if level_id is not None:
            appointments = appointments.loc[
                appointments["appointment_level_id"].astype("Int64") == level_id
            ]

    allowed_disciplines = {DISC_SINGLES_PAIRS_ID, DISC_DANCE_ID}
    appointments = appointments.loc[
        appointments["discipline_id"].astype("Int64").isin(list(allowed_disciplines))
    ].copy()
    appointments = appointments.loc[
        appointments["appointment_level_id"]
        .astype("Int64")
        .isin([DIRECTORY_LEVEL_ID_INTERNATIONAL, DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP])
    ]

    appointments, excluded_age_out = _filter_appointments_below_age_out(
        appointments,
        birthdates=birthdates,
        engine=engine,
    )
    meta["excluded_age_out"] = excluded_age_out

    if appointments.empty:
        meta["official_count"] = 0
        meta["form_match_count"] = 0
        return pd.DataFrame(), meta

    by_email, by_name = build_form_response_index(form_df, layout)
    official_ids = appointments["official_id"].astype(int).unique().tolist()
    official_emails = _load_official_emails(official_ids, engine=engine)

    rows: list[dict[str, Any]] = []
    codes_grid: list[list[str]] = []
    form_match_count = 0

    for appt in appointments.itertuples(index=False):
        form_row = _lookup_form_row(
            official_id=int(appt.official_id),
            official_name=str(appt.official_name or ""),
            official_emails=official_emails,
            by_email=by_email,
            by_name=by_name,
        )
        if form_row is not None:
            form_match_count += 1

        out_row: dict[str, Any] = {
            "Official": (appt.official_name or "").strip() or f"Id {appt.official_id}",
            "Discipline": appt.discipline,
            "Level": appt.appointment_level,
        }
        row_codes: list[str] = []

        for col in event_cols:
            label = event_labels[col]
            if form_row is None:
                code = "no_response"
                out_row[label] = _AVAILABILITY_DISPLAY["no_response"]
            else:
                code = availability_cell_code(form_row.get(col))
                out_row[label] = availability_cell_display(form_row.get(col))
            row_codes.append(code)

        for col in supplemental_cols:
            label = supplemental_labels[col]
            if form_row is None:
                out_row[label] = ""
            else:
                val = form_row.get(col)
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    out_row[label] = ""
                else:
                    out_row[label] = str(val).strip()

        rows.append(out_row)
        codes_grid.append(row_codes)

    display = pd.DataFrame(rows)
    meta["official_count"] = int(appointments["official_id"].nunique())
    meta["appointment_count"] = len(display)
    meta["form_match_count"] = form_match_count
    meta["availability_codes"] = codes_grid
    meta["event_short_labels"] = [event_labels[c] for c in event_cols]
    return display, meta
