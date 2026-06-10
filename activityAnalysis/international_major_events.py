"""
Major ISU event activity matrix for international officials (Worlds, Europeans, etc.).

Rows: officials with ISU directory appointments in Singles, Pairs, Ice Dance, or IDVO.
Columns: calendar years with role/discipline abbreviations (e.g. ``J-D``).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

_CURRENT_YEAR = datetime.now().year
# Sort key for officials who never served at this event (ascending: below 0).
YEARS_SINCE_LAST_SORT_SENTINEL = 9999

import pandas as pd
from sqlalchemy import bindparam, or_, select, text
from sqlalchemy.orm import Session

try:
    from activityAnalysis.isu_major_event_classification import (
        MAJOR_ISU_EVENT_KEYS,
        MAJOR_ISU_EVENT_LABELS,
        classify_isu_major_event,
        competition_matches_major_event,
        year_from_competition_name,
    )
    from activityAnalysis.international_officials_data import (
        IDVO_SEGMENT_DISCIPLINE_TYPE_IDS,
        INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
        INTERNATIONAL_APPOINTMENT_TYPE_IDS,
        _discipline_match_sql,
        _international_appointment_filter_clauses,
        national_segment_appointment_type_id,
    )
    from activityAnalysis.load_activity_data import (
        DISC_DANCE_ID,
        DISC_PAIRS_ID,
        DISC_SINGLES_PAIRS_ID,
        SINGLES_DISCIPLINE_ID,
        activity_database_is_postgresql,
        build_activity_matrix,
        engine,
        segment_discipline_type_ids_for_directory,
    )
    from activityAnalysis.officials_analysis_models import (
        Appointments,
        Officials,
    )
except ModuleNotFoundError:
    from isu_major_event_classification import (
        MAJOR_ISU_EVENT_KEYS,
        MAJOR_ISU_EVENT_LABELS,
        classify_isu_major_event,
        competition_matches_major_event,
        year_from_competition_name,
    )
    from international_officials_data import (
        IDVO_SEGMENT_DISCIPLINE_TYPE_IDS,
        INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
        INTERNATIONAL_APPOINTMENT_TYPE_IDS,
        _discipline_match_sql,
        _international_appointment_filter_clauses,
        national_segment_appointment_type_id,
    )
    from load_activity_data import (
        DISC_DANCE_ID,
        DISC_PAIRS_ID,
        DISC_SINGLES_PAIRS_ID,
        SINGLES_DISCIPLINE_ID,
        activity_database_is_postgresql,
        build_activity_matrix,
        engine,
        segment_discipline_type_ids_for_directory,
    )
    from officials_analysis_models import Appointments, Officials

from officials_competition_types import OFFICIALS_COMPETITION_TYPE_IDS_ISU_EVENT

# Directory disciplines for Singles / Pairs / Ice Dance (incl. combined Singles/Pairs).
SPD_DIRECTORY_DISCIPLINE_IDS: tuple[int, ...] = (
    SINGLES_DISCIPLINE_ID,
    DISC_PAIRS_ID,
    DISC_DANCE_ID,
    DISC_SINGLES_PAIRS_ID,
)

# ``segment_official.appointment_type_id`` → short role code for matrix cells.
NATIONAL_PANEL_ROLE_ABBREV: dict[int, str] = {
    1: "J",
    4: "R",
    8: "DO",
    9: "TS",
    11: "TC",
}

# ``segment.discipline_type_id`` (Singles / Pairs / Ice Dance).
SEGMENT_DISCIPLINE_ABBREV: dict[int, str] = {
    1: "S",
    2: "P",
    3: "D",
}

_ROLE_SORT_ORDER = {"J": 0, "R": 1, "TC": 2, "TS": 3, "DO": 4}
_DISC_SORT_ORDER = {"S": 0, "P": 1, "D": 2}

_MAJOR_EVENT_ACTIVITY_COLUMNS = [
    "official_id",
    "year",
    "competition_name",
    "national_appointment_type_id",
    "segment_discipline_type_id",
]


def _calendar_year_from_stored_season(competition_year: Any) -> int | None:
    if competition_year is None or (
        isinstance(competition_year, float) and pd.isna(competition_year)
    ):
        return None
    text_val = str(competition_year).strip()
    if not text_val.isdigit():
        return None
    code = int(text_val)
    if 1900 <= code <= 2100:
        return code
    if 1000 <= code <= 9999:
        end_yy = code % 100
        return 2000 + end_yy
    return None


def competition_calendar_year(
    *,
    start_date: Any,
    end_date: Any,
    competition_year: Any,
    competition_name: str = "",
) -> int | None:
    """Best-effort calendar year for matrix columns."""
    for val in (start_date, end_date):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        if isinstance(val, datetime):
            return int(val.year)
        if isinstance(val, date):
            return int(val.year)
        parsed = pd.to_datetime(val, errors="coerce")
        if pd.notna(parsed):
            return int(parsed.year)

    title_year = year_from_competition_name(competition_name)
    if title_year is not None and classify_isu_major_event(competition_name):
        return title_year

    from_stored = _calendar_year_from_stored_season(competition_year)
    if from_stored is not None:
        return from_stored
    return title_year


def major_event_assignment_label(
    national_appointment_type_id: Any,
    segment_discipline_type_id: Any,
) -> str:
    """Format one assignment as ``J-D``, ``TC-S``, ``DO``, etc."""
    try:
        role_id = int(national_appointment_type_id)
    except (TypeError, ValueError):
        return ""
    role = NATIONAL_PANEL_ROLE_ABBREV.get(role_id, f"R{role_id}")
    if role == "DO":
        return "DO"
    try:
        disc_id = int(segment_discipline_type_id)
    except (TypeError, ValueError):
        return role
    disc = SEGMENT_DISCIPLINE_ABBREV.get(disc_id)
    return f"{role}-{disc}" if disc else role


def _label_sort_key(label: str) -> tuple[int, int, str]:
    if "-" in label:
        role, disc = label.split("-", 1)
        return (_ROLE_SORT_ORDER.get(role, 99), _DISC_SORT_ORDER.get(disc, 99), label)
    return (_ROLE_SORT_ORDER.get(label, 99), 99, label)


def major_event_cell_labels(assignments: pd.DataFrame) -> str:
    """Comma-separated sorted labels for one official × year."""
    if assignments.empty:
        return ""
    labels = {
        major_event_assignment_label(
            r.get("national_appointment_type_id"),
            r.get("segment_discipline_type_id"),
        )
        for _, r in assignments.iterrows()
    }
    labels.discard("")
    return ", ".join(sorted(labels, key=_label_sort_key))


def major_event_cell_lookup(activity: pd.DataFrame) -> dict[tuple[int, int], str]:
    if activity.empty:
        return {}
    out: dict[tuple[int, int], str] = {}
    for (oid, year), grp in activity.groupby(["official_id", "year"], sort=False):
        out[(int(oid), int(year))] = major_event_cell_labels(grp)
    return out


def _resolve_national_role_ids(appointment_type_id: int | None) -> list[int]:
    if appointment_type_id is not None:
        nat = national_segment_appointment_type_id(int(appointment_type_id))
        return [nat] if nat is not None else []
    roles: list[int] = []
    for at_id in INTERNATIONAL_APPOINTMENT_TYPE_IDS:
        nat = national_segment_appointment_type_id(at_id)
        if nat is not None:
            roles.append(nat)
    return sorted(set(roles))


def _resolve_directory_discipline_ids(
    discipline_id: int | None,
    *,
    appointment_type_id: int | None = None,
) -> list[int] | None:
    if appointment_type_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
        return None
    if discipline_id is None:
        return list(SPD_DIRECTORY_DISCIPLINE_IDS)
    return [int(discipline_id)]


def _isu_major_event_official_scope(
    where_parts: list[Any],
    *,
    appointment_type_id: int | None,
    discipline_id: int | None,
) -> None:
    """Narrow directory rows to ISU SPD and/or IDVO appointments for the matrix."""
    if appointment_type_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
        return
    if appointment_type_id is not None:
        if discipline_id is None:
            where_parts.append(Appointments.discipline_id.in_(list(SPD_DIRECTORY_DISCIPLINE_IDS)))
        return
    if discipline_id is not None:
        return
    where_parts.append(
        or_(
            Appointments.appointment_type_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
            Appointments.discipline_id.in_(list(SPD_DIRECTORY_DISCIPLINE_IDS)),
        )
    )


def _collapse_qualified_officials(qualified: pd.DataFrame) -> pd.DataFrame:
    """One row per official (earliest ISU appointment date for years-in-grade)."""
    if qualified.empty:
        return qualified
    work = qualified.copy()
    work["achieved_date"] = pd.to_datetime(work["achieved_date"], errors="coerce")
    work = work.sort_values(
        ["official_id", "achieved_date"],
        ascending=[True, True],
        na_position="last",
    )
    return (
        work.drop_duplicates(subset=["official_id"], keep="first")
        .sort_values("full_name", na_position="last")
        .reset_index(drop=True)
    )


def years_eligible_for_appointment(
    achieved_year: Any,
    *,
    current_year: int | None = None,
) -> int | None:
    """Calendar years since earliest ISU appointment year (current year minus achieved year)."""
    try:
        if achieved_year is None or pd.isna(achieved_year):
            return None
        ach = int(achieved_year)
    except (TypeError, ValueError):
        return None
    end = int(current_year or _CURRENT_YEAR)
    return max(0, end - ach)


def load_isu_spd_appointments_for_eligibility(
    official_ids: Iterable[int],
    *,
    isu_level_id: int,
    active_appointments_only: bool = True,
) -> pd.DataFrame:
    """
    All ISU SPD and IDVO directory appointments for officials (ignores role/discipline UI filters).

    Used to compute years eligible (max across appointments) and pre-appointment shading.
    """
    ids = sorted({int(x) for x in official_ids if x is not None})
    if not ids:
        return pd.DataFrame(columns=["official_id", "achieved_date"])

    where_parts = _international_appointment_filter_clauses(
        appointment_type_id=None,
        active_appointments_only=active_appointments_only,
    )
    where_parts.append(Appointments.level_id == int(isu_level_id))
    where_parts.append(
        or_(
            Appointments.appointment_type_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
            Appointments.discipline_id.in_(list(SPD_DIRECTORY_DISCIPLINE_IDS)),
        )
    )
    where_parts.append(Appointments.official_id.in_(ids))

    with Session(engine) as session:
        stmt = (
            select(
                Appointments.official_id,
                Appointments.achieved_date,
            )
            .where(*where_parts)
            .order_by(Appointments.official_id.asc(), Appointments.achieved_date.asc())
        )
        rows = session.execute(stmt).all()

    return pd.DataFrame(rows, columns=["official_id", "achieved_date"])


def _matrix_year_columns(matrix: pd.DataFrame) -> list[int]:
    return sorted(
        int(c)
        for c in matrix.columns
        if isinstance(c, (int, float)) and not isinstance(c, bool)
        and str(c).replace(".0", "").isdigit()
    )


def _ensure_matrix_year_columns(
    matrix: pd.DataFrame,
    calendar_years: Iterable[int],
) -> pd.DataFrame:
    """Add empty year columns so role filters do not collapse the matrix to one season."""
    if matrix.empty:
        return matrix
    out = matrix.copy()
    for year in calendar_years:
        yc = int(year)
        if yc not in out.columns:
            out[yc] = 0
    return out


def enrich_major_event_matrix_eligibility(
    matrix: pd.DataFrame,
    appointment_rows: pd.DataFrame,
) -> pd.DataFrame:
    """
    Set ``eligible_years`` from the earliest ISU appointment year and ``achieved_year``
    for pre-appointment cell shading.
    """
    if matrix.empty:
        return matrix

    out = matrix.copy()
    if appointment_rows.empty:
        return out

    work = appointment_rows.copy()
    work["achieved_year"] = pd.to_datetime(work["achieved_date"], errors="coerce").dt.year
    min_achieved = work.groupby("official_id")["achieved_year"].min()

    out["achieved_year"] = out["official_id"].map(min_achieved)
    out["eligible_years"] = out["achieved_year"].apply(years_eligible_for_appointment)
    return out.drop(columns=["years_in_grade"], errors="ignore")


def major_event_pre_appointment_mask(
    display: pd.DataFrame,
    year_cols: list[str],
) -> pd.DataFrame | None:
    """Boolean mask: True where calendar year is on or before earliest appointment year."""
    if "achieved_year" not in display.columns or not year_cols:
        return None
    achieved = pd.to_numeric(display["achieved_year"], errors="coerce")
    if achieved is None:
        return None
    ystrs = [str(c) for c in year_cols if str(c) in display.columns]
    if not ystrs:
        return None
    return pd.DataFrame(
        {yc: (int(yc) <= achieved) & achieved.notna() for yc in ystrs},
        index=display.index,
    )


def style_major_event_matrix_display(
    display: pd.DataFrame,
    year_cols: list[str],
) -> Any:
    """Pandas styler with gray pre-appointment year cells (championships report pattern)."""
    shade_mask = major_event_pre_appointment_mask(display, year_cols)

    def _fmt_optional_int(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        try:
            return str(int(val))
        except (TypeError, ValueError):
            return ""

    fmt_map = {}
    for col in ("Years eligible", "Years since last", "Most recent year"):
        if col in display.columns:
            fmt_map[col] = _fmt_optional_int

    styled = display.style.format(fmt_map, na_rep="")
    if shade_mask is None or shade_mask.empty:
        return styled

    shade_cols = [c for c in shade_mask.columns if c in display.columns]

    def _shade_col(s):
        col = s.name
        if col not in shade_mask.columns:
            return [""] * len(s)
        return [
            "background-color: #ececec" if bool(shade_mask.loc[idx, col]) else ""
            for idx in s.index
        ]

    if shade_cols:
        styled = styled.apply(_shade_col, axis=0, subset=shade_cols)
    return styled


def get_isu_spd_qualified_officials(
    *,
    appointment_type_id: int | None = None,
    discipline_id: int | None = None,
    isu_level_id: int,
    active_appointments_only: bool = True,
) -> pd.DataFrame:
    """
    Officials with active ISU-level international appointments in SPD and/or IDVO.

    Returns one row per official even when they hold multiple matching appointments.
    """
    where_parts = _international_appointment_filter_clauses(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        active_appointments_only=active_appointments_only,
    )
    where_parts.append(Appointments.level_id == int(isu_level_id))
    _isu_major_event_official_scope(
        where_parts,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
    )

    with Session(engine) as session:
        stmt = (
            select(
                Officials.id.label("official_id"),
                Officials.full_name,
                Officials.region,
                Appointments.achieved_date,
            )
            .join(Appointments, Appointments.official_id == Officials.id)
            .where(*where_parts)
            .order_by(Officials.full_name.asc().nulls_last())
        )
        rows = session.execute(stmt).all()

    return _collapse_qualified_officials(
        pd.DataFrame(rows, columns=["official_id", "full_name", "region", "achieved_date"])
    )


def _segment_discipline_filter_sql(
    *,
    appointment_type_id: int | None,
    discipline_id: int | None,
) -> tuple[str, dict[str, Any]]:
    if appointment_type_id is not None and discipline_id is None:
        if int(appointment_type_id) == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
            return (
                " AND s.discipline_type_id IN :idvo_segment_discipline_type_ids",
                {"idvo_segment_discipline_type_ids": list(IDVO_SEGMENT_DISCIPLINE_TYPE_IDS)},
            )
        nat = national_segment_appointment_type_id(int(appointment_type_id))
        # Referees are often listed once per event without a segment discipline.
        if nat == 4:
            return "", {}
    if discipline_id is not None and appointment_type_id is not None:
        return _discipline_match_sql(
            intl_appointment_type_id=int(appointment_type_id),
            directory_discipline_id=int(discipline_id),
        )
    if discipline_id is not None:
        seg_ids = segment_discipline_type_ids_for_directory(
            int(discipline_id),
            national_segment_appointment_type_id(12) or 1,
        )
        if not seg_ids:
            return " AND 1=0", {}
        return (
            " AND s.discipline_type_id IN :segment_discipline_type_ids",
            {"segment_discipline_type_ids": list(seg_ids)},
        )
    return (
        " AND s.discipline_type_id IN :spd_segment_discipline_type_ids",
        {"spd_segment_discipline_type_ids": list(IDVO_SEGMENT_DISCIPLINE_TYPE_IDS)},
    )


def _query_major_event_assignment_rows(
    *,
    event_key: str,
    official_ids: Iterable[int] | None = None,
    appointment_type_id: int | None = None,
    discipline_id: int | None = None,
) -> pd.DataFrame:
    """Raw panel rows at ``event_key`` before calendar-year normalization."""
    if event_key not in MAJOR_ISU_EVENT_KEYS:
        return pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    if not activity_database_is_postgresql():
        return pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    role_ids = _resolve_national_role_ids(appointment_type_id)
    if not role_ids:
        return pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    ids: list[int] | None = None
    if official_ids is not None:
        ids = sorted({int(x) for x in official_ids if x is not None})
        if not ids:
            return pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    disc_sql, disc_params = _segment_discipline_filter_sql(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
    )
    comp_type_ids = sorted(OFFICIALS_COMPETITION_TYPE_IDS_ISU_EVENT)
    official_sql = " AND so.official_id IN :official_ids" if ids is not None else ""

    bind_names = [
        bindparam("national_role_ids", expanding=True),
        bindparam("competition_type_ids", expanding=True),
    ]
    if ids is not None:
        bind_names.insert(0, bindparam("official_ids", expanding=True))

    stmt = text(
        f"""
            SELECT DISTINCT
                so.official_id,
                c.name AS competition_name,
                c.start_date,
                c.end_date,
                c.year AS competition_year,
                so.appointment_type_id AS national_appointment_type_id,
                s.discipline_type_id AS segment_discipline_type_id
            FROM public.segment_official so
            INNER JOIN public.segment s ON s.id = so.segment_id
            INNER JOIN public.competition c ON c.id = s.competition_id
            WHERE so.official_id IS NOT NULL
              {official_sql}
              AND so.appointment_type_id IN :national_role_ids
              AND (
                s.level IN ('Junior', 'Senior')
                OR s.level IS NULL
                OR btrim(s.level::text) = ''
                OR s.level = 'Unspecified'
              )
              AND (
                c.officials_analysis_competition_type_id IN :competition_type_ids
                OR c.international IS TRUE
              )
              {disc_sql}
            """
    ).bindparams(*bind_names)
    params: dict[str, Any] = {
        "national_role_ids": role_ids,
        "competition_type_ids": comp_type_ids,
        **disc_params,
    }
    if ids is not None:
        params["official_ids"] = ids
    if "segment_discipline_type_ids" in disc_params:
        stmt = stmt.bindparams(bindparam("segment_discipline_type_ids", expanding=True))
    if "spd_segment_discipline_type_ids" in disc_params:
        stmt = stmt.bindparams(bindparam("spd_segment_discipline_type_ids", expanding=True))
    if "idvo_segment_discipline_type_ids" in disc_params:
        stmt = stmt.bindparams(bindparam("idvo_segment_discipline_type_ids", expanding=True))

    try:
        with Session(engine) as session:
            rows = session.execute(stmt, params).mappings().all()
    except Exception:
        return pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    if not rows:
        return pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    df = pd.DataFrame(rows)
    df = df.loc[
        df["competition_name"].apply(
            lambda n: competition_matches_major_event(str(n or ""), event_key)
        )
    ]
    if df.empty:
        return pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    df["year"] = df.apply(
        lambda r: competition_calendar_year(
            start_date=r.get("start_date"),
            end_date=r.get("end_date"),
            competition_year=r.get("competition_year"),
            competition_name=str(r.get("competition_name") or ""),
        ),
        axis=1,
    )
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    return df[_MAJOR_EVENT_ACTIVITY_COLUMNS].reset_index(drop=True)


def load_major_event_assignment_rows(
    official_ids: Iterable[int],
    *,
    event_key: str,
    appointment_type_id: int | None = None,
    discipline_id: int | None = None,
) -> pd.DataFrame:
    """Panel rows at the selected major ISU event with role and segment discipline."""
    return _query_major_event_assignment_rows(
        event_key=event_key,
        official_ids=official_ids,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
    )


def load_major_event_calendar_years(
    *,
    event_key: str,
    discipline_id: int | None = None,
) -> list[int]:
    """
    Calendar years with any panel role at ``event_key`` (all roles, all officials).

    Used for matrix column span so a role filter does not hide earlier event years.
    """
    activity = _query_major_event_assignment_rows(
        event_key=event_key,
        official_ids=None,
        appointment_type_id=None,
        discipline_id=discipline_id,
    )
    if activity.empty:
        return []
    return sorted(activity["year"].astype(int).unique().tolist())


def _apply_major_event_cell_labels(
    matrix: pd.DataFrame,
    activity: pd.DataFrame,
) -> pd.DataFrame:
    """Replace numeric year cells with role/discipline abbreviations."""
    if matrix.empty:
        return matrix
    cells = major_event_cell_lookup(activity)
    out = matrix.copy()
    year_cols = [
        c
        for c in out.columns
        if isinstance(c, (int, float)) and not isinstance(c, bool)
        and str(c).replace(".0", "").isdigit()
    ]
    for yc in year_cols:
        year_int = int(yc)
        out[yc] = out["official_id"].apply(
            lambda oid: cells.get((int(oid), year_int), "")
        )
    return out


def get_international_major_event_matrix(
    *,
    event_key: str,
    appointment_type_id: int | None = None,
    discipline_id: int | None = None,
    isu_level_id: int,
    active_appointments_only: bool = True,
) -> pd.DataFrame:
    """Activity matrix: ISU-appointed SPD officials × calendar years at ``event_key``."""
    qualified = get_isu_spd_qualified_officials(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        isu_level_id=isu_level_id,
        active_appointments_only=active_appointments_only,
    )
    if qualified.empty:
        return qualified

    official_ids = qualified["official_id"].astype(int).tolist()
    activity = load_major_event_assignment_rows(
        official_ids,
        event_key=event_key,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
    )
    if activity.empty:
        activity = pd.DataFrame(columns=_MAJOR_EVENT_ACTIVITY_COLUMNS)

    event_calendar_years = load_major_event_calendar_years(
        event_key=event_key,
        discipline_id=discipline_id,
    )

    presence = (
        activity[["official_id", "year"]].drop_duplicates()
        if not activity.empty
        else pd.DataFrame(columns=["official_id", "year"])
    )
    matrix = build_activity_matrix(qualified, presence)
    matrix = _ensure_matrix_year_columns(matrix, event_calendar_years)
    matrix = _apply_major_event_cell_labels(matrix, activity)
    matrix = matrix.drop_duplicates(subset=["official_id"], keep="first").reset_index(drop=True)
    appt_rows = load_isu_spd_appointments_for_eligibility(
        matrix["official_id"].astype(int).tolist(),
        isu_level_id=isu_level_id,
        active_appointments_only=active_appointments_only,
    )
    return enrich_major_event_matrix_eligibility(matrix, appt_rows)


def format_major_event_matrix_for_display(matrix: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Prepare matrix for Streamlit (year columns already hold role/discipline labels)."""
    if matrix.empty:
        return matrix, []

    df = matrix.copy()
    df.columns = [str(c) for c in df.columns]
    year_cols = sorted(
        [c for c in df.columns if str(c).isdigit()],
        key=lambda x: int(x),
        reverse=True,
    )
    meta_cols = [c for c in df.columns if c not in year_cols]
    display = df[[c for c in meta_cols + year_cols if c in df.columns]]
    display = display.rename(
        columns={
            "full_name": "Official",
            "years_since_last": "Years since last",
            "most_recent_year": "Most recent year",
            "total_championships": "Times at event",
            "eligible_years": "Years eligible",
        }
    )
    display = display.drop(
        columns=["region", "official_id", "never_used", "years_in_grade"],
        errors="ignore",
    )
    for col in ("Most recent year", "Years eligible", "Times at event"):
        if col in display.columns:
            display[col] = pd.to_numeric(display[col], errors="coerce").astype("Int64")
    if "Years since last" in display.columns:
        yrs = pd.to_numeric(display["Years since last"], errors="coerce")
        display["Years since last"] = yrs.mask(
            yrs >= YEARS_SINCE_LAST_SORT_SENTINEL,
            other=pd.NA,
        ).astype("Int64")
    for col in year_cols:
        if col in display.columns:
            display[col] = display[col].fillna("").astype(str).replace("nan", "")
    return display, year_cols


def major_event_matrix_legend() -> str:
    return (
        "J = Judge, R = Referee, TC = Technical Controller, TS = Technical Specialist, "
        "DO = International Data & Video Operator; "
        "S = Singles, P = Pairs, D = Ice Dance (e.g. J-D = dance judge). "
        "Gray year cells are on or before the official's earliest ISU appointment year. "
        "Years eligible is the current calendar year minus the year of their earliest "
        "ISU SPD or IDVO appointment."
    )
