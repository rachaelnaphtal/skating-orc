"""
Load and filter ISU seminar attendance for international listing requirements.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

try:
    from activityAnalysis.international_listing_seasons import (
        competition_year_matches_seasons,
        format_usfs_season_code,
        usfs_season_code_for_date,
    )
    from activityAnalysis.international_officials_data import _nullable_int_for_sql
    from activityAnalysis.load_activity_data import activity_database_is_postgresql, engine
except ModuleNotFoundError:
    from international_listing_seasons import (
        competition_year_matches_seasons,
        format_usfs_season_code,
        usfs_season_code_for_date,
    )
    from international_officials_data import _nullable_int_for_sql
    from load_activity_data import activity_database_is_postgresql, engine

SINGLES_PAIRS_DISCIPLINE_IDS: frozenset[int] = frozenset({1, 8, 9})
DISCIPLINE_ID_SINGLES = 1
DISCIPLINE_ID_PAIRS = 8
INTERNATIONAL_TECHNICAL_SPECIALIST_APPOINTMENT_TYPE_ID = 14
INTERNATIONAL_TECHNICAL_CONTROLLER_APPOINTMENT_TYPE_ID = 15

_SEMINAR_COLUMNS = (
    "id",
    "official_id",
    "appointment_type_id",
    "discipline_id",
    "seminar_date",
    "season_code",
    "in_person",
    "place",
    "at_event",
    "notes",
)


def seminar_discipline_matches(
    seminar_discipline_id: Any,
    appointment_discipline_id: Any,
    *,
    appointment_type_id: int | None = None,
) -> bool:
    """True when a seminar row applies to the appointment discipline."""
    sem_disc = _nullable_int_for_sql(seminar_discipline_id)
    appt_disc = _nullable_int_for_sql(appointment_discipline_id)
    if appt_disc is None:
        return True
    if sem_disc is None:
        return False
    if sem_disc == appt_disc:
        return True
    try:
        atid = int(appointment_type_id) if appointment_type_id is not None else None
    except (TypeError, ValueError):
        atid = None
    if atid in (
        INTERNATIONAL_TECHNICAL_SPECIALIST_APPOINTMENT_TYPE_ID,
        INTERNATIONAL_TECHNICAL_CONTROLLER_APPOINTMENT_TYPE_ID,
    ):
        # Pairs seminar counts toward Singles TC/TS appointments only.
        return appt_disc == DISCIPLINE_ID_SINGLES and sem_disc == DISCIPLINE_ID_PAIRS
    return False


def load_official_seminars_bulk(official_ids: list[int] | None = None) -> pd.DataFrame:
    """Bulk-load seminar rows for the given officials (or all when ``official_ids`` is None)."""
    if not activity_database_is_postgresql():
        return pd.DataFrame(columns=list(_SEMINAR_COLUMNS))

    official_filter = ""
    params: dict[str, Any] = {}
    if official_ids is not None:
        ids = sorted({int(x) for x in official_ids if x is not None})
        if not ids:
            return pd.DataFrame(columns=list(_SEMINAR_COLUMNS))
        official_filter = "WHERE s.official_id IN :official_ids"
        params["official_ids"] = ids

    stmt = text(
        f"""
        SELECT
            s.id,
            s.official_id,
            s.appointment_type_id,
            s.discipline_id,
            s.seminar_date,
            s.season_code,
            s.in_person,
            s.place,
            s.at_event,
            s.notes
        FROM officials_analysis.isu_official_seminar s
        {official_filter}
        ORDER BY s.official_id, s.seminar_date DESC, s.id
        """
    )
    if official_ids is not None:
        stmt = stmt.bindparams(bindparam("official_ids", expanding=True))

    try:
        with Session(engine) as session:
            rows = session.execute(stmt, params).mappings().all()
    except Exception:
        return pd.DataFrame(columns=list(_SEMINAR_COLUMNS))

    if not rows:
        return pd.DataFrame(columns=list(_SEMINAR_COLUMNS))
    return pd.DataFrame([dict(r) for r in rows])


def filter_seminars_for_appointment(
    seminars: pd.DataFrame,
    *,
    official_id: int,
    appointment_type_id: int,
    directory_discipline_id: Any,
) -> pd.DataFrame:
    """Keep seminar rows matching this official, appointment type, and discipline."""
    if seminars.empty:
        return seminars.iloc[0:0]
    mask = seminars["official_id"].astype(int) == int(official_id)
    mask &= seminars["appointment_type_id"].astype(int) == int(appointment_type_id)
    if directory_discipline_id is not None:
        disc_mask = seminars.apply(
            lambda r: seminar_discipline_matches(
                r.get("discipline_id"),
                directory_discipline_id,
                appointment_type_id=appointment_type_id,
            ),
            axis=1,
        )
        mask &= disc_mask
    return seminars.loc[mask].reset_index(drop=True)


def filter_seminars_to_season_codes(
    seminars: pd.DataFrame,
    season_codes: list[int],
    *,
    in_person: bool | None = None,
    at_event: bool | None = None,
) -> pd.DataFrame:
    """Filter seminars by season window and optional delivery mode."""
    if seminars.empty or not season_codes:
        return seminars.iloc[0:0]
    mask = seminars["season_code"].apply(
        lambda code: competition_year_matches_seasons(code, season_codes)
    )
    if in_person is not None:
        mask &= seminars["in_person"].astype(bool) == bool(in_person)
    if at_event is not None:
        mask &= seminars["at_event"].astype(bool) == bool(at_event)
    return seminars.loc[mask].reset_index(drop=True)


SEMINAR_DISPLAY_COLUMNS: tuple[str, ...] = (
    "Date",
    "Season",
    "In person",
    "At event",
    "Place",
    "Notes",
)


def seminars_display_for_appointment(
    official_id: int,
    appointment_type_id: int,
    directory_discipline_id: Any,
    *,
    seminars_bulk: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Seminar rows for one appointment, formatted for detail view / PDF tables."""
    if seminars_bulk is not None:
        source = seminars_bulk
    else:
        source = load_official_seminars_bulk([official_id])
    scoped = filter_seminars_for_appointment(
        source,
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        directory_discipline_id=directory_discipline_id,
    )
    if scoped.empty:
        return pd.DataFrame(columns=list(SEMINAR_DISPLAY_COLUMNS))

    rows: list[dict[str, str]] = []
    for rec in scoped.sort_values(
        ["seminar_date", "season_code", "id"], ascending=[False, False, False]
    ).itertuples(index=False):
        season_code = getattr(rec, "season_code", None)
        if season_code is not None and not (
            isinstance(season_code, float) and pd.isna(season_code)
        ):
            season_label = (
                f"{format_usfs_season_code(int(season_code))} ({int(season_code)})"
            )
        else:
            season_label = ""
        seminar_date = getattr(rec, "seminar_date", None)
        if seminar_date is not None and not (
            isinstance(seminar_date, float) and pd.isna(seminar_date)
        ):
            date_label = pd.Timestamp(seminar_date).strftime("%Y-%m-%d")
        else:
            date_label = ""
        rows.append(
            {
                "Date": date_label,
                "Season": season_label,
                "In person": "Yes" if bool(getattr(rec, "in_person", False)) else "No",
                "At event": "Yes" if bool(getattr(rec, "at_event", False)) else "No",
                "Place": str(getattr(rec, "place", "") or "").strip(),
                "Notes": str(getattr(rec, "notes", "") or "").strip(),
            }
        )
    return pd.DataFrame(rows, columns=list(SEMINAR_DISPLAY_COLUMNS))


def normalize_seminar_season_code(seminar_date: Any, season_code: Any) -> int:
    """Prefer stored season_code; derive from seminar_date when missing."""
    if season_code is not None and not (isinstance(season_code, float) and pd.isna(season_code)):
        return int(season_code)
    if seminar_date is None or (isinstance(seminar_date, float) and pd.isna(seminar_date)):
        raise ValueError("seminar requires season_code or seminar_date")
    parsed = pd.Timestamp(seminar_date).date()
    return usfs_season_code_for_date(parsed)


def seminar_table_exists() -> bool:
    if not activity_database_is_postgresql():
        return False
    try:
        with Session(engine) as session:
            row = session.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'officials_analysis'
                      AND table_name = 'isu_official_seminar'
                    """
                )
            ).first()
        return row is not None
    except Exception:
        return False


def search_officials_for_seminar_admin(
    query: str,
    *,
    limit: int = 40,
) -> pd.DataFrame:
    """Search directory officials by name or member number."""
    if not activity_database_is_postgresql():
        return pd.DataFrame(columns=["official_id", "full_name", "mbr_number"])
    q = (query or "").strip()
    if not q:
        return pd.DataFrame(columns=["official_id", "full_name", "mbr_number"])
    pattern = f"%{q}%"
    stmt = text(
        """
        SELECT o.id AS official_id, o.full_name, o.mbr_number
        FROM officials_analysis.officials o
        WHERE o.full_name ILIKE :pattern
           OR o.mbr_number ILIKE :pattern
        ORDER BY o.full_name NULLS LAST, o.id
        LIMIT :limit
        """
    )
    with Session(engine) as session:
        rows = session.execute(stmt, {"pattern": pattern, "limit": int(limit)}).mappings().all()
    return pd.DataFrame([dict(r) for r in rows])


def load_seminars_admin(
    *,
    official_id: int | None = None,
    name_query: str | None = None,
    appointment_type_id: int | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Seminar rows with official and lookup labels for the admin UI."""
    if not activity_database_is_postgresql() or not seminar_table_exists():
        return pd.DataFrame(columns=list(_SEMINAR_COLUMNS))

    filters = ["1=1"]
    params: dict[str, Any] = {"limit": int(limit)}
    if official_id is not None:
        filters.append("s.official_id = :official_id")
        params["official_id"] = int(official_id)
    if appointment_type_id is not None:
        filters.append("s.appointment_type_id = :appointment_type_id")
        params["appointment_type_id"] = int(appointment_type_id)
    name_q = (name_query or "").strip()
    if name_q:
        filters.append("(o.full_name ILIKE :name_pattern OR o.mbr_number ILIKE :name_pattern)")
        params["name_pattern"] = f"%{name_q}%"

    where_sql = " AND ".join(filters)
    stmt = text(
        f"""
        SELECT
            s.id,
            s.official_id,
            o.full_name AS official_name,
            o.mbr_number,
            s.appointment_type_id,
            at.name AS appointment_type,
            s.discipline_id,
            d.name AS discipline,
            s.seminar_date,
            s.season_code,
            s.in_person,
            s.place,
            s.at_event,
            s.notes
        FROM officials_analysis.isu_official_seminar s
        INNER JOIN officials_analysis.officials o ON o.id = s.official_id
        INNER JOIN officials_analysis.appointment_types at ON at.id = s.appointment_type_id
        LEFT JOIN officials_analysis.disciplines d ON d.id = s.discipline_id
        WHERE {where_sql}
        ORDER BY s.seminar_date DESC, s.id DESC
        LIMIT :limit
        """
    )
    try:
        with Session(engine) as session:
            rows = session.execute(stmt, params).mappings().all()
    except Exception:
        return pd.DataFrame(columns=list(_SEMINAR_COLUMNS))
    return pd.DataFrame([dict(r) for r in rows])


def insert_seminar_row(
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: int | None,
    seminar_date: Any,
    season_code: int | None = None,
    in_person: bool,
    place: str | None = None,
    at_event: bool = False,
    notes: str | None = None,
) -> int:
    """Insert one seminar attendance row; returns new row id."""
    code = normalize_seminar_season_code(seminar_date, season_code)
    disc = _nullable_int_for_sql(discipline_id)
    parsed_date = pd.Timestamp(seminar_date).date()
    stmt = text(
        """
        INSERT INTO officials_analysis.isu_official_seminar (
            official_id,
            appointment_type_id,
            discipline_id,
            seminar_date,
            season_code,
            in_person,
            place,
            at_event,
            notes,
            last_modified
        ) VALUES (
            :official_id,
            :appointment_type_id,
            :discipline_id,
            :seminar_date,
            :season_code,
            :in_person,
            :place,
            :at_event,
            :notes,
            now()
        )
        RETURNING id
        """
    )
    params = {
        "official_id": int(official_id),
        "appointment_type_id": int(appointment_type_id),
        "discipline_id": disc,
        "seminar_date": parsed_date,
        "season_code": int(code),
        "in_person": bool(in_person),
        "place": (place or "").strip() or None,
        "at_event": bool(at_event),
        "notes": (notes or "").strip() or None,
    }
    with Session(engine) as session:
        new_id = session.execute(stmt, params).scalar_one()
        session.commit()
    return int(new_id)


def update_seminar_row(
    seminar_id: int,
    *,
    appointment_type_id: int,
    discipline_id: int | None,
    seminar_date: Any,
    season_code: int,
    in_person: bool,
    place: str | None,
    at_event: bool,
    notes: str | None,
) -> None:
    params = {
        "id": int(seminar_id),
        "appointment_type_id": int(appointment_type_id),
        "discipline_id": _nullable_int_for_sql(discipline_id),
        "seminar_date": pd.Timestamp(seminar_date).date(),
        "season_code": int(season_code),
        "in_person": bool(in_person),
        "place": (place or "").strip() or None,
        "at_event": bool(at_event),
        "notes": (notes or "").strip() or None,
    }
    with Session(engine) as session:
        session.execute(
            text(
                """
                UPDATE officials_analysis.isu_official_seminar
                SET
                    appointment_type_id = :appointment_type_id,
                    discipline_id = :discipline_id,
                    seminar_date = :seminar_date,
                    season_code = :season_code,
                    in_person = :in_person,
                    place = :place,
                    at_event = :at_event,
                    notes = :notes,
                    last_modified = now()
                WHERE id = :id
                """
            ),
            params,
        )
        session.commit()


def delete_seminar_rows(seminar_ids: list[int]) -> int:
    ids = sorted({int(x) for x in seminar_ids if x is not None})
    if not ids:
        return 0
    stmt = text(
        """
        DELETE FROM officials_analysis.isu_official_seminar
        WHERE id IN :ids
        """
    ).bindparams(bindparam("ids", expanding=True))
    with Session(engine) as session:
        result = session.execute(stmt, {"ids": ids})
        session.commit()
        return int(result.rowcount or 0)
