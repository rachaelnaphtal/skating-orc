"""
International officials activity: directory appointments (types 12–16) matched to
``segment_official`` panel work at ISU / international competitions (types 15–17),
Junior and Senior segments only, with discipline aligned to each appointment.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import bindparam, select, text
from sqlalchemy.orm import Session

try:
    from activityAnalysis.load_activity_data import (
        NO_DISCIPLINE_DIRECTORY_ID,
        activity_database_is_postgresql,
        calendar_years_for_usfs_season_codes,
        engine,
        segment_discipline_type_ids_for_directory,
    )
    from activityAnalysis.international_listing_seasons import filter_panel_to_season_codes
    from activityAnalysis.officials_analysis_models import (
        AppointmentTypes,
        Appointments,
        Disciplines,
        Levels,
        Officials,
    )
except ModuleNotFoundError:
    from load_activity_data import (
        NO_DISCIPLINE_DIRECTORY_ID,
        activity_database_is_postgresql,
        calendar_years_for_usfs_season_codes,
        engine,
        segment_discipline_type_ids_for_directory,
    )
    from international_listing_seasons import filter_panel_to_season_codes
    from officials_analysis_models import (
        AppointmentTypes,
        Appointments,
        Disciplines,
        Levels,
        Officials,
    )

from officials_competition_types import (
    OFFICIALS_COMPETITION_TYPE_DISPLAY_NAMES,
    OFFICIALS_COMPETITION_TYPE_IDS_ADULT_COLLEGIATE,
    OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL,
)

# Directory ``appointment_types.id`` for international roles (USFS directory).
INTERNATIONAL_APPOINTMENT_TYPE_IDS: tuple[int, ...] = (12, 13, 14, 15, 16)

# IJS ``segment_official.appointment_type_id`` (national panel roles on protocol).
_INTERNATIONAL_TO_NATIONAL_SEGMENT_APPOINTMENT_TYPE_ID: dict[int, int] = {
    12: 1,   # International Judge → Competition Judge
    13: 4,   # International Referee → Referee
    14: 9,   # International Technical Specialist → Technical Specialist
    15: 11,  # International Technical Controller → Technical Controller
    16: 8,   # International Data / Video Operator → Data Operator
}

INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID = 16

DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL = "Singles, Pairs, Dance & Synchronized"

# ``public.discipline_type.id``: Singles, Pairs, Ice Dance, Synchronized.
IDVO_SEGMENT_DISCIPLINE_TYPE_IDS: tuple[int, ...] = (1, 2, 3, 5)

COUNTABLE_SEGMENT_LEVELS: frozenset[str] = frozenset({"Junior", "Senior"})

_DETAIL_COLUMNS = [
    "official_id",
    "official_name",
    "mbr_number",
    "appointment_type_id",
    "appointment_type",
    "discipline_id",
    "discipline",
    "competition_id",
    "competition_year",
    "competition_name",
    "competition_type_id",
    "competition_type",
    "competition_qualifying",
    "competition_scope",
    "results_url",
    "start_date",
    "end_date",
    "segment_id",
    "segment_name",
    "segment_level",
    "segment_discipline",
    "role",
]

_SUMMARY_COLUMNS = [
    "official_id",
    "official_name",
    "mbr_number",
    "appointment_type_id",
    "appointment_type",
    "appointment_level",
    "appointment_level_id",
    "discipline_id",
    "discipline",
    "competition_count",
    "segment_count",
]


def national_segment_appointment_type_id(international_appointment_type_id: int) -> int | None:
    """Map directory international appointment type → protocol ``segment_official`` role id."""
    try:
        return _INTERNATIONAL_TO_NATIONAL_SEGMENT_APPOINTMENT_TYPE_ID.get(int(international_appointment_type_id))
    except (TypeError, ValueError):
        return None


def _empty_detail() -> pd.DataFrame:
    return pd.DataFrame(columns=_DETAIL_COLUMNS)


def _empty_summary() -> pd.DataFrame:
    return pd.DataFrame(columns=_SUMMARY_COLUMNS)


def get_international_appointment_type_options() -> pd.DataFrame:
    """International appointment types (id + name), sorted by name."""
    with Session(engine) as session:
        stmt = (
            select(AppointmentTypes.id, AppointmentTypes.name)
            .where(AppointmentTypes.id.in_(list(INTERNATIONAL_APPOINTMENT_TYPE_IDS)))
            .order_by(AppointmentTypes.name.asc())
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(rows, columns=["appointment_type_id", "appointment_type"])


def _nullable_int_for_sql(value: Any) -> int | None:
    """Coerce directory discipline ids for SQL bind params (``pd.NA`` → ``None``)."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_appointment_level(level_name: Any) -> str:
    """Directory level display — International (17) or ISU Championship (16)."""
    text_val = str(level_name or "").strip()
    lower = text_val.lower()
    if lower == "international":
        return "International"
    if lower in ("isu", "isu championship") or lower.startswith("isu "):
        return "ISU Championship"
    return text_val


def _pick_collapsed_appointment_level(levels: pd.Series) -> str:
    normalized = levels.dropna().astype(str).str.strip().str.lower()
    if normalized.str.startswith("isu").any() or (normalized == "isu").any():
        return "ISU Championship"
    if (normalized == "international").any():
        return "International"
    first = levels.dropna().astype(str).str.strip()
    return _normalize_appointment_level(first.iloc[0]) if not first.empty else ""


def _collapse_data_operator_appointments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge International Data / Video Operator directory rows per official into one
    appointment (all segment disciplines count toward the same row).
    """
    if df.empty:
        return df

    idvo_mask = df["appointment_type_id"] == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID
    if not idvo_mask.any():
        return df.drop_duplicates(
            subset=["official_id", "appointment_type_id", "discipline_id"]
        )

    other = df[~idvo_mask]
    idvo = df[idvo_mask]
    collapsed = idvo.groupby(
        ["official_id", "appointment_type_id"],
        as_index=False,
        dropna=False,
    ).agg(
        official_name=("official_name", "first"),
        mbr_number=("mbr_number", "first"),
        appointment_type=("appointment_type", "first"),
        appointment_level=("appointment_level", _pick_collapsed_appointment_level),
    )
    collapsed["discipline_id"] = pd.NA
    collapsed["discipline"] = DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL

    out = pd.concat([other, collapsed], ignore_index=True)
    if "appointment_level" in out.columns:
        out["appointment_level"] = out["appointment_level"].map(_normalize_appointment_level)
    return out.drop_duplicates(
        subset=["official_id", "appointment_type_id", "discipline_id"]
    )


def get_international_discipline_options(
    *,
    appointment_type_id: int | None = None,
    active_appointments_only: bool = True,
) -> pd.DataFrame:
    """
    Disciplines that appear on international directory appointments (for filters).
    Excludes IDVO split rows — that role is tracked as one combined appointment.
    """
    if appointment_type_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
        return pd.DataFrame(columns=["discipline_id", "discipline"])

    where_parts = [
        Appointments.appointment_type_id.in_(list(INTERNATIONAL_APPOINTMENT_TYPE_IDS)),
        Appointments.appointment_type_id != INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
        Appointments.discipline_id.isnot(None),
    ]
    if active_appointments_only:
        where_parts.append(Appointments.active.is_(True))
    if appointment_type_id is not None:
        where_parts.append(Appointments.appointment_type_id == int(appointment_type_id))

    with Session(engine) as session:
        stmt = (
            select(Disciplines.id, Disciplines.name)
            .join(Appointments, Appointments.discipline_id == Disciplines.id)
            .where(*where_parts)
            .distinct()
            .order_by(Disciplines.name.asc())
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(rows, columns=["discipline_id", "discipline"])


def get_international_officials_for_filters(
    *,
    appointment_type_id: int | None = None,
    discipline_id: int | None = None,
    official_id: int | None = None,
    active_appointments_only: bool = True,
) -> pd.DataFrame:
    """
    Officials with at least one international directory appointment matching filters.
    One row per (official, appointment type, discipline) appointment.
    """
    where_parts = [Appointments.appointment_type_id.in_(list(INTERNATIONAL_APPOINTMENT_TYPE_IDS))]
    if active_appointments_only:
        where_parts.append(Appointments.active.is_(True))
    if appointment_type_id is not None:
        where_parts.append(Appointments.appointment_type_id == int(appointment_type_id))
    if discipline_id is not None:
        if appointment_type_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
            pass  # IDVO is combined; discipline filter does not apply
        else:
            where_parts.append(Appointments.discipline_id == int(discipline_id))
            where_parts.append(
                Appointments.appointment_type_id
                != INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID
            )
    if official_id is not None:
        where_parts.append(Appointments.official_id == int(official_id))

    with Session(engine) as session:
        stmt = (
            select(
                Officials.id.label("official_id"),
                Officials.full_name.label("official_name"),
                Officials.mbr_number,
                Appointments.appointment_type_id,
                AppointmentTypes.name.label("appointment_type"),
                Levels.name.label("appointment_level"),
                Appointments.level_id.label("appointment_level_id"),
                Appointments.discipline_id,
                Disciplines.name.label("discipline"),
            )
            .join(Officials, Appointments.official_id == Officials.id)
            .join(AppointmentTypes, Appointments.appointment_type_id == AppointmentTypes.id)
            .outerjoin(Disciplines, Appointments.discipline_id == Disciplines.id)
            .outerjoin(Levels, Appointments.level_id == Levels.id)
            .where(*where_parts)
            .order_by(
                Officials.full_name.asc().nulls_last(),
                AppointmentTypes.name.asc(),
                Disciplines.name.asc().nulls_last(),
            )
        )
        rows = session.execute(stmt).all()
    df = pd.DataFrame(
        rows,
        columns=[
            "official_id",
            "official_name",
            "mbr_number",
            "appointment_type_id",
            "appointment_type",
            "appointment_level",
            "appointment_level_id",
            "discipline_id",
            "discipline",
        ],
    )
    df = _collapse_data_operator_appointments(df)
    if "appointment_level" in df.columns:
        df["appointment_level"] = df["appointment_level"].map(_normalize_appointment_level)
    return df


def _discipline_match_sql(
    *,
    intl_appointment_type_id: int,
    directory_discipline_id: int | None,
) -> tuple[str, dict[str, Any]]:
    """
    SQL fragment + bind params ensuring segment discipline matches the directory appointment.
    International Data / Video Operator: Singles, Pairs, Ice Dance, and Synchronized segments.
    """
    if int(intl_appointment_type_id) == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
        return (
            " AND s.discipline_type_id IN :idvo_segment_discipline_type_ids",
            {"idvo_segment_discipline_type_ids": list(IDVO_SEGMENT_DISCIPLINE_TYPE_IDS)},
        )

    nat_at_id = national_segment_appointment_type_id(intl_appointment_type_id)
    if nat_at_id is None:
        return " AND 1=0", {}

    dir_disc_id = _nullable_int_for_sql(directory_discipline_id)
    if dir_disc_id is None or dir_disc_id in (NO_DISCIPLINE_DIRECTORY_ID,):
        return "", {}

    seg_disc_ids = segment_discipline_type_ids_for_directory(
        dir_disc_id,
        nat_at_id,
    )
    if not seg_disc_ids:
        return " AND 1=0", {}
    return (
        " AND s.discipline_type_id IN :segment_discipline_type_ids",
        {"segment_discipline_type_ids": list(seg_disc_ids)},
    )


def allowed_segment_discipline_type_ids(
    intl_appointment_type_id: int,
    directory_discipline_id: Any,
) -> frozenset[int] | None:
    """
    Segment discipline filter for an appointment row.

    ``None`` = no discipline restriction; empty set = no matching segments.
    """
    if int(intl_appointment_type_id) == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
        return frozenset(IDVO_SEGMENT_DISCIPLINE_TYPE_IDS)

    nat_at_id = national_segment_appointment_type_id(intl_appointment_type_id)
    if nat_at_id is None:
        return frozenset()

    dir_disc_id = _nullable_int_for_sql(directory_discipline_id)
    if dir_disc_id is None or dir_disc_id in (NO_DISCIPLINE_DIRECTORY_ID,):
        return None

    seg_disc_ids = segment_discipline_type_ids_for_directory(dir_disc_id, nat_at_id)
    return frozenset(seg_disc_ids) if seg_disc_ids else frozenset()


def filter_panel_for_appointment(
    panel: pd.DataFrame,
    *,
    official_id: int,
    intl_appointment_type_id: int,
    directory_discipline_id: Any,
    national_role_ids: tuple[int, ...] | None = None,
) -> pd.DataFrame:
    """In-memory panel rows matching one directory appointment (and optional role set)."""
    if panel.empty:
        return panel

    mask = panel["official_id"] == int(official_id)
    if national_role_ids:
        mask &= panel["national_appointment_type_id"].isin([int(x) for x in national_role_ids])
    else:
        nat_at_id = national_segment_appointment_type_id(intl_appointment_type_id)
        if nat_at_id is None:
            return panel.iloc[0:0]
        mask &= panel["national_appointment_type_id"] == nat_at_id

    allowed = allowed_segment_discipline_type_ids(
        intl_appointment_type_id, directory_discipline_id
    )
    if allowed is not None:
        if not allowed:
            return panel.iloc[0:0]
        mask &= panel["segment_discipline_type_id"].isin(allowed)

    return panel.loc[mask]


def _normalize_merge_key_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Align merge-key dtypes (``discipline_id`` object vs float from groupby)."""
    if df.empty:
        return df
    out = df.copy()
    if "official_id" in out.columns:
        out["official_id"] = pd.to_numeric(out["official_id"], errors="coerce").astype("Int64")
    if "appointment_type_id" in out.columns:
        out["appointment_type_id"] = pd.to_numeric(
            out["appointment_type_id"], errors="coerce"
        ).astype("Int64")
    if "discipline_id" in out.columns:
        out["discipline_id"] = out["discipline_id"].map(_nullable_int_for_sql).astype("Int64")
    return out


def _national_qualifying_competition_sql_or() -> str:
    """SQL fragment: qualifying national comps, excluding adult/collegiate types 12–14."""
    excluded = sorted(OFFICIALS_COMPETITION_TYPE_IDS_ADULT_COLLEGIATE)
    excluded_list = ", ".join(str(x) for x in excluded)
    return f"""
                OR (
                    c.qualifying IS TRUE
                    AND c.officials_analysis_competition_type_id NOT IN ({excluded_list})
                )"""


def competition_scope_label(competition_type_id: Any, competition_qualifying: Any) -> str:
    """International (types 15–17) vs US national qualifying (not adult/collegiate)."""
    try:
        if int(competition_type_id) in OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL:
            return "International"
    except (TypeError, ValueError):
        pass
    try:
        if int(competition_type_id) in OFFICIALS_COMPETITION_TYPE_IDS_ADULT_COLLEGIATE:
            return "Other"
    except (TypeError, ValueError):
        pass
    if competition_qualifying is True or str(competition_qualifying or "").lower() in (
        "true",
        "t",
        "1",
    ):
        return "National"
    return "Other"


def _competition_season_sql_predicate(alias: str = "c") -> str:
    """Match USFS season codes and 4-digit calendar years on ``competition.year``."""
    return f"""
              AND btrim({alias}.year::text) ~ '^[0-9]+$'
              AND (
                (btrim({alias}.year::text)::integer IN :season_year_codes)
                OR (
                  btrim({alias}.year::text) ~ '^[0-9]{{4}}$'
                  AND btrim({alias}.year::text)::integer IN :calendar_year_codes
                )
              )
"""


def load_international_panel_segments_bulk(
    official_ids: list[int],
    *,
    season_codes: list[int] | None = None,
) -> pd.DataFrame:
    """
    One query: Junior/Senior panel segments at international competitions (types 15–17)
    and US national qualifying competitions (``competition.qualifying = true``).

    Resolves ``segment_official`` rows linked by ``official_id`` or judge name match.
    """
    cols = [
        "official_id",
        "national_appointment_type_id",
        "segment_discipline_type_id",
        "competition_id",
        "competition_year",
        "competition_name",
        "competition_type_id",
        "competition_qualifying",
        "results_url",
        "start_date",
        "end_date",
        "segment_id",
        "segment_name",
        "segment_level",
        "segment_discipline",
        "role",
    ]
    ids = sorted({int(x) for x in official_ids if x is not None})
    if not ids or not activity_database_is_postgresql():
        return pd.DataFrame(columns=cols)

    comp_type_ids = sorted(OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL)
    level_list = sorted(COUNTABLE_SEGMENT_LEVELS)

    season_sql = ""
    if season_codes:
        season_sql = _competition_season_sql_predicate("c")

    nat_qual_or = _national_qualifying_competition_sql_or()
    stmt = (
        text(
            f"""
            SELECT
                resolved.official_id,
                so.appointment_type_id AS national_appointment_type_id,
                s.discipline_type_id AS segment_discipline_type_id,
                c.id AS competition_id,
                c.year AS competition_year,
                c.name AS competition_name,
                c.officials_analysis_competition_type_id AS competition_type_id,
                c.qualifying AS competition_qualifying,
                c.results_url,
                c.start_date,
                c.end_date,
                s.id AS segment_id,
                s.name AS segment_name,
                s.level AS segment_level,
                dt.name AS segment_discipline,
                so.role
            FROM public.segment_official so
            INNER JOIN public.segment s ON s.id = so.segment_id
            INNER JOIN public.competition c ON c.id = s.competition_id
            LEFT JOIN public.discipline_type dt ON dt.id = s.discipline_type_id
            INNER JOIN LATERAL (
                SELECT COALESCE(
                    so.official_id,
                    (
                        SELECT jol.official_id
                        FROM public.judge_official_link jol
                        INNER JOIN public.judge j ON j.id = jol.judge_id
                        WHERE jol.status = 'linked'
                          AND jol.official_id IS NOT NULL
                          AND jol.official_id IN :official_ids
                          AND so.official_name IS NOT NULL
                          AND lower(btrim(j.name)) = lower(btrim(so.official_name))
                        LIMIT 1
                    )
                ) AS official_id
            ) resolved ON true
            WHERE resolved.official_id IN :official_ids
              AND s.level IN :segment_levels
              AND (
                c.officials_analysis_competition_type_id IN :competition_type_ids
                {nat_qual_or}
              ){season_sql}
            """
        )
        .bindparams(
            bindparam("official_ids", expanding=True),
            bindparam("competition_type_ids", expanding=True),
            bindparam("segment_levels", expanding=True),
        )
    )
    params: dict[str, Any] = {
        "official_ids": ids,
        "competition_type_ids": comp_type_ids,
        "segment_levels": level_list,
    }
    if season_codes:
        calendar_years = calendar_years_for_usfs_season_codes(season_codes)
        if not calendar_years:
            return pd.DataFrame(columns=cols)
        stmt = stmt.bindparams(
            bindparam("season_year_codes", expanding=True),
            bindparam("calendar_year_codes", expanding=True),
        )
        params["season_year_codes"] = [int(x) for x in season_codes]
        params["calendar_year_codes"] = [int(x) for x in calendar_years]
    try:
        with Session(engine) as session:
            rows = session.execute(stmt, params).mappings().all()
    except Exception:
        return pd.DataFrame(columns=cols)

    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)
    df["official_id"] = pd.to_numeric(df["official_id"], errors="coerce").astype("Int64")
    df["national_appointment_type_id"] = pd.to_numeric(
        df["national_appointment_type_id"], errors="coerce"
    )
    df["segment_discipline_type_id"] = pd.to_numeric(
        df["segment_discipline_type_id"], errors="coerce"
    )
    df["competition_qualifying"] = df["competition_qualifying"].fillna(False).astype(bool)
    df["competition_scope"] = df.apply(
        lambda r: competition_scope_label(
            r["competition_type_id"], r["competition_qualifying"]
        ),
        axis=1,
    )
    return df


def _finalize_activity_detail(detail: pd.DataFrame) -> pd.DataFrame:
    if detail.empty:
        return _empty_detail()
    detail = detail.drop_duplicates(
        subset=["official_id", "appointment_type_id", "segment_id", "role"]
    )
    detail["competition_type"] = detail["competition_type_id"].map(
        lambda x: OFFICIALS_COMPETITION_TYPE_DISPLAY_NAMES.get(int(x), f"Type {x}")
        if pd.notna(x)
        else ""
    )
    if "competition_qualifying" not in detail.columns:
        detail["competition_qualifying"] = False
    detail["competition_scope"] = detail.apply(
        lambda r: competition_scope_label(
            r["competition_type_id"], r.get("competition_qualifying")
        ),
        axis=1,
    )
    detail["role"] = detail["appointment_type"]
    return detail[_DETAIL_COLUMNS]


def get_international_official_activity_detail(
    *,
    appointment_type_id: int | None = None,
    discipline_id: int | None = None,
    official_id: int | None = None,
    active_appointments_only: bool = True,
    appointments: pd.DataFrame | None = None,
    panel_bulk: pd.DataFrame | None = None,
    season_codes: list[int] | None = None,
) -> pd.DataFrame:
    """
    One row per matching ``segment_official`` panel assignment at international
    competitions, attributed to the official's international directory appointment
    (matching appointment type + discipline).
    """
    if not activity_database_is_postgresql():
        return _empty_detail()

    if appointments is None:
        appointments = get_international_officials_for_filters(
            appointment_type_id=appointment_type_id,
            discipline_id=discipline_id,
            official_id=official_id,
            active_appointments_only=active_appointments_only,
        )
    if appointments.empty:
        return _empty_detail()

    official_ids = appointments["official_id"].astype(int).unique().tolist()
    if panel_bulk is not None:
        panel = panel_bulk
        if season_codes is not None:
            panel = filter_panel_to_season_codes(panel, season_codes)
    else:
        panel = load_international_panel_segments_bulk(
            official_ids, season_codes=season_codes
        )
    if panel.empty:
        return _empty_detail()

    panel_by_official = {
        int(oid): group for oid, group in panel.groupby("official_id", sort=False)
    }
    frames: list[pd.DataFrame] = []
    for row in appointments.itertuples(index=False):
        intl_at_id = int(row.appointment_type_id)
        if national_segment_appointment_type_id(intl_at_id) is None:
            continue
        sub = panel_by_official.get(int(row.official_id))
        if sub is None or sub.empty:
            continue
        part = filter_panel_for_appointment(
            sub,
            official_id=int(row.official_id),
            intl_appointment_type_id=intl_at_id,
            directory_discipline_id=row.discipline_id,
        )
        if part.empty:
            continue
        part = part.copy()
        part["appointment_type_id"] = intl_at_id
        part["discipline_id"] = row.discipline_id
        part["official_name"] = row.official_name
        part["mbr_number"] = row.mbr_number
        part["appointment_type"] = row.appointment_type
        part["discipline"] = row.discipline
        frames.append(part)

    if not frames:
        return _empty_detail()

    return _finalize_activity_detail(pd.concat(frames, ignore_index=True))


def summarize_international_activity(detail: pd.DataFrame) -> pd.DataFrame:
    """Aggregate detail rows to competition/segment counts per official × appointment × discipline."""
    if detail.empty:
        return _empty_summary()

    summary = (
        detail.groupby(
            [
                "official_id",
                "official_name",
                "mbr_number",
                "appointment_type_id",
                "appointment_type",
                "discipline_id",
                "discipline",
            ],
            dropna=False,
        )
        .agg(
            competition_count=("competition_id", "nunique"),
            segment_count=("segment_id", "nunique"),
        )
        .reset_index()
    )
    summary["competition_count"] = summary["competition_count"].astype(int)
    summary["segment_count"] = summary["segment_count"].astype(int)
    return summary.sort_values(
        by=["official_name", "appointment_type", "discipline"],
        na_position="last",
    ).reset_index(drop=True)


def get_international_official_activity_summary(
    *,
    appointment_type_id: int | None = None,
    discipline_id: int | None = None,
    official_id: int | None = None,
    active_appointments_only: bool = True,
    appointments: pd.DataFrame | None = None,
    panel_bulk: pd.DataFrame | None = None,
    season_codes: list[int] | None = None,
) -> pd.DataFrame:
    """Summary counts derived from :func:`get_international_official_activity_detail`."""
    if appointments is None:
        appointments = get_international_officials_for_filters(
            appointment_type_id=appointment_type_id,
            discipline_id=discipline_id,
            official_id=official_id,
            active_appointments_only=active_appointments_only,
        )
    if appointments.empty:
        return _empty_summary()

    detail = get_international_official_activity_detail(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_appointments_only,
        appointments=appointments,
        panel_bulk=panel_bulk,
        season_codes=season_codes,
    )
    summary = summarize_international_activity(detail)

    appt_keys = appointments
    if summary.empty:
        out = appt_keys.copy()
        out["competition_count"] = 0
        out["segment_count"] = 0
        return out[_SUMMARY_COLUMNS]

    merge_keys = ["official_id", "appointment_type_id", "discipline_id"]
    counts = _normalize_merge_key_dtypes(
        summary[merge_keys + ["competition_count", "segment_count"]]
    )
    out = _normalize_merge_key_dtypes(appt_keys).merge(counts, on=merge_keys, how="left")
    out["competition_count"] = (
        pd.to_numeric(out["competition_count"], errors="coerce").fillna(0).astype(int)
    )
    out["segment_count"] = (
        pd.to_numeric(out["segment_count"], errors="coerce").fillna(0).astype(int)
    )
    return out.sort_values(
        by=["official_name", "appointment_type", "discipline"],
        na_position="last",
    ).reset_index(drop=True)[_SUMMARY_COLUMNS]
