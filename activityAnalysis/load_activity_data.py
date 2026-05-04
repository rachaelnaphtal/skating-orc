import os
import pandas as pd
try:
    from activityAnalysis.officials_analysis_models import (
        Base,
        Officials,
        Appointments,
        Assignment,
        Disciplines,
        Competition,
        CompetitionType,
        AppointmentTypes,
        Levels,
    )
except ModuleNotFoundError:
    from officials_analysis_models import (
        Base,
        Officials,
        Appointments,
        Assignment,
        Disciplines,
        Competition,
        CompetitionType,
        AppointmentTypes,
        Levels,
    )
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select, func, and_, or_, case, text, bindparam
import math
import re
from datetime import datetime

appointment_codes_file = "activityAnalysis/Appointments_to_database.xlsx"
DEFAULT_ACTIVITY_DB_URL = "sqlite:////tmp/activity_tracker.db"


def _resolve_database_url():
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        return DEFAULT_ACTIVITY_DB_URL
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql://", 1)
    return database_url


def activity_database_is_postgresql() -> bool:
    """True when activity/judging data is expected on PostgreSQL (``public`` judging tables)."""
    return _resolve_database_url().startswith("postgresql")


def _build_engine(database_url):
    engine_kwargs = {"echo": False}
    if database_url.startswith("sqlite:"):
        engine_kwargs["execution_options"] = {
            "schema_translate_map": {"officials_analysis": None}
        }
    else:
        engine_kwargs["connect_args"] = {"options": "-csearch_path=officials_analysis"}
    return create_engine(database_url, **engine_kwargs)


def _seed_local_sqlite_data_if_empty(db_engine):
    """Seed tiny local data so the Streamlit app can render controls."""
    with Session(db_engine) as session:
        existing = session.execute(select(func.count()).select_from(Appointments)).scalar_one()
        if existing:
            return

        current_year = datetime.now().year
        appointment_type = AppointmentTypes(id=1, name="Competition Judge")
        discipline = Disciplines(id=2, name="Synchronized")
        level = Levels(id=7, name="National")
        official = Officials(
            id=1,
            mbr_number="LOCAL-1",
            first_name="Local",
            last_name="Official",
            full_name="Local Official",
            is_coach=False,
            email=None,
            phone=None,
            city="Local",
            state="NA",
            region="Pacific Coast",
        )
        appointment = Appointments(
            id=1,
            official_id=1,
            appointment_type_id=1,
            discipline_id=2,
            level_id=7,
            achieved_date=datetime(current_year - 2, 1, 1).date(),
            active=True,
        )
        competition_type = CompetitionType(
            id=8, name="US Synchronized Skating Championships"
        )
        competition = Competition(
            id=1,
            name="Local Seed Competition",
            year=current_year - 1,
            competition_type_id=8,
        )
        assignment = Assignment(
            id=1,
            competition_id=1,
            official_id=1,
            discipline_id=2,
            appointment_type_id=1,
            chief=False,
            lower_levels_only=False,
        )
        session.add_all(
            [
                appointment_type,
                discipline,
                level,
                official,
                appointment,
                competition_type,
                competition,
                assignment,
            ]
        )
        session.commit()


def get_engine():
    database_url = _resolve_database_url()
    db_engine = _build_engine(database_url)
    if database_url.startswith("sqlite:"):
        Base.metadata.create_all(db_engine)
        _seed_local_sqlite_data_if_empty(db_engine)
    return db_engine


engine = get_engine()

# competition_type.id — identifies US Championships competitions (find/create rows).
# (Do not confuse with appointment_type ids, which are separate.)
US_CHAMPIONSHIPS_COMPETITION_TYPE_ID = 4
REFEREE_APPOINTMENT_TYPE_ID = 4
NATIONAL_LEVEL_ID = 7
SECTIONAL_LEVEL_ID = 2
# USFS "level 2" in the directory for these roles; other appointment types use ``SECTIONAL_LEVEL_ID`` (2) for sectionals.
SCORING_SECTIONAL_LEVEL2_ID = 8
APPOINTMENT_TYPES_SCORING_SECTIONAL_LEVEL2 = frozenset(
    ("Scoring Official", "Scoring System Technician")
)
DISC_DANCE_ID = 4
DISC_SYNCHRO_ID = 2
DISC_SINGLES_PAIRS_ID = 9
# Directory discipline id for Pairs-only appointments (not the Singles/Pairs combined id 9).
DISC_PAIRS_ID = 8
# Same convention as ``Assignment`` / ``activity_tracker_app`` omit label: "no discipline" rows.
NO_DISCIPLINE_DIRECTORY_ID = 7

# NQS (National Qualifying Series) report: directory ``levels.id`` for "qualifying" tier (see USFS data).
NQS_DIRECTORY_QUALIFYING_LEVEL_ID = 9
# ``public.discipline_type.id`` on IJS segments: Singles, Pairs, Ice Dance (Ice Dance = 3 per ISU export).
NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES = 1
NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS = 2
NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE = 3

# National directory appointment ``name`` → international (ISU) appointment ``name``.
# IDs for Referee/ITS/ITC/IDVO are typically 13–16; International Judge id comes from DB.
_NATIONAL_TO_INTERNATIONAL_APPOINTMENT_NAME = {
    "Competition Judge": "International Judge",
    "Referee": "International Referee",
    "Technical Specialist": "International Technical Specialist",
    "Technical Controller": "International Technical Controller",
    "Data Operator": "International Data / Video Operator",
}


# Keys must match ``SUMMARY_COMPETITION_TYPES`` labels in ``activity_tracker_app``.
REF_REPORT_COMP_GROUPS = frozenset(
    {
        "US Championships",
        "US Synchronized Skating Championships",
        "SPD Sectionals",
        "Synchronized Sectionals",
    }
)


def _ref_assignment_discipline_ids(comp_group_name, discipline_id):
    """Assignment rows counted for this report use these discipline ids."""
    if discipline_id is not None:
        return [int(discipline_id)]
    if comp_group_name in ("US Championships", "SPD Sectionals"):
        return [DISC_DANCE_ID, DISC_SINGLES_PAIRS_ID]
    if comp_group_name in (
        "US Synchronized Skating Championships",
        "Synchronized Sectionals",
    ):
        return [DISC_SYNCHRO_ID]
    raise ValueError(f"Unknown competition group: {comp_group_name!r}")


def _referee_eligibility_where(comp_group_name, discipline_filter_id=None):
    """SQLAlchemy boolean expression: this official has a qualifying referee appointment."""
    base = Appointments.appointment_type_id == REFEREE_APPOINTMENT_TYPE_ID
    if comp_group_name == "US Championships":
        disc_clause = (
            Appointments.discipline_id == discipline_filter_id
            if discipline_filter_id is not None
            else Appointments.discipline_id.in_([DISC_DANCE_ID, DISC_SINGLES_PAIRS_ID])
        )
        return and_(base, Appointments.level_id == NATIONAL_LEVEL_ID, disc_clause)
    if comp_group_name == "US Synchronized Skating Championships":
        return and_(
            base,
            Appointments.level_id == NATIONAL_LEVEL_ID,
            Appointments.discipline_id
            == (discipline_filter_id if discipline_filter_id is not None else DISC_SYNCHRO_ID),
        )
    if comp_group_name == "SPD Sectionals":
        disc_clause = (
            Appointments.discipline_id == discipline_filter_id
            if discipline_filter_id is not None
            else Appointments.discipline_id.in_([DISC_DANCE_ID, DISC_SINGLES_PAIRS_ID])
        )
        return and_(
            base,
            disc_clause,
            or_(
                Appointments.level_id == NATIONAL_LEVEL_ID,
                Appointments.level_id == SECTIONAL_LEVEL_ID,
            ),
        )
    if comp_group_name == "Synchronized Sectionals":
        return and_(
            base,
            Appointments.discipline_id
            == (discipline_filter_id if discipline_filter_id is not None else DISC_SYNCHRO_ID),
            or_(
                Appointments.level_id == NATIONAL_LEVEL_ID,
                Appointments.level_id == SECTIONAL_LEVEL_ID,
            ),
        )
    raise ValueError(f"Unknown competition group: {comp_group_name!r}")


def get_referee_eligible_official_ids(comp_group_name, discipline_id=None):
    """Official ids with at least one referee appointment matching group eligibility rules."""
    if comp_group_name not in REF_REPORT_COMP_GROUPS:
        raise ValueError(f"Unknown competition group: {comp_group_name!r}")
    where_elig = _referee_eligibility_where(comp_group_name, discipline_id)
    with Session(engine) as session:
        stmt = (
            select(Appointments.official_id)
            .distinct()
            .where(where_elig, Appointments.official_id.isnot(None))
        )
        rows = session.execute(stmt).all()
    return sorted({int(r[0]) for r in rows if r[0] is not None})


def get_referee_discipline_options_for_comp_group(comp_group_name):
    """Discipline ids/names offered for 'single discipline' on the referee report."""
    if comp_group_name not in REF_REPORT_COMP_GROUPS:
        return pd.DataFrame(columns=["discipline_id", "discipline_name"])
    ids = _ref_assignment_discipline_ids(comp_group_name, None)
    if not ids:
        return pd.DataFrame(columns=["discipline_id", "discipline_name"])
    with Session(engine) as session:
        stmt = (
            select(Disciplines.id, Disciplines.name)
            .where(Disciplines.id.in_(ids))
            .order_by(Disciplines.name)
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(rows, columns=["discipline_id", "discipline_name"])


def create_appointment_code_df(file=appointment_codes_file):
    df = pd.read_excel(
        file,
        # dtype={"Chief": bool}
        # converters={
        #     "Chief": lambda x: pd.notna(x),
        #     "Lower_Levels_Only": lambda x: pd.notna(x)
        # }
    )
    df["Chief"] = df["Chief"].notna()
    df["Lower_Levels_Only"] = df["Lower_Levels_Only"].notna()
    return df


def parse_event_title(value, is_synchro):
    if pd.isna(value):
        return None, None

    text = str(value)

    match = re.match(r"^(\d{4})\s*(.*)", text)
    if not match:
        return None, text  # fallback: no year found

    year = int(match.group(1))
    event_name = match.group(2).strip()

    if event_name == "":
        if is_synchro:
            event_name = "US Synchronized Skating Championships"
        else:
            event_name = "US Championships"

    return year, event_name


def find_competition_type(competition_name, is_synchro):
    if is_synchro:
        if competition_name == "US Synchronized Skating Championships":
            return 8
        if "MidPac" in competition_name:
            return 9
        if "Eastern" in competition_name:
            return 5
        if "Midwestern" in competition_name:
            return 6
        if "Pacific" in competition_name:
            return 7
    else:
        if competition_name == "US Championships":
            return 4
        if "Eastern" in competition_name:
            return 1
        if "Midwest" in competition_name:
            return 2
        if "Pacific" in competition_name:
            return 3
    print(f"No type found for {competition_name}")


def load_event(
    appointment_code_df, file="activityAnalysis/US_Champs.xlsx", is_synchro=False
):
    df_raw = pd.read_excel(file, header=None)

    all_data = []

    for col in range(0, df_raw.shape[1], 3):
        name_col = col
        pos_col = col + 1

        if pos_col >= df_raw.shape[1]:
            continue

        year_raw = df_raw.iloc[0, name_col]

        year, event_name = parse_event_title(year_raw, is_synchro)

        if year is None:
            continue

        data = df_raw.iloc[2:, [name_col, pos_col]].copy()
        data.columns = ["Person", "Position"]

        data = data.dropna(subset=["Person"])

        data["Year"] = year
        data["CompetitionName"] = event_name

        # pass flag into event type logic
        data["CompetitionType"] = data["CompetitionName"].apply(
            lambda x: find_competition_type(x, is_synchro=is_synchro)
        )

        all_data.append(data)

    final_df = pd.concat(all_data, ignore_index=True)

    final_df = final_df.merge(appointment_code_df, on="Position", how="left")

    return final_df


def find_missing_officials_names_and_positions(session, officials_df):
    missing_positions_df = officials_df[officials_df["Discipline"].isnull()]
    if not missing_positions_df.empty:
        print("missing positions:")
        print(missing_positions_df)

    results = session.query(Officials.full_name).all()

    db_names = {
        name
        # normalize(name)
        for (name,) in results
        if name is not None
    }

    excel_names = set(officials_df["Person"])

    retired_df = pd.read_excel("activityAnalysis/Retired_officials.xlsx")
    retired_names = set(retired_df["Name"])

    missing = excel_names - db_names - retired_names

    print(f"Missing ({len(missing)}):")
    for name in sorted(missing):
        print(name)


def get_or_create_competition(
    session,
    year,
    competition_name,
    competition_type,
    *,
    sectionals_match_by_type_and_year: bool = False,
):
    """
    Find or create a ``Competition`` row.

    For **sectional** loads, ``sectionals_match_by_type_and_year`` reuses the first
    existing competition for the same calendar ``year`` and ``competition_type``,
    so multiple spreadsheet columns with different event names (same type/year) do
    not create duplicate competition rows. Championships and other loads keep the
    default: match on (name, year, competition type).
    """
    if sectionals_match_by_type_and_year:
        existing = (
            session.query(Competition)
            .filter(
                Competition.year == year,
                Competition.competition_type_id == competition_type,
            )
            .order_by(Competition.id)
            .first()
        )
        if existing is not None:
            return existing
    else:
        comp = (
            session.query(Competition)
            .filter_by(
                name=competition_name, year=year, competition_type_id=competition_type
            )
            .one_or_none()
        )
        if comp is not None:
            return comp

    comp = Competition(
        name=competition_name, year=year, competition_type_id=competition_type
    )
    session.add(comp)
    session.flush()  # get ID
    return comp


def insert_assignments(
    session, assignments_df, officials, *, sectionals_dedupe_by_type_year: bool = False
):
    retired_df = pd.read_excel("activityAnalysis/Retired_officials.xlsx")
    retired_names = set(retired_df["Name"])

    for _, row in assignments_df.iterrows():
        competiton_name = row["CompetitionName"]
        competition_type_id = row["CompetitionType"]

        official_id = officials.get(row["Person"])

        if not official_id:
            if row["Person"] not in retired_names:
                print(f"Missing official: {row['Person']}")
            continue

        discipline_id = row.Discipline
        appointment_type_id = row.Appointment_Type_Id

        if not discipline_id or not appointment_type_id or math.isnan(discipline_id):
            print(f"Missing mapping: {row}")
            continue

        comp = get_or_create_competition(
            session,
            row["Year"],
            competiton_name,
            competition_type_id,
            sectionals_match_by_type_and_year=sectionals_dedupe_by_type_year,
        )

        # 🚫 Check if already exists
        exists = (
            session.query(Assignment)
            .filter_by(
                competition_id=comp.id,
                official_id=official_id,
                discipline_id=int(discipline_id),
                appointment_type_id=int(appointment_type_id),
            )
            .first()
        )

        if exists:
            continue  # skip duplicates

        assignment = Assignment(
            competition_id=comp.id,
            official_id=official_id,
            discipline_id=int(discipline_id),
            appointment_type_id=int(appointment_type_id),
            chief=bool(row.get("Chief", False)),
            lower_levels_only=bool(row.get("Lower_Levels_Only", False)),
        )

        session.add(assignment)

    session.commit()


def load_history(write_to_database=False):
    appointment_code_df = create_appointment_code_df()
    us_champs_df = load_event(
        appointment_code_df, file="activityAnalysis/US_Champs.xlsx"
    )
    us_synchro_champs_df = load_event(
        appointment_code_df, file="activityAnalysis/US_SYS_Champs.xlsx", is_synchro=True
    )
    us_synchro_sectionals_df = load_event(
        appointment_code_df,
        file="activityAnalysis/US_SYS_Sectionals.xlsx",
        is_synchro=True,
    )
    us_spd_sectionals_df = load_event(
        appointment_code_df,
        file="activityAnalysis/US_SPD_Sectionals.xlsx",
        is_synchro=False,
    )

    with Session(engine) as session:
        find_missing_officials_names_and_positions(session, us_champs_df)
        find_missing_officials_names_and_positions(session, us_synchro_champs_df)
        find_missing_officials_names_and_positions(session, us_synchro_sectionals_df)
        find_missing_officials_names_and_positions(session, us_spd_sectionals_df)

        officials = {
            o.full_name: o.id for o in session.query(Officials).all() if o.full_name
        }

        if write_to_database:
            insert_assignments(session, us_champs_df, officials)
            insert_assignments(session, us_synchro_champs_df, officials)
            insert_assignments(
                session,
                us_synchro_sectionals_df,
                officials,
                sectionals_dedupe_by_type_year=True,
            )
            insert_assignments(
                session,
                us_spd_sectionals_df,
                officials,
                sectionals_dedupe_by_type_year=True,
            )


def get_assignments_for_person(person_name):
    with Session(engine) as session:
        officials = {
            o.full_name: o.id for o in session.query(Officials).all() if o.full_name
        }

        official_id = officials.get(person_name)
        stmt = (
            select(
                Assignment.id,
                Officials.full_name,
                Competition.name.label("competition"),
                Competition.year,
                Disciplines.name.label("discipline"),
                AppointmentTypes.name.label("appointment_type"),
                Assignment.chief,
                Assignment.lower_levels_only,
            )
            .join(Officials, Assignment.official_id == Officials.id)
            .join(Competition, Assignment.competition_id == Competition.id)
            .join(Disciplines, Assignment.discipline_id == Disciplines.id)
            .join(
                AppointmentTypes, Assignment.appointment_type_id == AppointmentTypes.id
            )
            .where(Officials.id == official_id)
        )

        rows = session.execute(stmt).all()
        for row in rows:
            print(row)
        return rows


def get_number_assignments_per_competition_type(competition_types):
    with Session(engine) as session:
        stmt = (
            select(
                Officials.full_name,
                func.count(func.distinct(Competition.year)).label("num_years"),
            )
            .join(Assignment, Assignment.official_id == Officials.id)
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(Competition.competition_type_id.in_(competition_types))
            .group_by(Officials.full_name)
            .order_by("num_years")
        )

        rows = session.execute(stmt).all()
        for row in rows:
            print(row)
        return rows


def get_qualified_officials(
    discipline_id,
    appointment_type_id,
    level_ids,
    include_appointment_level: bool = False,
    active_appointments_only: bool = True,
):
    with Session(engine) as session:
        if discipline_id is None:
            # Directory exports now use 7 for no discipline; older rows may still be NULL.
            disc_filter = or_(
                Appointments.discipline_id.is_(None),
                Appointments.discipline_id == NO_DISCIPLINE_DIRECTORY_ID,
            )
        else:
            resolved = _resolve_discipline_ids(discipline_id, appointment_type_id)
            if len(resolved) == 1:
                disc_filter = Appointments.discipline_id == resolved[0]
            else:
                disc_filter = Appointments.discipline_id.in_(resolved)
        where_parts = [
            disc_filter,
            Appointments.appointment_type_id == appointment_type_id,
            Appointments.level_id.in_(level_ids),
        ]
        if active_appointments_only:
            where_parts.append(Appointments.active.is_(True))
        select_cols = [
            Officials.id,
            Officials.full_name,
            Officials.region,
            Appointments.achieved_date,
        ]
        if include_appointment_level:
            select_cols.append(Appointments.level_id)
        stmt = (
            select(*select_cols)
            .join(Appointments, Appointments.official_id == Officials.id)
            .where(*where_parts)
            .distinct()
        )

        rows = session.execute(stmt).all()

    out_cols = ["official_id", "full_name", "region", "achieved_date"]
    if include_appointment_level:
        out_cols.append("level_id")
    return pd.DataFrame(rows, columns=out_cols)


def national_appointment_type_has_isu_mapping(national_appointment_type_id) -> bool:
    """
    True if the championships report should show the **ISU** column for this national
    ``appointment_type_id`` (a defined pair in
    ``_NATIONAL_TO_INTERNATIONAL_APPOINTMENT_NAME``).
    """
    try:
        nat_tid = int(national_appointment_type_id)
    except (TypeError, ValueError):
        return False
    with Session(engine) as session:
        nat_name = (
            session.execute(
                select(AppointmentTypes.name).where(AppointmentTypes.id == nat_tid)
            ).scalar()
            or ""
        ).strip()
    return nat_name in _NATIONAL_TO_INTERNATIONAL_APPOINTMENT_NAME


def get_official_ids_with_isu_appointment(
    official_ids: list | tuple,
    national_appointment_type_id,
    discipline_id,
    *,
    active_appointments_only: bool = True,
) -> set[int]:
    """
    Officials having an active appointment in the international role that
    pairs with the selected national ``national_appointment_type_id``.

    * Directory **level** on the international appointment is not filtered (any level id).
    * Discipline must match the report filter (including Singles/Pairs resolution), except
      for **Data Operator**: an ``International Data / Video Operator`` appointment in
      **any** discipline counts.
    * No matching international role for this national type → empty set (callers show "No").
    """
    if not official_ids:
        return set()
    oids = [int(x) for x in official_ids if x is not None and not pd.isna(x)]
    if not oids:
        return set()
    try:
        nat_tid = int(national_appointment_type_id)
    except (TypeError, ValueError):
        return set()
    with Session(engine) as session:
        nat_name = (
            session.execute(
                select(AppointmentTypes.name).where(AppointmentTypes.id == nat_tid)
            ).scalar()
            or ""
        ).strip()
        intl_name = _NATIONAL_TO_INTERNATIONAL_APPOINTMENT_NAME.get(nat_name)
        if not intl_name:
            return set()
        intl_row = session.execute(
            select(AppointmentTypes.id).where(AppointmentTypes.name == intl_name)
        ).first()
        if not intl_row:
            return set()
        intl_id = int(intl_row[0])

    # Data Operator + International D/VO: any discipline on the intl appointment counts.
    match_discipline = nat_name != "Data Operator"

    where_parts = [
        Appointments.official_id.in_(oids),
        Appointments.appointment_type_id == intl_id,
    ]
    if active_appointments_only:
        where_parts.append(Appointments.active.is_(True))
    if match_discipline:
        if discipline_id is None or pd.isna(discipline_id):
            where_parts.append(
                or_(
                    Appointments.discipline_id.is_(None),
                    Appointments.discipline_id == NO_DISCIPLINE_DIRECTORY_ID,
                )
            )
        else:
            resolved = _resolve_discipline_ids(discipline_id, nat_tid)
            if len(resolved) == 1:
                where_parts.append(Appointments.discipline_id == resolved[0])
            else:
                where_parts.append(Appointments.discipline_id.in_(resolved))

    with Session(engine) as session:
        stmt = select(Appointments.official_id).where(*where_parts).distinct()
        rows = session.execute(stmt).all()
    return {int(r[0]) for r in rows if r[0] is not None}


def _level_id_to_name_map(level_ids) -> dict:
    """``Levels.id`` → display ``name`` (directory text)."""
    u = sorted(
        {int(x) for x in level_ids if x is not None and not (isinstance(x, float) and pd.isna(x))}
    )
    if not u:
        return {}
    with Session(engine) as session:
        rows = session.execute(
            select(Levels.id, Levels.name).where(Levels.id.in_(u))
        ).all()
    return {int(r[0]): (r[1] or "").strip() for r in rows}


def sectional_qualified_level_ids(appointment_type_id) -> list:
    """
    ``level_id`` values that qualify someone for the **sectionals** activity matrix
    (sectional-level or national). Most roles use ``SECTIONAL_LEVEL_ID`` (2) for
    sectional; Scoring Official and Scoring System Technician use USFS "level 2"
    (``SCORING_SECTIONAL_LEVEL2_ID`` = 8) for their sectional work.
    """
    try:
        atid = int(appointment_type_id)
    except (TypeError, ValueError):
        return [SECTIONAL_LEVEL_ID, NATIONAL_LEVEL_ID]
    with Session(engine) as session:
        name = session.execute(
            select(AppointmentTypes.name).where(AppointmentTypes.id == atid)
        ).scalar()
    n = (name or "").strip()
    if n in APPOINTMENT_TYPES_SCORING_SECTIONAL_LEVEL2:
        return [SCORING_SECTIONAL_LEVEL2_ID, NATIONAL_LEVEL_ID]
    return [SECTIONAL_LEVEL_ID, NATIONAL_LEVEL_ID]


# Directory: Singles = 1, Singles/Pairs = 9 (``DISC_SINGLES_PAIRS_ID``). Do not use 8
# here — 8 is ``SCORING_SECTIONAL_LEVEL2_ID``; older code mistakenly compared SP to 8.
SINGLES_DISCIPLINE_ID = 1
SINGLES_PAIRS_APPT_TYPES = {1, 4}  # Competition Judge, Referee


def _resolve_discipline_ids(discipline_id, appointment_type_id):
    """
    For Competition Judges and Referees choosing **Singles/Pairs** in the UI, count both
    **Singles (1)** and **Singles/Pairs (9)** in directory and assignment data. (Do not
    confuse with discipline id **8**, which is Pairs as its own column in this app.)
    """
    if discipline_id is None:
        return None
    try:
        atid = int(appointment_type_id)
        did = int(discipline_id)
    except (TypeError, ValueError):
        return [discipline_id]
    if atid in SINGLES_PAIRS_APPT_TYPES and did == DISC_SINGLES_PAIRS_ID:
        return [SINGLES_DISCIPLINE_ID, DISC_SINGLES_PAIRS_ID]
    return [did]


def _normalize_competition_type_ids(competition_type_id):
    """Single id or iterable → list of ints for SQL ``IN``."""
    if isinstance(competition_type_id, (list, tuple, set)):
        return [int(x) for x in competition_type_id]
    return [int(competition_type_id)]


def _lower_levels_only_clauses(include_lower_levels) -> list:
    """
    Filter ``Assignment`` rows by ``lower_levels_only`` (championships / national work).

    * ``None`` — no extra filter (e.g. sectionals).
    * ``True`` — no extra filter (championships with “include lower level” work).
    * ``False`` — only assignments with ``lower_levels_only`` false (hide lower-levels-only).
    """
    if include_lower_levels is not False:
        return []
    return [Assignment.lower_levels_only.is_(False)]


# SPD sectionals (1–3) + synchronized sectionals (5–7, 9), aggregated by calendar year.
SECTIONAL_ACTIVITY_COMPETITION_TYPES = (1, 2, 3, 5, 6, 7, 9)
# When “no discipline” is selected on the sectionals view, the matrix can be limited to:
SECTIONAL_SPD_COMPETITION_TYPES = (1, 2, 3)
SECTIONAL_SYNCHRO_COMPETITION_TYPES = (5, 6, 7, 9)

# Other-role rows for sectionals: same competition types as the matrix (sectionals only;
# type 8 US Sync Championships is not a sectional and is excluded).
SECTIONAL_ANY_ROLE_COMPETITION_TYPES = (1, 2, 3, 5, 6, 7, 9)

SPD_SECTIONAL_BUCKET_TYPES = frozenset({1, 2, 3})
SYNCHRO_SECTIONAL_BUCKET_TYPES = frozenset({5, 6, 7, 9})


def sectional_epm_letters_from_competition_type_id(competition_type_id) -> list[str]:
    """
    E / M / P suffix letters from USFS ``competition_type_id`` (not competition name).
    Type 9 (Midwestern/Pacific Synchro Sectional) contributes both M and P.
    """
    t = int(competition_type_id)
    if t in (1, 5):
        return ["E"]
    if t in (2, 6):
        return ["M"]
    if t in (3, 7):
        return ["P"]
    if t == 9:
        return ["M", "P"]
    return []


def build_sectional_year_allowed_type_map(
    primary_df: pd.DataFrame,
) -> dict:
    """
    From selected-role sectional rows (official_id, year, competition_type_id),
    map (official_id, year) to allowed competition_type_ids for "other roles"
    in the same bucket: SPD types {1,2,3} vs synchro sectionals {5,6,7,9}.
    """
    if primary_df is None or primary_df.empty:
        return {}
    need = {"official_id", "year", "competition_type_id"}
    if not need.issubset(primary_df.columns):
        return {}
    out = {}
    for (oid, yr), g in primary_df.groupby(["official_id", "year"], sort=False):
        u = {int(x) for x in g["competition_type_id"].dropna().unique()}
        allowed = set()
        if u & SPD_SECTIONAL_BUCKET_TYPES:
            allowed |= SPD_SECTIONAL_BUCKET_TYPES
        if u & SYNCHRO_SECTIONAL_BUCKET_TYPES:
            allowed |= SYNCHRO_SECTIONAL_BUCKET_TYPES
        if allowed:
            out[(int(oid), int(yr))] = frozenset(allowed)
    return out


def get_assignment_years(
    discipline_id,
    appointment_type_id,
    competition_type_id,
    include_lower_levels=None,
):
    discipline_ids = _resolve_discipline_ids(discipline_id, appointment_type_id)
    ct_ids = _normalize_competition_type_ids(competition_type_id)

    with Session(engine) as session:
        filters = [
            Assignment.appointment_type_id == appointment_type_id,
            Competition.competition_type_id.in_(ct_ids),
        ]
        if discipline_ids is not None:
            filters.append(Assignment.discipline_id.in_(discipline_ids))
        filters.extend(_lower_levels_only_clauses(include_lower_levels))

        stmt = (
            select(Assignment.official_id, Competition.year)
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(*filters)
            .distinct()
            .order_by(Competition.year.desc())
        )

        rows = session.execute(stmt).all()

    return pd.DataFrame(rows, columns=["official_id", "year"])


def get_years_all_lower_level_only_in_role(
    official_ids, discipline_id, appointment_type_id, competition_type_id
):
    """
    Calendar years where every assignment in the selected role / discipline filter
    for the competition type(s) is ``lower_levels_only`` (at least one assignment).
    Ignores the championships "Include Lower Levels" UI filter so the mark reflects
    actual stored flags. Used for an **L** prefix next to the checkmark.
    """
    if not official_ids:
        return pd.DataFrame(columns=["official_id", "year"])
    discipline_ids = _resolve_discipline_ids(discipline_id, appointment_type_id)
    ct_ids = _normalize_competition_type_ids(competition_type_id)

    with Session(engine) as session:
        filters = [
            Assignment.appointment_type_id == appointment_type_id,
            Competition.competition_type_id.in_(ct_ids),
            Assignment.official_id.in_(list(official_ids)),
        ]
        if discipline_ids is not None:
            filters.append(Assignment.discipline_id.in_(discipline_ids))

        stmt = (
            select(
                Assignment.official_id,
                Competition.year,
                Assignment.lower_levels_only,
            )
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(*filters)
        )
        rows = session.execute(stmt).all()

    if not rows:
        return pd.DataFrame(columns=["official_id", "year"])

    dfr = pd.DataFrame(
        rows, columns=["official_id", "year", "lower_levels_only"]
    )
    out_rows = []
    for (oid, y), g in dfr.groupby(["official_id", "year"], sort=False):
        if g.empty:
            continue
        lo = g["lower_levels_only"]
        # DB may use bool or 0/1 (e.g. SQLite)
        t = (lo == True) | (lo == 1) | (lo == 1.0)
        if t.all() and len(g) > 0:
            out_rows.append({"official_id": int(oid), "year": int(y)})
    return pd.DataFrame(out_rows)


def build_activity_matrix(qualified_df, activity_df):
    current_year = datetime.now().year

    # ---- merge so EVERY official is kept ----
    df = qualified_df.merge(activity_df, on="official_id", how="left")

    # ---- Years in grade ----
    df["achieved_year"] = pd.to_datetime(df["achieved_date"], errors="coerce").dt.year

    years_in_grade = (
        df.groupby(["official_id", "full_name"])["achieved_year"].min().reset_index()
    )
    years_in_grade["years_in_grade"] = current_year - years_in_grade["achieved_year"]

    # ---- Most recent year served ----
    recent_year = (
        df.dropna(subset=["year"])
        .groupby(["official_id", "full_name"])["year"]
        .max()
        .reset_index()
        .rename(columns={"year": "most_recent_year"})
    )

    # ---- Activity matrix ----
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    df["worked"] = df["year"].notna().astype(int)

    pivot = df.pivot_table(
        index="official_id",
        columns="year",
        values="worked",
        aggfunc="max",
        fill_value=0,
    )

    pivot = pivot.merge(
        qualified_df[["official_id", "full_name"]], on="official_id", how="left"
    )

    # ensure ALL officials exist
    pivot = pivot.reset_index().merge(
        qualified_df[["official_id", "full_name"]],
        on="official_id",
        how="right",  # 🔥 this is the key fix
    )

    # bring back names
    pivot = pivot.reset_index().merge(
        qualified_df[["official_id", "full_name"]], on="official_id", how="left"
    )

    # ---- Merge metrics ----
    result = pivot.merge(years_in_grade, on=["official_id", "full_name"], how="left")
    result = result.merge(recent_year, on=["official_id", "full_name"], how="left")

    # ---- Years since last ----
    result["years_since_last"] = current_year - result["most_recent_year"]
    result["never_used"] = result["most_recent_year"].isna()

    year_cols = sorted(
        [c for c in result.columns if isinstance(c, (int, float)) and not isinstance(c, bool)],
        reverse=True
    )

    if year_cols:
        ach = pd.to_numeric(result["achieved_year"], errors="coerce")
        sum_work = result[year_cols].fillna(0).sum(axis=1).astype(int)
        # Cannot have served at this role's nationals in the same calendar year as
        # appointment; exclude that matrix cell from attended-year count.
        appt_yr_worked = pd.Series(0, index=result.index, dtype=int)
        for yc in year_cols:
            m = ach == yc
            if m.any():
                appt_yr_worked.loc[m] = (
                    pd.to_numeric(result.loc[m, yc], errors="coerce")
                    .fillna(0)
                    .astype(int)
                )
        result["total_championships"] = (sum_work - appt_yr_worked).clip(lower=0)

        # Denominator: calendar years from the later of (earliest data year,
        # first year *after* appointment) through current year. Appointment year
        # itself is not eligible (could not attend that year's championship).
        data_year_min = int(min(year_cols))
        dmin_s = pd.Series(data_year_min, index=result.index, dtype="float64")
        first_eligible_year = ach + 1.0
        obs_start = pd.concat([dmin_s, first_eligible_year], axis=1).max(axis=1)
        eligible = (current_year - obs_start + 1).clip(lower=0)
        result["eligible_years"] = eligible.where(ach.notna())
    else:
        result["total_championships"] = 0
        result["eligible_years"] = float("nan")

    loc_meta = (
        qualified_df.sort_values("achieved_date", ascending=False, na_position="last")
        .drop_duplicates("official_id", keep="first")[["official_id", "region"]]
    )
    result = result.merge(loc_meta, on="official_id", how="left")

    fixed_cols = [
        "official_id",
        "full_name",
        "region",
        "achieved_year",  # calendar year of first national appt; used in app for champs cell shading
        "years_in_grade",
        "years_since_last",
        "most_recent_year",
        "never_used",
        "total_championships",
        "eligible_years",
    ]
    result = result[fixed_cols + year_cols]

    return result


def clean_activity_df(df):
    df = df.copy()

    new_cols = []

    for c in df.columns:
        try:
            # convert things like "2023", 2023.0 → 2023
            if str(c).replace(".0", "").isdigit():
                new_cols.append(int(float(c)))
            else:
                new_cols.append(c)
        except:
            new_cols.append(c)

    df.columns = new_cols

    # convert everything except year columns
    for col in df.columns:
        if col not in [
            "official_id",
            "full_name",
            "years_in_grade",
            "years_since_last",
            "most_recent_year",
        ]:
            df[col] = df[col].fillna(0)

    # ensure numeric columns are clean ints
    for col in df.columns:
        if isinstance(col, int):
            df[col] = df[col].astype(int)

    return df


def get_any_role_years(official_ids, competition_type_id, include_lower_levels=None):
    """
    Years each official attended the competition in ANY appointment type / discipline.
    Returns official_id, year, competition ids/types, and role name so callers can show
    which role(s) they had. Used to mark cells where the person was present in a different role (○).

    ``competition_type_id`` may be a single id or an iterable (e.g. all sectional types).
    """
    if not official_ids:
        return pd.DataFrame(
            columns=[
                "official_id",
                "year",
                "competition_id",
                "competition_type_id",
                "role",
                "discipline_id",
            ]
        )
    ct_ids = _normalize_competition_type_ids(competition_type_id)
    with Session(engine) as session:
        w = [
            Competition.competition_type_id.in_(ct_ids),
            Assignment.official_id.in_(official_ids),
        ]
        w.extend(_lower_levels_only_clauses(include_lower_levels))
        stmt = (
            select(
                Assignment.official_id,
                Competition.year,
                Competition.id.label("competition_id"),
                Competition.competition_type_id,
                AppointmentTypes.name.label("role"),
                Assignment.discipline_id,
            )
            .join(Competition, Assignment.competition_id == Competition.id)
            .join(AppointmentTypes, Assignment.appointment_type_id == AppointmentTypes.id)
            .where(*w)
            .distinct()
            .order_by(Competition.year.desc(), AppointmentTypes.name)
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(
        rows,
        columns=[
            "official_id",
            "year",
            "competition_id",
            "competition_type_id",
            "role",
            "discipline_id",
        ],
    )


def get_chief_years(
    official_ids,
    discipline_id,
    appointment_type_id,
    competition_type_id,
    include_lower_levels=None,
):
    """
    Years each official served as chief in the currently selected role/discipline
    for the selected competition type(s).
    Returns: official_id, year

    ``competition_type_id`` may be a single id or an iterable.
    """
    if not official_ids:
        return pd.DataFrame(columns=["official_id", "year"])

    discipline_ids = _resolve_discipline_ids(discipline_id, appointment_type_id)
    ct_ids = _normalize_competition_type_ids(competition_type_id)

    with Session(engine) as session:
        filters = [
            Assignment.appointment_type_id == appointment_type_id,
            Competition.competition_type_id.in_(ct_ids),
            Assignment.official_id.in_(official_ids),
            Assignment.chief.is_(True),
        ]
        if discipline_ids is not None:
            filters.append(Assignment.discipline_id.in_(discipline_ids))
        filters.extend(_lower_levels_only_clauses(include_lower_levels))

        stmt = (
            select(Assignment.official_id, Competition.year)
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(*filters)
            .distinct()
            .order_by(Competition.year.desc())
        )
        rows = session.execute(stmt).all()

    return pd.DataFrame(rows, columns=["official_id", "year"])


def appointment_type_has_chiefs(appointment_type_id):
    """Whether this appointment type uses chief assignments anywhere in history."""
    with Session(engine) as session:
        stmt = (
            select(Assignment.id)
            .where(
                Assignment.appointment_type_id == appointment_type_id,
                Assignment.chief.is_(True),
            )
            .limit(1)
        )
        return session.execute(stmt).first() is not None


def get_assigned_competition_counts(competition_type_ids):
    """
    Return officials with distinct competitions assigned in the given competition types.
    Output columns: official_id, full_name, competitions_assigned, most_recent_year
    """
    if not competition_type_ids:
        return pd.DataFrame(
            columns=[
                "official_id",
                "full_name",
                "region",
                "competitions_assigned",
                "most_recent_year",
            ]
        )

    with Session(engine) as session:
        stmt = (
            select(
                Officials.id.label("official_id"),
                Officials.full_name,
                func.max(Officials.region).label("region"),
                func.count(func.distinct(Competition.id)).label("competitions_assigned"),
                func.max(Competition.year).label("most_recent_year"),
            )
            .join(Assignment, Assignment.official_id == Officials.id)
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(Competition.competition_type_id.in_(competition_type_ids))
            .group_by(Officials.id, Officials.full_name)
            .order_by(
                func.count(func.distinct(Competition.id)).desc(),
                func.max(Competition.year).desc(),
                Officials.full_name.asc(),
            )
        )
        rows = session.execute(stmt).all()

    return pd.DataFrame(
        rows,
        columns=[
            "official_id",
            "full_name",
            "region",
            "competitions_assigned",
            "most_recent_year",
        ],
    )


def get_competition_count_for_types(competition_type_ids):
    """Return number of distinct competitions in the selected competition types."""
    if not competition_type_ids:
        return 0
    with Session(engine) as session:
        stmt = select(func.count(func.distinct(Competition.id))).where(
            Competition.competition_type_id.in_(competition_type_ids)
        )
        result = session.execute(stmt).scalar()
    return int(result or 0)


def get_referee_disciplines_for_competition_types(competition_type_ids):
    """Distinct disciplines that appear on referee assignments for the given competition types."""
    if not competition_type_ids:
        return pd.DataFrame(columns=["discipline_id", "discipline_name"])
    with Session(engine) as session:
        stmt = (
            select(Disciplines.id.label("discipline_id"), Disciplines.name.label("discipline_name"))
            .join(Assignment, Assignment.discipline_id == Disciplines.id)
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(
                Competition.competition_type_id.in_(competition_type_ids),
                Assignment.appointment_type_id == REFEREE_APPOINTMENT_TYPE_ID,
            )
            .distinct()
            .order_by(Disciplines.name)
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(rows, columns=["discipline_id", "discipline_name"])


def get_referee_competition_count_for_types(
    competition_type_ids, discipline_id=None, comp_group_name=None
):
    """
    Count distinct competitions in ``competition_type_ids`` that have at least one
    referee (appointment type) assignment.

    When ``discipline_id`` is set, only that assignment discipline counts.
    When unset and ``comp_group_name`` is set, assignment disciplines follow that
    group's rules (e.g. dance + singles/pairs for US Championships / SPD).
    """
    if not competition_type_ids:
        return 0
    filters = [
        Competition.competition_type_id.in_(competition_type_ids),
        Assignment.appointment_type_id == REFEREE_APPOINTMENT_TYPE_ID,
    ]
    if discipline_id is not None:
        filters.append(Assignment.discipline_id == discipline_id)
    elif comp_group_name:
        filters.append(
            Assignment.discipline_id.in_(
                _ref_assignment_discipline_ids(comp_group_name, None)
            )
        )
    with Session(engine) as session:
        stmt = (
            select(func.count(func.distinct(Competition.id)))
            .select_from(Competition)
            .join(Assignment, Assignment.competition_id == Competition.id)
            .where(*filters)
        )
        result = session.execute(stmt).scalar()
    return int(result or 0)


def get_referee_yearly_activity_report(
    competition_type_ids,
    discipline_id=None,
    window_years=10,
    comp_group_name=None,
):
    """
    Per-official referee report: everyone eligible for ``comp_group_name`` (see
    appointment rules in code), plus assignment stats at ``competition_type_ids``.

    - ``discipline_id`` None: overall (group-relevant disciplines combined).
    - ``window_years``: rolling calendar-year window ending in the current year (inclusive).

    Columns:
      official_id, full_name, discipline_label, years_served_csv (chief years as ``YYYY (C)``),
      total_distinct_years, chief_distinct_years, years_last_10, chief_years_last_10,
      years_in_grade, eligible_years_last_10, normalized_last_10
      (the ``*_last_10`` names are historical; values use ``window_years``.)

    For **SPD Sectionals** (overall, Dance + Singles/Pairs), ``years_in_grade`` and the
    normalized-window eligibility use whichever of Dance vs Singles/Pairs yields the
    **longer** tenure: per discipline, sectional referee ``achieved_date`` when present,
    otherwise national there; then the **earliest** of those two effective dates
    (same as max years in grade across the two).

    For **Synchronized Sectionals**, sectional-first then national fallback in
    synchronized only. Other groups use national only.
    """
    if not competition_type_ids or not comp_group_name:
        return pd.DataFrame()
    if comp_group_name not in REF_REPORT_COMP_GROUPS:
        raise ValueError(f"Unknown competition group: {comp_group_name!r}")

    window_years = int(window_years)
    if window_years < 1:
        window_years = 10

    eligible_ids = get_referee_eligible_official_ids(comp_group_name, discipline_id)
    if not eligible_ids:
        return pd.DataFrame()

    assign_disc_ids = _ref_assignment_discipline_ids(comp_group_name, discipline_id)

    with Session(engine) as session:
        filters = [
            Competition.competition_type_id.in_(competition_type_ids),
            Assignment.appointment_type_id == REFEREE_APPOINTMENT_TYPE_ID,
            Assignment.discipline_id.in_(assign_disc_ids),
            Officials.id.in_(eligible_ids),
        ]
        stmt = (
            select(
                Officials.id.label("official_id"),
                Officials.full_name,
                Competition.year,
                Assignment.chief,
                Assignment.discipline_id,
                Disciplines.name.label("discipline_name"),
            )
            .join(Assignment, Assignment.official_id == Officials.id)
            .join(Competition, Assignment.competition_id == Competition.id)
            .join(Disciplines, Assignment.discipline_id == Disciplines.id)
            .where(*filters)
        )
        rows = session.execute(stmt).all()

        name_rows = session.execute(
            select(Officials.id, Officials.full_name).where(
                Officials.id.in_(eligible_ids)
            )
        ).all()
        name_map = {int(r[0]): r[1] for r in name_rows}

        single_disc_name = None
        if discipline_id is not None:
            single_disc_name = session.execute(
                select(Disciplines.name).where(Disciplines.id == discipline_id)
            ).scalar()

        ach_filters_base = [
            Appointments.official_id.in_(eligible_ids),
            Appointments.appointment_type_id == REFEREE_APPOINTMENT_TYPE_ID,
            Appointments.discipline_id.in_(assign_disc_ids),
        ]
        sectional_report = comp_group_name in (
            "SPD Sectionals",
            "Synchronized Sectionals",
        )
        spd_overall_tig = (
            comp_group_name == "SPD Sectionals" and discipline_id is None
        )
        sec_rows_4 = nat_rows_4 = sec_rows_9 = nat_rows_9 = None
        if sectional_report and spd_overall_tig:

            def _spd_sec_nat_rows(disc_id):
                bf = [
                    Appointments.official_id.in_(eligible_ids),
                    Appointments.appointment_type_id == REFEREE_APPOINTMENT_TYPE_ID,
                    Appointments.discipline_id == disc_id,
                ]
                sstmt = (
                    select(
                        Appointments.official_id,
                        func.min(Appointments.achieved_date).label("first_achieved"),
                    )
                    .where(*bf, Appointments.level_id == SECTIONAL_LEVEL_ID)
                    .group_by(Appointments.official_id)
                )
                nstmt = (
                    select(
                        Appointments.official_id,
                        func.min(Appointments.achieved_date).label("first_achieved"),
                    )
                    .where(*bf, Appointments.level_id == NATIONAL_LEVEL_ID)
                    .group_by(Appointments.official_id)
                )
                return (
                    session.execute(sstmt).all(),
                    session.execute(nstmt).all(),
                )

            sec_rows_4, nat_rows_4 = _spd_sec_nat_rows(DISC_DANCE_ID)
            sec_rows_9, nat_rows_9 = _spd_sec_nat_rows(DISC_SINGLES_PAIRS_ID)
            sec_rows = nat_rows = ach_rows = None
        elif sectional_report:
            sec_stmt = (
                select(
                    Appointments.official_id,
                    func.min(Appointments.achieved_date).label("first_achieved"),
                )
                .where(*ach_filters_base, Appointments.level_id == SECTIONAL_LEVEL_ID)
                .group_by(Appointments.official_id)
            )
            nat_stmt = (
                select(
                    Appointments.official_id,
                    func.min(Appointments.achieved_date).label("first_achieved"),
                )
                .where(*ach_filters_base, Appointments.level_id == NATIONAL_LEVEL_ID)
                .group_by(Appointments.official_id)
            )
            sec_rows = session.execute(sec_stmt).all()
            nat_rows = session.execute(nat_stmt).all()
            ach_rows = None
        else:
            ach_stmt = (
                select(
                    Appointments.official_id,
                    func.min(Appointments.achieved_date).label("first_achieved"),
                )
                .where(*ach_filters_base, Appointments.level_id == NATIONAL_LEVEL_ID)
                .group_by(Appointments.official_id)
            )
            ach_rows = session.execute(ach_stmt).all()
            sec_rows = nat_rows = None

    df = pd.DataFrame(
        rows,
        columns=[
            "official_id",
            "full_name",
            "year",
            "chief",
            "discipline_id",
            "discipline_name",
        ],
    )
    if not df.empty:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df["chief"] = df["chief"].fillna(False).astype(bool)

    cy = datetime.now().year
    win_start = cy - window_years + 1

    def _eligible_years_in_window(achieved_year):
        if achieved_year is None or pd.isna(achieved_year):
            return max(1, cy - win_start + 1)
        ay = int(achieved_year)
        first_eligible = max(win_start, ay)
        if first_eligible > cy:
            return 1
        return cy - first_eligible + 1

    def _achieved_year_lookup(rows):
        if not rows:
            return {}
        adf = pd.DataFrame(rows, columns=["official_id", "first_achieved"])
        adf["achieved_year"] = pd.to_datetime(
            adf["first_achieved"], errors="coerce"
        ).dt.year
        out = {}
        for _, r in adf.iterrows():
            y = r["achieved_year"]
            if pd.notna(y):
                out[int(r["official_id"])] = int(y)
        return out

    if sectional_report and spd_overall_tig:
        sec_map_4 = _achieved_year_lookup(sec_rows_4)
        nat_map_4 = _achieved_year_lookup(nat_rows_4)
        sec_map_9 = _achieved_year_lookup(sec_rows_9)
        nat_map_9 = _achieved_year_lookup(nat_rows_9)
        sec_ach_map = nat_ach_map = None
        ach_df = None
    elif sectional_report:
        sec_ach_map = _achieved_year_lookup(sec_rows)
        nat_ach_map = _achieved_year_lookup(nat_rows)
        sec_map_4 = nat_map_4 = sec_map_9 = nat_map_9 = None
        ach_df = None
    else:
        ach_df = pd.DataFrame(ach_rows, columns=["official_id", "first_achieved"])
        ach_df["achieved_year"] = pd.to_datetime(
            ach_df["first_achieved"], errors="coerce"
        ).dt.year
        sec_ach_map = nat_ach_map = None
        sec_map_4 = nat_map_4 = sec_map_9 = nat_map_9 = None

    by_off = {}
    if not df.empty:
        for oid, g in df.groupby("official_id"):
            by_off[int(oid)] = g

    disc_label = "Overall" if discipline_id is None else (single_disc_name or "")

    records = []
    for oid in eligible_ids:
        g = by_off.get(oid)
        name = name_map.get(oid, "")
        if g is not None:
            years = sorted(int(y) for y in g["year"].dropna().unique().tolist())
            chief_years = sorted(
                int(y) for y in g.loc[g["chief"], "year"].dropna().unique().tolist()
            )
            row_disc_label = (
                disc_label
                if discipline_id is None
                else str(g["discipline_name"].iloc[0])
            )
        else:
            years = []
            chief_years = []
            row_disc_label = disc_label
        chief_year_set = set(chief_years)
        years_csv = ", ".join(
            f"{y} (C)" if y in chief_year_set else str(y) for y in years
        )
        total_y = len(years)
        chief_distinct = len(chief_year_set)
        y10 = [y for y in years if win_start <= y <= cy]
        chief_10 = len(set(chief_years) & set(y10))
        if sectional_report and spd_overall_tig:

            def _eff_disc(sec_m, nat_m, o):
                ys = sec_m.get(o)
                yn = nat_m.get(o)
                if ys is not None:
                    return ys
                if yn is not None:
                    return yn
                return None

            eff4 = _eff_disc(sec_map_4, nat_map_4, oid)
            eff9 = _eff_disc(sec_map_9, nat_map_9, oid)
            cands = [y for y in (eff4, eff9) if y is not None]
            achieved_year = min(cands) if cands else None
        elif sectional_report:
            ay_s = sec_ach_map.get(oid)
            ay_n = nat_ach_map.get(oid)
            if ay_s is not None:
                achieved_year = ay_s
            elif ay_n is not None:
                achieved_year = ay_n
            else:
                achieved_year = None
        else:
            arow = ach_df.loc[ach_df["official_id"] == oid]
            achieved_year = (
                arow["achieved_year"].iloc[0] if len(arow) else None
            )
        yig = (
            int(cy - achieved_year)
            if achieved_year is not None and not pd.isna(achieved_year)
            else None
        )
        elig = _eligible_years_in_window(achieved_year)
        norm = (len(y10) / max(1, elig)) if elig else 0.0
        records.append(
            {
                "official_id": oid,
                "full_name": name,
                "discipline_label": row_disc_label,
                "years_served_csv": years_csv,
                "total_distinct_years": total_y,
                "chief_distinct_years": chief_distinct,
                "years_last_10": len(y10),
                "chief_years_last_10": chief_10,
                "years_in_grade": yig,
                "eligible_years_last_10": elig,
                "normalized_last_10": round(norm, 3),
            }
        )

    out = pd.DataFrame(records)
    out = out.sort_values(
        by=["total_distinct_years", "years_last_10", "full_name"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    return out


def get_officials_with_assignments():
    """Officials who have at least one assignment (id + display name)."""
    with Session(engine) as session:
        stmt = (
            select(Officials.id, Officials.full_name)
            .join(Assignment, Assignment.official_id == Officials.id)
            .distinct()
            .order_by(Officials.full_name.asc())
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(rows, columns=["official_id", "full_name"])


def get_official_assignment_detail_rows(official_id: int):
    """
    One row per assignment for ``official_id`` with fields needed for display/sort.
    Columns: competition_id, year, competition_name, competition_type_id,
    appt_type_name, discipline_id, discipline_name, chief, lower_levels_only
    """
    with Session(engine) as session:
        stmt = (
            select(
                Competition.id.label("competition_id"),
                Competition.year,
                Competition.name.label("competition_name"),
                Competition.competition_type_id,
                AppointmentTypes.name.label("appt_type_name"),
                Assignment.discipline_id,
                Disciplines.name.label("discipline_name"),
                Assignment.chief,
                Assignment.lower_levels_only,
            )
            .join(Competition, Assignment.competition_id == Competition.id)
            .join(AppointmentTypes, Assignment.appointment_type_id == AppointmentTypes.id)
            .join(Disciplines, Assignment.discipline_id == Disciplines.id)
            .where(Assignment.official_id == int(official_id))
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(
        rows,
        columns=[
            "competition_id",
            "year",
            "competition_name",
            "competition_type_id",
            "appt_type_name",
            "discipline_id",
            "discipline_name",
            "chief",
            "lower_levels_only",
        ],
    )


def get_official_appointment_rows(official_id: int):
    """All appointments for directory display (type, discipline, level, achieved).

    Sorted by appointment type name (A–Z), then achieved date (newest first, blanks last).
    """
    with Session(engine) as session:
        stmt = (
            select(
                AppointmentTypes.name.label("appointment_type"),
                Disciplines.name.label("discipline"),
                Levels.name.label("level"),
                Appointments.achieved_date,
            )
            .join(AppointmentTypes, Appointments.appointment_type_id == AppointmentTypes.id)
            .outerjoin(Disciplines, Appointments.discipline_id == Disciplines.id)
            .outerjoin(Levels, Appointments.level_id == Levels.id)
            .where(Appointments.official_id == int(official_id))
            .order_by(
                AppointmentTypes.name.asc(),
                Appointments.achieved_date.desc().nulls_last(),
            )
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(
        rows,
        columns=["appointment_type", "discipline", "level", "achieved_date"],
    )


def get_official_segment_official_activity_detail(official_id: int) -> pd.DataFrame:
    """
    One row per ``segment_official`` entry for this directory ``official_id``,
    with competition and segment fields. Rows are ordered by competition (newest first)
    then segment name and role.

    Returns empty when ``DATABASE_URL`` is not PostgreSQL or the judging tables are
    unavailable.
    """
    cols = [
        "competition_id",
        "year",
        "competition_name",
        "results_url",
        "start_date",
        "end_date",
        "qualifying",
        "segment_id",
        "segment_name",
        "discipline",
        "role",
    ]
    database_url = _resolve_database_url()
    if not database_url.startswith("postgresql"):
        return pd.DataFrame(columns=cols)
    stmt = text(
        """
        SELECT
            c.id AS competition_id,
            c.year,
            c.name AS competition_name,
            c.results_url,
            c.start_date,
            c.end_date,
            COALESCE(c.qualifying, false) AS qualifying,
            s.id AS segment_id,
            s.name AS segment_name,
            dt.name AS discipline,
            so.role
        FROM public.segment_official so
        INNER JOIN public.segment s ON s.id = so.segment_id
        INNER JOIN public.competition c ON c.id = s.competition_id
        LEFT JOIN public.discipline_type dt ON dt.id = s.discipline_type_id
        WHERE so.official_id = :oid
        ORDER BY
            COALESCE(c.end_date, c.start_date) DESC NULLS LAST,
            CASE
                WHEN c.year ~ '^[0-9]+$' THEN c.year::integer
                ELSE 0
            END DESC,
            c.name ASC,
            s.name ASC,
            so.role ASC
        """
    )
    try:
        with Session(engine) as session:
            rows = session.execute(stmt, {"oid": int(official_id)}).mappings().all()
    except Exception:
        return pd.DataFrame(columns=cols)
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows)


def get_official_segment_official_competitions(official_id: int) -> pd.DataFrame:
    """
    Unique competitions for this official in ``segment_official`` (summary only).
    Prefer :func:`get_official_segment_official_activity_detail` when segment rows are needed.
    """
    detail = get_official_segment_official_activity_detail(official_id)
    if detail.empty:
        return pd.DataFrame(
            columns=[
                "competition_id",
                "year",
                "competition_name",
                "results_url",
                "start_date",
                "end_date",
                "panel_segment_count",
                "discipline",
            ]
        )
    def _disciplines_summary(series: pd.Series) -> str:
        parts = sorted(
            {str(x).strip() for x in series.dropna() if str(x).strip()},
            key=str.lower,
        )
        return ", ".join(parts)

    summary = (
        detail.groupby("competition_id", sort=False)
        .agg(
            year=("year", "first"),
            competition_name=("competition_name", "first"),
            results_url=("results_url", "first"),
            start_date=("start_date", "first"),
            end_date=("end_date", "first"),
            panel_segment_count=("segment_id", "nunique"),
            discipline=("discipline", _disciplines_summary),
        )
        .reset_index()
    )
    return summary


def _competition_type_group_order_expr():
    """Sort key: US Synchro Champs, US Champs, SYS sectionals, SPD sectionals, then other."""
    return case(
        (Competition.competition_type_id == 8, 0),
        (Competition.competition_type_id == 4, 1),
        (Competition.competition_type_id.in_((5, 6, 7, 9)), 2),
        (Competition.competition_type_id.in_((1, 2, 3)), 3),
        else_=99,
    )


def get_competitions_for_report_dropdown():
    """All competitions for per-competition report select (newest / priority order)."""
    ord_type = _competition_type_group_order_expr()
    with Session(engine) as session:
        stmt = (
            select(
                Competition.id,
                Competition.year,
                Competition.name,
                Competition.competition_type_id,
            )
            .order_by(
                Competition.year.desc(),
                ord_type,
                Competition.competition_type_id.asc(),
                Competition.name.asc(),
            )
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(
        rows,
        columns=[
            "competition_id",
            "year",
            "name",
            "competition_type_id",
        ],
    )


def get_competition_assignment_rows(competition_id: int):
    """
    All assignments at ``competition_id`` with fields for per-competition assignment display.
    Columns: full_name, appt_type_name, discipline_id, discipline_name, chief,
    lower_levels_only
    """
    with Session(engine) as session:
        stmt = (
            select(
                Officials.full_name,
                AppointmentTypes.name.label("appt_type_name"),
                Assignment.discipline_id,
                Disciplines.name.label("discipline_name"),
                Assignment.chief,
                Assignment.lower_levels_only,
            )
            .join(Officials, Assignment.official_id == Officials.id)
            .join(AppointmentTypes, Assignment.appointment_type_id == AppointmentTypes.id)
            .join(Disciplines, Assignment.discipline_id == Disciplines.id)
            .where(Assignment.competition_id == int(competition_id))
            .order_by(Officials.full_name.asc())
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(
        rows,
        columns=[
            "full_name",
            "appt_type_name",
            "discipline_id",
            "discipline_name",
            "chief",
            "lower_levels_only",
        ],
    )


def _parse_chiefed_years(value):
    if pd.isna(value):
        return []
    text = str(value).strip()
    if not text or text.upper() == "X":
        return []
    years = []
    for part in re.split(r"[,\s;/]+", text):
        part = part.strip()
        if not part:
            continue
        if part.isdigit() and len(part) == 4:
            years.append(int(part))
    return sorted(set(years))


def load_chief_scoring_officials_us_champs(
    file_path,
    sheet_name="National Accountants",
    write_to_database=False,
    create_missing_competitions=True,
    create_missing_assignments=True,
):
    """
    Load chief scoring officials for US Championships (competition_type_id=4)
    from an Excel sheet with columns including:
      - First Name
      - Last Name
      - Chiefed US Champs

    Behavior:
      - For each (official, year), set chief=True on existing assignments for
        appointment type "Scoring Official" at US Championships competitions.
      - If no matching assignment exists, record it in 'missing_assignment'
        for manual follow-up.
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    required_cols = {"First Name", "Last Name", "Chiefed US Champs"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in Excel: {sorted(missing_cols)}")

    stats = {
        "rows_read": len(df),
        "years_listed": 0,
        "updated_to_chief": 0,
        "already_chief": 0,
        "missing_official": 0,
        "missing_competition": 0,
        "created_competition": 0,
        "missing_assignment": 0,
        "created_assignment": 0,
    }
    missing_details = []

    with Session(engine) as session:
        scoring_official_appt = (
            session.query(AppointmentTypes)
            .filter(AppointmentTypes.name == "Scoring Official")
            .one_or_none()
        )
        if scoring_official_appt is None:
            raise ValueError("Could not find appointment type 'Scoring Official'.")

        officials_by_name = {
            (o.full_name or "").strip().lower(): o.id
            for o in session.query(Officials).all()
            if o.full_name
        }

        competitions = session.query(Competition).filter(
            Competition.competition_type_id == US_CHAMPIONSHIPS_COMPETITION_TYPE_ID
        ).all()
        comp_by_year = {}
        for comp in competitions:
            comp_by_year.setdefault(comp.year, []).append(comp)

        for _, row in df.iterrows():
            first = "" if pd.isna(row["First Name"]) else str(row["First Name"]).strip()
            last = "" if pd.isna(row["Last Name"]) else str(row["Last Name"]).strip()
            full_name = f"{first} {last}".strip()
            if not full_name:
                continue

            years = _parse_chiefed_years(row["Chiefed US Champs"])
            stats["years_listed"] += len(years)

            # Ensure listed US Championships years exist, even when the name
            # does not match an official record.
            if create_missing_competitions:
                for yr in years:
                    if not comp_by_year.get(yr):
                        comp = get_or_create_competition(
                            session,
                            yr,
                            "US Championships",
                            US_CHAMPIONSHIPS_COMPETITION_TYPE_ID,
                        )
                        stats["created_competition"] += 1
                        comp_by_year[yr] = [comp]

            official_id = officials_by_name.get(full_name.lower())

            if official_id is None:
                if years:
                    stats["missing_official"] += len(years)
                    for yr in years:
                        missing_details.append(
                            {"name": full_name, "year": yr, "reason": "missing_official"}
                        )
                continue

            for yr in years:
                comps = comp_by_year.get(yr, [])
                if not comps:
                    if create_missing_competitions:
                        comp = get_or_create_competition(
                            session,
                            yr,
                            "US Championships",
                            US_CHAMPIONSHIPS_COMPETITION_TYPE_ID,
                        )
                        stats["created_competition"] += 1
                        comps = [comp]
                        comp_by_year[yr] = comps
                    else:
                        stats["missing_competition"] += 1
                        missing_details.append(
                            {"name": full_name, "year": yr, "reason": "missing_competition"}
                        )
                        continue

                found_assignment = False
                for comp in comps:
                    assignments = (
                        session.query(Assignment)
                        .filter(
                            Assignment.competition_id == comp.id,
                            Assignment.official_id == official_id,
                            Assignment.appointment_type_id == scoring_official_appt.id,
                        )
                        .all()
                    )
                    if not assignments:
                        if create_missing_assignments:
                            # Chief accountants are loaded as Scoring Official
                            # assignments. Discipline is required in schema; use
                            # discipline id 7 per data model convention.
                            new_assignment = Assignment(
                                competition_id=comp.id,
                                official_id=official_id,
                                discipline_id=7,
                                appointment_type_id=scoring_official_appt.id,
                                chief=True,
                                lower_levels_only=False,
                            )
                            session.add(new_assignment)
                            stats["created_assignment"] += 1
                            found_assignment = True
                        continue
                    found_assignment = True
                    for assignment in assignments:
                        if assignment.chief:
                            stats["already_chief"] += 1
                        else:
                            assignment.chief = True
                            stats["updated_to_chief"] += 1

                if not found_assignment:
                    stats["missing_assignment"] += 1
                    missing_details.append(
                        {"name": full_name, "year": yr, "reason": "missing_assignment"}
                    )

        if write_to_database:
            session.commit()
        else:
            session.rollback()

    return {"stats": stats, "missing_details": pd.DataFrame(missing_details)}


# US Championships referees (US_Champs_Referees.xlsx)
# Referee appointment type id: REFEREE_APPOINTMENT_TYPE_ID (module-level).
US_CHAMPS_REFEREE_DISCIPLINE_DEFAULT = 9
US_CHAMPS_REFEREE_DISCIPLINE_DANCE = 4


def _parse_us_champs_referee_role(role_value):
    """
    Map Excel Role to (chief, discipline_id) for US Championships referee panel.
    Appointment type is always Referee (REFEREE_APPOINTMENT_TYPE_ID).
    """
    if pd.isna(role_value):
        return None
    r = " ".join(str(role_value).strip().lower().split())
    if r == "assistant referee dance":
        return False, US_CHAMPS_REFEREE_DISCIPLINE_DANCE
    if r == "assistant referee":
        return False, US_CHAMPS_REFEREE_DISCIPLINE_DEFAULT
    if r == "chief referee":
        return True, US_CHAMPS_REFEREE_DISCIPLINE_DEFAULT
    return None


def load_us_champs_referees_assignments(
    file_path="activityAnalysis/US_Champs_Referees.xlsx",
    sheet_name=0,
    write_to_database=False,
    create_missing_competitions=True,
):
    """
    Load US Championships referee assignments from Excel (Year, Role, Name).

    Competitions are resolved by ``competition_type_id ==
    US_CHAMPIONSHIPS_COMPETITION_TYPE_ID`` (4). New competitions created for a
    missing year use that same type.

    Roles (case-insensitive, normalized whitespace):
      - Chief Referee: chief=True, discipline_id=9, appointment_type_id=4 (Referee)
      - Assistant Referee: chief=False, discipline_id=9, appointment_type_id=4
      - Assistant Referee Dance: chief=False, discipline_id=4, appointment_type_id=4

    Skips rows that already have the same (competition, official, discipline, type).
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    required = {"Year", "Role", "Name"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {sorted(missing_cols)}")

    stats = {
        "rows_read": len(df),
        "inserted": 0,
        "skipped_duplicate": 0,
        "skipped_retired": 0,
        "empty_name": 0,
        "missing_official": 0,
        "unknown_role": 0,
        "bad_year": 0,
        "created_competition": 0,
        "missing_competition": 0,
    }
    missing_details = []

    retired_path = "activityAnalysis/Retired_officials.xlsx"
    try:
        retired_df = pd.read_excel(retired_path)
        retired_names = {
            str(n).strip().lower() for n in retired_df["Name"] if pd.notna(n)
        }
    except Exception:
        retired_names = set()

    with Session(engine) as session:
        referee_row = (
            session.query(AppointmentTypes)
            .filter(AppointmentTypes.id == REFEREE_APPOINTMENT_TYPE_ID)
            .one_or_none()
        )
        if referee_row is None:
            raise ValueError(
                f"Appointment type id {REFEREE_APPOINTMENT_TYPE_ID} not found."
            )

        officials_by_name = {
            (o.full_name or "").strip().lower(): o.id
            for o in session.query(Officials).all()
            if o.full_name
        }

        competitions = (
            session.query(Competition)
            .filter(
                Competition.competition_type_id == US_CHAMPIONSHIPS_COMPETITION_TYPE_ID
            )
            .all()
        )
        comp_by_year = {}
        for comp in competitions:
            if comp.competition_type_id != US_CHAMPIONSHIPS_COMPETITION_TYPE_ID:
                continue
            comp_by_year.setdefault(comp.year, []).append(comp)

        for _, row in df.iterrows():
            name = "" if pd.isna(row["Name"]) else str(row["Name"]).strip()
            if not name:
                stats["empty_name"] += 1
                missing_details.append(
                    {"name": "", "year": None, "reason": "empty_name", "role": row.get("Role")}
                )
                continue

            if name.lower() in retired_names:
                stats["skipped_retired"] += 1
                continue

            parsed = _parse_us_champs_referee_role(row.get("Role"))
            if parsed is None:
                stats["unknown_role"] += 1
                missing_details.append(
                    {
                        "name": name,
                        "year": row.get("Year"),
                        "reason": "unknown_role",
                        "role": row.get("Role"),
                    }
                )
                continue

            chief, discipline_id = parsed

            if pd.isna(row["Year"]):
                stats["bad_year"] += 1
                missing_details.append(
                    {"name": name, "year": None, "reason": "bad_year", "role": row.get("Role")}
                )
                continue
            try:
                year = int(float(row["Year"]))
            except (TypeError, ValueError):
                stats["bad_year"] += 1
                missing_details.append(
                    {"name": name, "year": row.get("Year"), "reason": "bad_year", "role": row.get("Role")}
                )
                continue

            if create_missing_competitions and not comp_by_year.get(year):
                comp = get_or_create_competition(
                    session,
                    year,
                    "US Championships",
                    US_CHAMPIONSHIPS_COMPETITION_TYPE_ID,
                )
                stats["created_competition"] += 1
                comp_by_year[year] = [comp]

            comps = [
                c
                for c in comp_by_year.get(year, [])
                if c.competition_type_id == US_CHAMPIONSHIPS_COMPETITION_TYPE_ID
            ]
            if not comps:
                stats["missing_competition"] += 1
                missing_details.append(
                    {"name": name, "year": year, "reason": "missing_competition", "role": row.get("Role")}
                )
                continue

            us_named = [
                c
                for c in comps
                if (c.name or "").strip() == "US Championships"
            ]
            comp = us_named[0] if us_named else comps[0]

            official_id = officials_by_name.get(name.lower())
            if official_id is None:
                stats["missing_official"] += 1
                missing_details.append(
                    {"name": name, "year": year, "reason": "missing_official", "role": row.get("Role")}
                )
                continue

            exists = (
                session.query(Assignment)
                .filter_by(
                    competition_id=comp.id,
                    official_id=official_id,
                    discipline_id=discipline_id,
                    appointment_type_id=REFEREE_APPOINTMENT_TYPE_ID,
                )
                .first()
            )
            if exists:
                stats["skipped_duplicate"] += 1
                continue

            session.add(
                Assignment(
                    competition_id=comp.id,
                    official_id=official_id,
                    discipline_id=discipline_id,
                    appointment_type_id=REFEREE_APPOINTMENT_TYPE_ID,
                    chief=chief,
                    lower_levels_only=False,
                )
            )
            stats["inserted"] += 1

        if write_to_database:
            session.commit()
        else:
            session.rollback()

    return {"stats": stats, "missing_details": pd.DataFrame(missing_details)}


def get_sectional_in_role_distinct_competition_counts(
    discipline_id, appointment_type_id, official_ids, sectional_competition_type_ids
):
    """
    Per official: sum over calendar years of (distinct ``competition_id`` count) for the
    selected appointment type at sectional activity types. Used as "# In Role" for the
    sectionals matrix when multiple competitions occur in the same year.
    """
    if not official_ids:
        return pd.DataFrame(columns=["official_id", "total_championships"])
    ct_ids = list(sectional_competition_type_ids)
    discipline_ids = _resolve_discipline_ids(discipline_id, appointment_type_id)
    with Session(engine) as session:
        filters = [
            Assignment.official_id.in_(list(official_ids)),
            Assignment.appointment_type_id == appointment_type_id,
            Competition.competition_type_id.in_(ct_ids),
        ]
        if discipline_ids is not None:
            filters.append(Assignment.discipline_id.in_(discipline_ids))
        stmt = (
            select(
                Assignment.official_id,
                Competition.year,
                Competition.id.label("competition_id"),
            )
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(*filters)
            .distinct()
        )
        rows = session.execute(stmt).all()
    df = pd.DataFrame(rows, columns=["official_id", "year", "competition_id"])
    base = pd.DataFrame({"official_id": official_ids}).drop_duplicates()
    if df.empty:
        base["total_championships"] = 0
        return base.astype({"total_championships": int})
    per_year = (
        df.groupby(["official_id", "year"])["competition_id"]
        .nunique()
        .reset_index(name="_n")
    )
    summed = (
        per_year.groupby("official_id")["_n"]
        .sum()
        .reset_index(name="total_championships")
    )
    return base.merge(summed, on="official_id", how="left").fillna(
        {"total_championships": 0}
    ).astype({"total_championships": int})


def get_activity_matrix(
    discipline_id,
    appointment_type_id,
    level_ids,
    competition_type_id,
    include_lower_levels=None,
    active_appointments_only: bool = True,
):
    qualified_df = get_qualified_officials(
        discipline_id,
        appointment_type_id,
        level_ids,
        active_appointments_only=active_appointments_only,
    )

    activity_df = get_assignment_years(
        discipline_id,
        appointment_type_id,
        competition_type_id,
        include_lower_levels=include_lower_levels,
    )

    result = build_activity_matrix(qualified_df, activity_df)
    # result = clean_activity_df(result)
    return result


def get_activity_matrix_sectionals(
    discipline_id,
    appointment_type_id,
    sectional_competition_type_ids=SECTIONAL_ACTIVITY_COMPETITION_TYPES,
    active_appointments_only: bool = True,
):
    """
    Activity matrix for **sectionals**: ``sectional_competition_type_ids`` defaults to
    all SPD and synchro sectionals, or a subset (SPD-only or synchro-only).

    Qualified officials have a **sectional or national** appointment in the selected
    role and discipline. Sectional level is usually ``SECTIONAL_LEVEL_ID`` (2); Scoring
    Official and Scoring System Technician use ``SCORING_SECTIONAL_LEVEL2_ID`` (8) for sectional.
    """
    ct_ids = list(sectional_competition_type_ids)
    level_ids = sectional_qualified_level_ids(appointment_type_id)
    qualified_df = get_qualified_officials(
        discipline_id,
        appointment_type_id,
        level_ids,
        include_appointment_level=True,
        active_appointments_only=active_appointments_only,
    )
    qualified_df = qualified_df.sort_values(
        "achieved_date", ascending=True, na_position="last"
    ).drop_duplicates(subset=["official_id"], keep="first")
    _level_names = _level_id_to_name_map(qualified_df["level_id"].tolist())
    appointment_level_by_official = qualified_df[
        ["official_id", "level_id"]
    ].copy()
    appointment_level_by_official["appointment_level"] = appointment_level_by_official[
        "level_id"
    ].map(
        lambda v: _level_names.get(int(v), "")
        if v is not None and not (isinstance(v, float) and pd.isna(v))
        else ""
    )
    appointment_level_by_official = appointment_level_by_official[
        ["official_id", "appointment_level"]
    ]
    qualified_df = qualified_df.drop(columns=["level_id"], errors="ignore")
    activity_df = get_assignment_years(
        discipline_id,
        appointment_type_id,
        ct_ids,
    )
    result = build_activity_matrix(qualified_df, activity_df)
    result = result.merge(appointment_level_by_official, on="official_id", how="left")
    extra = get_sectional_in_role_distinct_competition_counts(
        discipline_id,
        appointment_type_id,
        result["official_id"].astype(int).tolist(),
        tuple(ct_ids),
    )
    result = result.drop(columns=["total_championships"], errors="ignore").merge(
        extra, on="official_id", how="left"
    )
    result["total_championships"] = (
        pd.to_numeric(result["total_championships"], errors="coerce").fillna(0).astype(int)
    )
    return result


def get_sectional_assignment_region_rows(
    discipline_id, appointment_type_id, official_ids, sectional_competition_type_ids=SECTIONAL_ACTIVITY_COMPETITION_TYPES
):
    """
    Selected-role assignments at sectional activity types for **E / M / P** suffixes
    and same-bucket "other role" filtering. Columns: official_id, year, competition_type_id.
    """
    if not official_ids:
        return pd.DataFrame(columns=["official_id", "year", "competition_type_id"])
    ct_ids = list(sectional_competition_type_ids)
    discipline_ids = _resolve_discipline_ids(discipline_id, appointment_type_id)
    with Session(engine) as session:
        filters = [
            Assignment.official_id.in_(list(official_ids)),
            Assignment.appointment_type_id == appointment_type_id,
            Competition.competition_type_id.in_(ct_ids),
        ]
        if discipline_ids is not None:
            filters.append(Assignment.discipline_id.in_(discipline_ids))
        stmt = (
            select(
                Assignment.official_id,
                Competition.year,
                Competition.competition_type_id,
            )
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(*filters)
            .distinct()
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(
        rows, columns=["official_id", "year", "competition_type_id"]
    )


def _nqs_eligible_directory_level_ids() -> tuple[int, ...]:
    """Qualifying (9), sectional (2), and national (7) USFS directory levels."""
    return (
        NQS_DIRECTORY_QUALIFYING_LEVEL_ID,
        SECTIONAL_LEVEL_ID,
        NATIONAL_LEVEL_ID,
    )


# NQS report UI: multiselect labels → ``officials_analysis.appointments.level_id``.
NQS_REPORT_LEVEL_FILTER_BY_LABEL: dict[str, int] = {
    "Qualifying": NQS_DIRECTORY_QUALIFYING_LEVEL_ID,
    "Sectional": SECTIONAL_LEVEL_ID,
    "National": NATIONAL_LEVEL_ID,
}


def _nqs_directory_appointment_type_name(official_type_label: str) -> str:
    m = {
        "Judge": "Competition Judge",
        "Referee": "Referee",
        "Technical Controller": "Technical Controller",
        "Technical Specialist": "Technical Specialist",
    }
    if official_type_label not in m:
        raise ValueError(f"Unknown NQS official type: {official_type_label!r}")
    return m[official_type_label]


def _nqs_panel_role_kind(official_type_label: str) -> str:
    m = {
        "Judge": "judge",
        "Referee": "referee",
        "Technical Controller": "technical_controller",
        "Technical Specialist": "technical_specialist",
    }
    return m[official_type_label]


def _nqs_resolve_directory_and_segment_disciplines(
    official_type_label: str, discipline_label: str
) -> tuple[int, tuple[int, ...]]:
    """
    Directory discipline id for ``get_qualified_officials`` and IJS ``segment.discipline_type_id``
    values to match (Singles=1, Pairs=2, Ice Dance=3).
    """
    if official_type_label in ("Judge", "Referee"):
        if discipline_label == "Singles/Pairs":
            return DISC_SINGLES_PAIRS_ID, (
                NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES,
                NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,
            )
        if discipline_label == "Dance":
            return DISC_DANCE_ID, (NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,)
        raise ValueError(
            f"Discipline {discipline_label!r} must be Singles/Pairs or Dance for Judge/Referee."
        )
    if official_type_label in ("Technical Controller", "Technical Specialist"):
        if discipline_label == "Singles":
            return SINGLES_DISCIPLINE_ID, (NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES,)
        if discipline_label == "Pairs":
            return DISC_PAIRS_ID, (NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,)
        if discipline_label == "Dance":
            return DISC_DANCE_ID, (NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,)
        raise ValueError(
            f"Discipline {discipline_label!r} must be Singles, Pairs, or Dance for TC/TS."
        )
    raise ValueError(f"Unknown official type: {official_type_label!r}")


def _nqs_panel_role_sql_predicate(panel_role_kind: str) -> str:
    """SQL boolean expression on ``so.role``; values are fixed internally (not user input)."""
    if panel_role_kind == "judge":
        return "LOWER(BTRIM(so.role)) LIKE 'judge%'"
    if panel_role_kind == "referee":
        return "LOWER(so.role) LIKE '%referee%'"
    if panel_role_kind == "technical_controller":
        return "LOWER(so.role) LIKE '%technical controller%'"
    if panel_role_kind == "technical_specialist":
        return "LOWER(so.role) LIKE '%technical specialist%'"
    raise ValueError(f"Unknown panel role kind: {panel_role_kind!r}")


def _get_appointment_type_id_by_name(name: str) -> int | None:
    with Session(engine) as session:
        row = session.execute(
            select(AppointmentTypes.id).where(AppointmentTypes.name == name).limit(1)
        ).first()
    return int(row[0]) if row else None


def _query_nqs_competition_counts_by_year(
    official_ids: list[int],
    panel_role_kind: str,
    segment_discipline_type_ids: tuple[int, ...],
) -> pd.DataFrame:
    """
    Rows: official_id, season_year, nqs_competitions (distinct ``public.competition`` per year).
    """
    if not official_ids:
        return pd.DataFrame(
            columns=["official_id", "season_year", "nqs_competitions"],
        )
    role_pred = _nqs_panel_role_sql_predicate(panel_role_kind)
    stmt = (
        text(
            f"""
            SELECT so.official_id AS official_id,
                   c.year::integer AS season_year,
                   COUNT(DISTINCT c.id) AS nqs_competitions
            FROM public.segment_official so
            INNER JOIN public.segment s ON s.id = so.segment_id
            INNER JOIN public.competition c ON c.id = s.competition_id
            WHERE so.official_id IS NOT NULL
              AND so.official_id IN :official_ids
              AND COALESCE(c.nqs, false) = true
              AND s.discipline_type_id IN :discipline_type_ids
              AND c.year ~ '^[0-9]+$'
              AND ({role_pred})
            GROUP BY so.official_id, c.year::integer
            """
        )
        .bindparams(
            bindparam("official_ids", expanding=True),
            bindparam("discipline_type_ids", expanding=True),
        )
    )
    try:
        with Session(engine) as session:
            rows = session.execute(
                stmt,
                {
                    "official_ids": official_ids,
                    "discipline_type_ids": list(segment_discipline_type_ids),
                },
            ).mappings().all()
    except Exception:
        return pd.DataFrame(
            columns=["official_id", "season_year", "nqs_competitions"],
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["official_id", "season_year", "nqs_competitions"],
    )


def get_nqs_detailed_activity_report_df(
    official_type_label: str,
    discipline_label: str,
    *,
    active_appointments_only: bool = True,
    directory_level_ids: tuple[int, ...] | None = None,
) -> pd.DataFrame:
    """
    One row per eligible directory official: name, USFS appointment level label, total NQS
    competitions, then counts by season year (from ``public.competition.year``).

    Eligibility: appointment in the selected role and discipline at one of the given
    directory levels (default: qualifying, sectional, national). Panel matches use
    ``public.segment_official``; competitions must have ``public.competition.nqs`` true.
    """
    if not activity_database_is_postgresql():
        return pd.DataFrame()

    try:
        appt_name = _nqs_directory_appointment_type_name(official_type_label)
        dir_disc_id, seg_dt_ids = _nqs_resolve_directory_and_segment_disciplines(
            official_type_label, discipline_label
        )
        panel_kind = _nqs_panel_role_kind(official_type_label)
    except ValueError:
        return pd.DataFrame()

    atid = _get_appointment_type_id_by_name(appt_name)
    if atid is None:
        return pd.DataFrame()

    level_ids = (
        directory_level_ids
        if directory_level_ids is not None
        else _nqs_eligible_directory_level_ids()
    )
    if not level_ids:
        return pd.DataFrame()

    qualified = get_qualified_officials(
        dir_disc_id,
        atid,
        level_ids,
        include_appointment_level=True,
        active_appointments_only=active_appointments_only,
    )
    if qualified.empty:
        return pd.DataFrame()

    qualified = qualified.sort_values(
        "achieved_date", ascending=True, na_position="last"
    ).drop_duplicates(subset=["official_id"], keep="first")
    level_names = _level_id_to_name_map(qualified["level_id"].tolist())
    qualified = qualified.copy()
    qualified["Level"] = qualified["level_id"].map(
        lambda v: level_names.get(int(v), "")
        if v is not None and not (isinstance(v, float) and pd.isna(v))
        else ""
    )
    oids = [int(x) for x in qualified["official_id"].tolist()]
    counts = _query_nqs_competition_counts_by_year(oids, panel_kind, seg_dt_ids)
    if counts.empty:
        pivot = pd.DataFrame({"official_id": oids})
    else:
        pivot = counts.pivot_table(
            index="official_id",
            columns="season_year",
            values="nqs_competitions",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()

    out = qualified[["official_id", "full_name", "Level"]].merge(
        pivot, on="official_id", how="left"
    )
    numeric_years = [c for c in out.columns if c not in ("official_id", "full_name", "Level")]
    for c in numeric_years:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)
    year_cols_sorted = sorted(
        numeric_years,
        key=lambda x: int(x) if isinstance(x, (int, float)) and not pd.isna(x) else 0,
        reverse=True,
    )
    if year_cols_sorted:
        out["Total"] = out[year_cols_sorted].sum(axis=1).astype(int)
    else:
        out["Total"] = 0

    out = out.rename(columns={"full_name": "Name"})
    out = out.drop(columns=["official_id"], errors="ignore")
    col_order = ["Name", "Level", "Total"] + year_cols_sorted
    out = out[[c for c in col_order if c in out.columns]]
    # Streamlit/PyArrow warn on mixed-type column labels (str vs int year keys from pivot).
    out.columns = pd.Index([str(c) for c in out.columns])
    return out.sort_values("Name", kind="mergesort").reset_index(drop=True)


if __name__ == "__main__":
    load_history(write_to_database=True)
# get_assignments_for_person("Rachael Naphtal Einstein")

# get_number_assignments_per_competition_type([5,6,7,9])
# activity_matrix = get_activity_matrix(2,1,[7],8)
# print(activity_matrix[activity_matrix["never_used"]==True]['full_name'])
