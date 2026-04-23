import os
import pandas as pd
try:
    from activityAnalysis.officials_analysis_models import (
        Officials,
        Appointments,
        Assignment,
        Disciplines,
        Competition,
        AppointmentTypes,
        Levels,
    )
except ModuleNotFoundError:
    from officials_analysis_models import (
        Officials,
        Appointments,
        Assignment,
        Disciplines,
        Competition,
        AppointmentTypes,
        Levels,
    )
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select, func, and_, or_, case
import math
import re
from datetime import datetime

appointment_codes_file = "activityAnalysis/Appointments_to_database.xlsx"
def get_engine():
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set")

    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    return create_engine(DATABASE_URL, echo=False, connect_args={"options": "-csearch_path=officials_analysis"})


engine = get_engine()

# competition_type.id — identifies US Championships competitions (find/create rows).
# (Do not confuse with appointment_type ids, which are separate.)
US_CHAMPIONSHIPS_COMPETITION_TYPE_ID = 4
REFEREE_APPOINTMENT_TYPE_ID = 4
NATIONAL_LEVEL_ID = 7
SECTIONAL_LEVEL_ID = 2
DISC_DANCE_ID = 4
DISC_SYNCHRO_ID = 2
DISC_SINGLES_PAIRS_ID = 9

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


def get_or_create_competition(session, year, competition_name, competition_type):
    comp = (
        session.query(Competition)
        .filter_by(
            name=competition_name, year=year, competition_type_id=competition_type
        )
        .one_or_none()
    )

    if not comp:
        comp = Competition(
            name=competition_name, year=year, competition_type_id=competition_type
        )
        session.add(comp)
        session.flush()  # get ID

    return comp


def insert_assignments(session, assignments_df, officials):
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
            session, row["Year"], competiton_name, competition_type_id
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
            insert_assignments(session, us_synchro_sectionals_df, officials)
            insert_assignments(session, us_spd_sectionals_df, officials)


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


def get_qualified_officials(discipline_id, appointment_type_id, level_ids):
    with Session(engine) as session:
        disc_filter = (
            Appointments.discipline_id.is_(None)
            if discipline_id is None
            else Appointments.discipline_id == discipline_id
        )
        stmt = (
            select(
                Officials.id,
                Officials.full_name,
                Officials.region,
                Appointments.achieved_date,
            )
            .join(Appointments, Appointments.official_id == Officials.id)
            .where(
                disc_filter,
                Appointments.appointment_type_id == appointment_type_id,
                Appointments.level_id.in_(level_ids),
            )
            .distinct()
        )

        rows = session.execute(stmt).all()

    return pd.DataFrame(rows, columns=["official_id", "full_name", "region", "achieved_date"])


SINGLES_PAIRS_DISCIPLINE_ID = 8
SINGLES_DISCIPLINE_ID = 1
SINGLES_PAIRS_APPT_TYPES = {1, 4}  # Competition Judge, Referee


def _resolve_discipline_ids(discipline_id, appointment_type_id):
    """
    For Competition Judges and Referees selecting Singles/Pairs,
    also include Singles assignments (officials work both).
    """
    if (
        discipline_id == SINGLES_PAIRS_DISCIPLINE_ID
        and appointment_type_id in SINGLES_PAIRS_APPT_TYPES
    ):
        return [SINGLES_DISCIPLINE_ID, SINGLES_PAIRS_DISCIPLINE_ID]
    return [discipline_id] if discipline_id is not None else None


def get_assignment_years(discipline_id, appointment_type_id, competition_type_id):
    discipline_ids = _resolve_discipline_ids(discipline_id, appointment_type_id)

    with Session(engine) as session:
        filters = [
            Assignment.appointment_type_id == appointment_type_id,
            Competition.competition_type_id == competition_type_id,
        ]
        if discipline_ids is not None:
            filters.append(Assignment.discipline_id.in_(discipline_ids))

        stmt = (
            select(Assignment.official_id, Competition.year)
            .join(Competition, Assignment.competition_id == Competition.id)
            .where(*filters)
            .distinct()
            .order_by(Competition.year.desc())
        )

        rows = session.execute(stmt).all()

    return pd.DataFrame(rows, columns=["official_id", "year"])


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
        result["total_championships"] = (
            result[year_cols].fillna(0).sum(axis=1).astype(int)
        )
    else:
        result["total_championships"] = 0

    loc_meta = (
        qualified_df.sort_values("achieved_date", ascending=False, na_position="last")
        .drop_duplicates("official_id", keep="first")[["official_id", "region"]]
    )
    result = result.merge(loc_meta, on="official_id", how="left")

    fixed_cols = [
        "official_id",
        "full_name",
        "region",
        "years_in_grade",
        "years_since_last",
        "most_recent_year",
        "never_used",
        "total_championships",
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


def get_any_role_years(official_ids, competition_type_id):
    """
    Years each official attended the competition in ANY appointment type / discipline.
    Returns official_id, year, and role name so callers can show which role(s) they had.
    Used to mark cells where the person was present in a different role (○).
    """
    if not official_ids:
        return pd.DataFrame(columns=["official_id", "year", "role", "discipline_id"])
    with Session(engine) as session:
        stmt = (
            select(
                Assignment.official_id,
                Competition.year,
                AppointmentTypes.name.label("role"),
                Assignment.discipline_id,
            )
            .join(Competition, Assignment.competition_id == Competition.id)
            .join(AppointmentTypes, Assignment.appointment_type_id == AppointmentTypes.id)
            .where(
                Competition.competition_type_id == competition_type_id,
                Assignment.official_id.in_(official_ids),
            )
            .distinct()
            .order_by(Competition.year.desc(), AppointmentTypes.name)
        )
        rows = session.execute(stmt).all()
    return pd.DataFrame(rows, columns=["official_id", "year", "role", "discipline_id"])


def get_chief_years(
    official_ids, discipline_id, appointment_type_id, competition_type_id
):
    """
    Years each official served as chief in the currently selected role/discipline
    for the selected competition type.
    Returns: official_id, year
    """
    if not official_ids:
        return pd.DataFrame(columns=["official_id", "year"])

    discipline_ids = _resolve_discipline_ids(discipline_id, appointment_type_id)

    with Session(engine) as session:
        filters = [
            Assignment.appointment_type_id == appointment_type_id,
            Competition.competition_type_id == competition_type_id,
            Assignment.official_id.in_(official_ids),
            Assignment.chief.is_(True),
        ]
        if discipline_ids is not None:
            filters.append(Assignment.discipline_id.in_(discipline_ids))

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
    appt_type_name, discipline_id, discipline_name, chief
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
    Columns: full_name, appt_type_name, discipline_id, discipline_name, chief
    """
    with Session(engine) as session:
        stmt = (
            select(
                Officials.full_name,
                AppointmentTypes.name.label("appt_type_name"),
                Assignment.discipline_id,
                Disciplines.name.label("discipline_name"),
                Assignment.chief,
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


def get_activity_matrix(
    discipline_id, appointment_type_id, level_ids, competition_type_id
):
    qualified_df = get_qualified_officials(
        discipline_id, appointment_type_id, level_ids
    )

    activity_df = get_assignment_years(
        discipline_id, appointment_type_id, competition_type_id
    )

    result = build_activity_matrix(qualified_df, activity_df)
    # result = clean_activity_df(result)
    return result


# load_history(write_to_database=True)
# get_assignments_for_person("Rachael Naphtal Einstein")

# get_number_assignments_per_competition_type([5,6,7,9])
# activity_matrix = get_activity_matrix(2,1,[7],8)
# print(activity_matrix[activity_matrix["never_used"]==True]['full_name'])
