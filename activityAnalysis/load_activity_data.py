import os
import pandas as pd
from officials_analysis_models import (
    Officials,
    Appointments,
    Assignment,
    Disciplines,
    Competition,
    AppointmentTypes,
)
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select, func, and_
import math
import re
from datetime import datetime

appointment_codes_file = "activityAnalysis/Appointments_to_database.xlsx"
DATABASE_URL = os.environ.get("DATABASE_URL", "")


# Fix the prefix for SQLAlchemy 1.4+ compatibility
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

print (DATABASE_URL)
engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"options": "-csearch_path=officials_analysis"},
)


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
            select(Officials.id, Officials.full_name, Appointments.achieved_date)
            .join(Appointments, Appointments.official_id == Officials.id)
            .where(
                disc_filter,
                Appointments.appointment_type_id == appointment_type_id,
                Appointments.level_id.in_(level_ids),
            )
            .distinct()
        )

        rows = session.execute(stmt).all()

    return pd.DataFrame(rows, columns=["official_id", "full_name", "achieved_date"])


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

    fixed_cols = [
        "official_id",
        "full_name",
        "years_in_grade",
        "years_since_last",
        "most_recent_year",
        "never_used",
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
