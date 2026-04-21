import streamlit as st
from load_activity_data import (
    get_assigned_competition_counts,
    get_competition_count_for_types,
    get_activity_matrix,
    get_any_role_years,
    appointment_type_has_chiefs,
    get_chief_years,
    get_engine,
)
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select
from officials_analysis_models import Disciplines, AppointmentTypes, Appointments
import pandas as pd
from datetime import datetime

engine = get_engine()

NATIONAL_LEVEL_ID = 7
SYNCHRO_DISCIPLINE_ID = 2
COMPETITION_TYPE_MAP = {SYNCHRO_DISCIPLINE_ID: 8}   # US Synchronized Skating Championships
DEFAULT_COMPETITION_TYPE = 4                          # US Championships
NO_DISCIPLINE_LABEL = "(No Discipline)"
CURRENT_YEAR = datetime.now().year

SENTINEL_HIGH = 9999
SENTINEL_LOW  = 0

OTHER_ROLE_SYMBOL = "○"
SELECTED_ROLE_SYMBOL = "✔"

# Short abbreviations shown inside ○ cells when other-roles mode is on
ROLE_ABBREV = {
    "Announcers":                          "Ann",
    "Competition Judge":                   "J",
    "Data Operator":                       "DO",
    "International Data / Video Operator": "IDVO",
    "International Judge":                 "IJ",
    "International Referee":               "IR",
    "International Technical Controller":  "ITC",
    "International Technical Specialist":  "ITS",
    "Music Coordinator":                   "MC",
    "Music Technician":                    "MT",
    "Referee":                             "Ref",
    "Scoring Official":                    "SO",
    "Scoring System Technician":           "SST",
    "Technical Controller":                "TC",
    "Technical Specialist":                "TS",
    "Test Judge":                          "TJ",
    "Video Replay":                        "VR",
}

# Discipline suffixes appended when the assignment is for a different discipline
# e.g. "TS-S" = Technical Specialist for Singles
DISC_ABBREV = {
    1: "S",    # Singles
    2: "SYS",  # Synchronized
    4: "D",    # Dance
    8: "P",    # Pairs
    9: "SP",   # Singles/Pairs
}

st.set_page_config(layout="wide", page_title="Officials Activity Tracker",
                   page_icon="⛸️")
st.title("Officials Activity Tracker")

report_mode = st.radio(
    "Report Type",
    options=["Activity Tracker", "Assigned Summary"],
    horizontal=True,
)

SUMMARY_COMPETITION_TYPES = {
    "US Championships": [4],
    "US Synchronized Skating Championships": [8],
    "SPD Sectionals": [1, 2, 3],
    "Synchronized Sectionals": [5, 6, 7, 9],
}


@st.cache_data
def load_appt_data_date():
    """Most recent achieved_date across all appointments — used as the data currency note."""
    from sqlalchemy import func as sqlfunc
    with Session(engine) as session:
        result = session.execute(
            select(sqlfunc.max(Appointments.achieved_date))
        ).scalar()
    return result


@st.cache_data
def load_all_appt_types():
    with Session(engine) as session:
        rows = session.execute(
            select(AppointmentTypes.id, AppointmentTypes.name)
            .join(Appointments, Appointments.appointment_type_id == AppointmentTypes.id)
            .where(Appointments.level_id == NATIONAL_LEVEL_ID)
            .distinct()
            .order_by(AppointmentTypes.name)
        ).all()
    return {name: id_ for id_, name in rows}


@st.cache_data
def load_disciplines_for_appt_type(appointment_type_id):
    from officials_analysis_models import Assignment
    with Session(engine) as session:
        rows = session.execute(
            select(Disciplines.id, Disciplines.name)
            .join(Appointments, Appointments.discipline_id == Disciplines.id)
            .join(Assignment, Assignment.discipline_id == Disciplines.id)
            .where(
                Appointments.appointment_type_id == appointment_type_id,
                Appointments.level_id == NATIONAL_LEVEL_ID,
                Assignment.appointment_type_id == appointment_type_id,
            )
            .distinct()
            .order_by(Disciplines.name)
        ).all()

    result = {}
    for id_, name in rows:
        result[name] = id_

    with Session(engine) as session:
        null_count = session.execute(
            select(Appointments.id)
            .where(
                Appointments.appointment_type_id == appointment_type_id,
                Appointments.level_id == NATIONAL_LEVEL_ID,
                Appointments.discipline_id.is_(None),
            )
            .limit(1)
        ).first()

    if null_count:
        result = {NO_DISCIPLINE_LABEL: None, **result}

    return result


# ---- MAIN PAGE FILTERS ----
col1, col2 = st.columns(2)

appt_types_map = load_all_appt_types()

with col1:
    appt_options = list(appt_types_map.keys())
    default_appt = "Competition Judge" if "Competition Judge" in appt_options else appt_options[0]
    selected_appt_type = st.selectbox(
        "Official Type",
        options=appt_options,
        index=appt_options.index(default_appt),
    )

appointment_type_id = appt_types_map[selected_appt_type]
disciplines_map = load_disciplines_for_appt_type(appointment_type_id)

with col2:
    disc_options = list(disciplines_map.keys())
    if not disc_options:
        st.info("No disciplines found for this official type.")
        st.stop()
    default_disc = "Synchronized" if "Synchronized" in disc_options else disc_options[0]
    selected_discipline = st.selectbox(
        "Discipline",
        options=disc_options,
        index=disc_options.index(default_disc) if default_disc in disc_options else 0,
    )

discipline_id = disciplines_map[selected_discipline]

if discipline_id is None:
    comp_choice = st.radio(
        "Competition",
        options=["US Championships", "US Synchro Championships"],
        horizontal=True,
    )
    competition_type_id = 8 if comp_choice == "US Synchro Championships" else DEFAULT_COMPETITION_TYPE
else:
    competition_type_id = COMPETITION_TYPE_MAP.get(discipline_id, DEFAULT_COMPETITION_TYPE)

show_other_roles = st.checkbox(
    f"Show attendance in other roles  ({OTHER_ROLE_SYMBOL} = present at competition in a different role)"
)

st.divider()


def normalize_df(df):
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    year_cols = [c for c in df.columns if c.isdigit()]
    for c in year_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df, year_cols


@st.cache_data
def load_matrix(discipline_id, appointment_type_id, competition_type_id):
    return get_activity_matrix(
        discipline_id,
        appointment_type_id,
        [NATIONAL_LEVEL_ID],
        competition_type_id,
    )


@st.cache_data
def load_any_role_data(official_ids_tuple, competition_type_id):
    return get_any_role_years(list(official_ids_tuple), competition_type_id)


@st.cache_data
def load_chief_data(
    official_ids_tuple, discipline_id, appointment_type_id, competition_type_id
):
    return get_chief_years(
        list(official_ids_tuple),
        discipline_id,
        appointment_type_id,
        competition_type_id,
    )


@st.cache_data
def load_appt_has_chiefs(appointment_type_id):
    return appointment_type_has_chiefs(appointment_type_id)


@st.cache_data
def load_assigned_summary(competition_type_ids_tuple):
    return get_assigned_competition_counts(list(competition_type_ids_tuple))


@st.cache_data
def load_competition_count(competition_type_ids_tuple):
    return get_competition_count_for_types(list(competition_type_ids_tuple))


if report_mode == "Assigned Summary":
    comp_group = st.selectbox(
        "Competition Type Group",
        options=list(SUMMARY_COMPETITION_TYPES.keys()),
        index=0,
    )
    comp_type_ids = SUMMARY_COMPETITION_TYPES[comp_group]
    competition_count = load_competition_count(tuple(comp_type_ids))

    summary_df = load_assigned_summary(tuple(comp_type_ids))
    if summary_df.empty:
        st.info("No assignments found for the selected competition type group.")
        st.caption(f"Data coverage: {competition_count} unique competitions in this group.")
        st.stop()

    summary_df = summary_df.rename(
        columns={
            "full_name": "Name",
            "competitions_assigned": "Competitions Assigned",
            "most_recent_year": "Most Recent",
        }
    )
    summary_df["Competitions Assigned"] = (
        pd.to_numeric(summary_df["Competitions Assigned"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    summary_df["Most Recent"] = (
        pd.to_numeric(summary_df["Most Recent"], errors="coerce")
        .fillna(SENTINEL_LOW)
        .astype(int)
    )
    summary_df = summary_df.sort_values(
        by=["Competitions Assigned", "Most Recent", "Name"],
        ascending=[False, False, True],
        kind="mergesort",
    )

    def _fmt_summary_recent(val):
        return "" if pd.isna(val) or int(val) == SENTINEL_LOW else str(int(val))

    styled_summary = summary_df.drop(columns=["official_id"], errors="ignore").style.format(
        {"Most Recent": _fmt_summary_recent},
        na_rep="",
    )
    st.markdown(f"**{len(summary_df)} officials** — {comp_group}")
    st.caption(f"Data coverage: {competition_count} unique competitions in this group.")
    st.dataframe(
        styled_summary,
        width="stretch",
        height=700,
        hide_index=True,
        column_config={
            "Name": st.column_config.Column(pinned=True),
            "Competitions Assigned": st.column_config.NumberColumn(
                "Competitions Assigned", format="%.0f"
            ),
            "Most Recent": st.column_config.NumberColumn("Most Recent", format="%.0f"),
        },
    )
    appt_date = load_appt_data_date()
    if appt_date is not None:
        date_str = pd.Timestamp(appt_date).strftime("%-m/%-d/%Y")
        st.caption(f"Appointment data current as of {date_str}")
    st.stop()


df = load_matrix(discipline_id, appointment_type_id, competition_type_id)
df, year_cols = normalize_df(df)
appt_has_chiefs = load_appt_has_chiefs(appointment_type_id)

if df.empty:
    st.info("No officials found for the selected filters.")
    st.stop()

year_cols_sorted = sorted(year_cols, reverse=True)
meta_cols = [c for c in df.columns if c not in year_cols]
display = df[[c for c in meta_cols + year_cols_sorted if c in df.columns]].copy()

# ---- OTHER-ROLE ATTENDANCE (optional) ----
# role_lookup maps (official_id, year_int) -> sorted list of (role_name, disc_id) tuples
role_lookup: dict = {}
chief_year_set = set()


def _role_label(role: str, disc_id, selected_disc_id) -> str:
    """Return abbreviated role name, appending discipline suffix when it differs."""
    base = ROLE_ABBREV.get(role, role)
    disc_id_int = int(disc_id) if disc_id is not None and not pd.isna(disc_id) else None
    if disc_id_int is not None and disc_id_int != selected_disc_id:
        suffix = DISC_ABBREV.get(disc_id_int, "")
        if suffix:
            return f"{base}-{suffix}"
    return base


if "official_id" in display.columns:
    official_ids = display["official_id"].dropna().astype(int).tolist()
    chief_df = load_chief_data(
        tuple(official_ids), discipline_id, appointment_type_id, competition_type_id
    )
    chief_year_set = {
        (int(row["official_id"]), int(row["year"])) for _, row in chief_df.iterrows()
    }
    if appt_has_chiefs:
        chief_recent = (
            chief_df.groupby("official_id")["year"]
            .max()
            .reset_index(name="chief_recent_year")
        )
        display = display.merge(chief_recent, on="official_id", how="left")

if show_other_roles and "official_id" in display.columns:
    any_role_df = load_any_role_data(tuple(official_ids), competition_type_id)

    # Build (official_id, year) -> deduplicated sorted list of (role, disc_id)
    seen: dict = {}
    for _, row in any_role_df.iterrows():
        key = (int(row["official_id"]), int(row["year"]))
        disc = int(row["discipline_id"]) if pd.notna(row.get("discipline_id")) else None
        entry = (row["role"], disc)
        seen.setdefault(key, set()).add(entry)
    role_lookup = {k: sorted(v, key=lambda x: (x[0], x[1] if x[1] is not None else -1)) for k, v in seen.items()}

    # Build set of all (official_id, year) present in any role
    any_role_set = set(role_lookup.keys())

    # Compute "last any role" stats per official
    any_last = (
        any_role_df.groupby("official_id")["year"].max()
        .reset_index()
        .rename(columns={"year": "any_last_year"})
    )
    any_last["any_last_year"] = any_last["any_last_year"].astype(int)

    display = display.merge(any_last, on="official_id", how="left")
    display["any_yrs_since"] = display["any_last_year"].apply(
        lambda v: CURRENT_YEAR - int(v) if pd.notna(v) else None
    )

    # Distinct championship years with any role at this competition type
    total_any = (
        any_role_df.groupby("official_id")["year"]
        .nunique()
        .reset_index(name="total_any_championships")
    )
    display = display.merge(total_any, on="official_id", how="left")
    display["total_any_championships"] = (
        pd.to_numeric(display["total_any_championships"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # Apply symbols to year columns
    for col in year_cols_sorted:
        if col not in display.columns:
            continue
        year_int = int(col)
        def mark_cell(row, yr=year_int):
            key = (int(row["official_id"]), yr)
            entries = role_lookup.get(key, [])
            if row[str(yr)] == 1:
                selected_symbol = (
                    f"C {SELECTED_ROLE_SYMBOL}"
                    if key in chief_year_set
                    else SELECTED_ROLE_SYMBOL
                )
                other = [(r, d) for r, d in entries if r != selected_appt_type]
                if other:
                    abbrevs = ", ".join(_role_label(r, d, discipline_id) for r, d in other)
                    return f"{selected_symbol} {OTHER_ROLE_SYMBOL}{abbrevs}"
                return selected_symbol
            if key in any_role_set:
                abbrevs = ", ".join(
                    _role_label(r, d, discipline_id) for r, d in entries
                )
                return f"{OTHER_ROLE_SYMBOL} {abbrevs}" if abbrevs else OTHER_ROLE_SYMBOL
            return ""
        display[col] = display.apply(mark_cell, axis=1)
else:
    for col in year_cols_sorted:
        if col in display.columns:
            year_int = int(col)
            def mark_in_role(row, yr=year_int):
                if row[str(yr)] != 1:
                    return ""
                key = (int(row["official_id"]), yr)
                return (
                    f"C {SELECTED_ROLE_SYMBOL}"
                    if key in chief_year_set
                    else SELECTED_ROLE_SYMBOL
                )
            display[col] = display.apply(mark_in_role, axis=1)

# ---- RENAME COLUMNS ----
rename = {
    "full_name": "Name",
    "years_in_grade": "Yrs in Grade",
    "years_since_last": "Yrs Since Last",
    "most_recent_year": "Most Recent",
    "never_used": "Never Assigned?",
    "total_championships": "# In Role",
}
if show_other_roles:
    rename["any_last_year"] = "Last (Any Role)"
    rename["any_yrs_since"] = "Yrs Since (Any)"
    rename["total_any_championships"] = "# Total"
if appt_has_chiefs:
    rename["chief_recent_year"] = "Most Recent as Chief"

display = display.rename(columns={k: v for k, v in rename.items() if k in display.columns})

if show_other_roles and "# Total" in display.columns and "# In Role" in display.columns:
    _cols = list(display.columns)
    _cols.remove("# Total")
    _insert_at = _cols.index("# In Role") + 1
    _cols.insert(_insert_at, "# Total")
    display = display[_cols]

if "Never Assigned?" in display.columns:
    display["Never Assigned?"] = display["Never Assigned?"].map(
        {True: "Yes", False: "", 1: "Yes", 0: ""}
    ).fillna("")

# ---- NUMERIC SENTINEL HANDLING ----
def _to_num_fill(series, sentinel):
    return pd.to_numeric(series, errors="coerce").fillna(sentinel).astype(int)

if "Yrs Since Last" in display.columns:
    display["Yrs Since Last"] = _to_num_fill(display["Yrs Since Last"], SENTINEL_HIGH)
if "Most Recent" in display.columns:
    display["Most Recent"] = _to_num_fill(display["Most Recent"], SENTINEL_LOW)
if "Most Recent as Chief" in display.columns:
    display["Most Recent as Chief"] = _to_num_fill(
        display["Most Recent as Chief"], SENTINEL_LOW
    )
if "Yrs in Grade" in display.columns:
    display["Yrs in Grade"] = _to_num_fill(display["Yrs in Grade"], SENTINEL_HIGH)
if "Last (Any Role)" in display.columns:
    display["Last (Any Role)"] = _to_num_fill(display["Last (Any Role)"], SENTINEL_LOW)
if "Yrs Since (Any)" in display.columns:
    display["Yrs Since (Any)"] = _to_num_fill(display["Yrs Since (Any)"], SENTINEL_HIGH)

# Drop internal official_id
display = display.drop(columns=["official_id"], errors="ignore")

# ---- MULTI-COLUMN SORT CONTROLS ----
sortable_cols = list(display.columns)
default_sort = [c for c in ["Yrs Since Last", "Yrs In Grade"] if c in sortable_cols]
sort_cols = st.multiselect(
    "Sort by (priority left to right)",
    options=sortable_cols,
    default=default_sort,
    help="Choose multiple columns to sort. First selection has highest priority.",
)
if sort_cols:
    sort_orders = []
    sort_ui_cols = st.columns(len(sort_cols))
    for idx, col in enumerate(sort_cols):
        with sort_ui_cols[idx]:
            order = st.selectbox(
                f"{col}",
                options=["Descending", "Ascending"],
                index=0,
                key=f"sort_order_{col}",
            )
            sort_orders.append(order == "Ascending")
    display = display.sort_values(by=sort_cols, ascending=sort_orders, kind="mergesort")

# ---- STYLER ----
def _fmt_hide_high(val):
    return "" if pd.isna(val) or int(val) == SENTINEL_HIGH else str(int(val))

def _fmt_hide_low(val):
    return "" if pd.isna(val) or int(val) == SENTINEL_LOW else str(int(val))

fmt_map = {
    "Yrs in Grade":   _fmt_hide_high,
    "Yrs Since Last": _fmt_hide_high,
    "Most Recent":      _fmt_hide_low,
}
if "Most Recent as Chief" in display.columns:
    fmt_map["Most Recent as Chief"] = _fmt_hide_low
if show_other_roles:
    fmt_map["Last (Any Role)"] = _fmt_hide_low
    fmt_map["Yrs Since (Any)"] = _fmt_hide_high

fmt_map_filtered = {k: v for k, v in fmt_map.items() if k in display.columns}
styled = display.style.format(fmt_map_filtered, na_rep="")

# ---- RENDER ----
col_cfg = {"Name": st.column_config.Column(pinned=True)}
if "# In Role" in display.columns:
    col_cfg["# In Role"] = st.column_config.NumberColumn(
        "# In Role",
        help="Years assigned in the selected official type at this championship (once per year)",
        format="%.0f",
    )
if show_other_roles and "# Total" in display.columns:
    col_cfg["# Total"] = st.column_config.NumberColumn(
        "# Total",
        help="Distinct years at this championship type counting any appointment type / discipline",
        format="%.0f",
    )

st.markdown(f"**{len(display)} officials** — National level · {selected_appt_type} · {selected_discipline}")
st.dataframe(styled, width="stretch", height=700, hide_index=True, column_config=col_cfg)
legend_parts = [f"{SELECTED_ROLE_SYMBOL} = Assigned in selected role"]
if appt_has_chiefs:
    legend_parts.insert(0, "C = Chief in selected role")
if show_other_roles:
    legend_parts.append(f"{OTHER_ROLE_SYMBOL} = Present in other role(s)")
st.caption("Legend: " + " · ".join(legend_parts))

appt_date = load_appt_data_date()
if appt_date is not None:
    date_str = pd.Timestamp(appt_date).strftime("%-m/%-d/%Y")
    st.caption(f"Appointment data current as of {date_str}")
