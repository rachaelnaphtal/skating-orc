import re

import streamlit as st
from load_activity_data import (
    get_assigned_competition_counts,
    get_competition_count_for_types,
    get_activity_matrix,
    get_any_role_years,
    appointment_type_has_chiefs,
    get_chief_years,
    get_engine,
    get_referee_competition_count_for_types,
    get_referee_discipline_options_for_comp_group,
    get_referee_yearly_activity_report,
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

# USFS: `region` in the directory maps to a macro area; each area has sub-sections.
# Filter uses the macro (Eastern / Midwestern / Pacific Coast); display prefers
# the subsection label when the stored region matches a known subsection.
_REGION_TO_MACRO = {
    "eastern": "Eastern",
    "new england": "Eastern",
    "north atlantic": "Eastern",
    "south atlantic": "Eastern",
    "midwestern": "Midwestern",
    "upper great lakes": "Midwestern",
    "eastern great lakes": "Midwestern",
    "southwestern": "Midwestern",
    "pacific coast": "Pacific Coast",
    "northwest pacific": "Pacific Coast",
    "central pacific": "Pacific Coast",
    "southwest pacific": "Pacific Coast",
}

_SUBSECTION_DISPLAY = {
    "new england": "New England",
    "north atlantic": "North Atlantic",
    "south atlantic": "South Atlantic",
    "upper great lakes": "Upper Great Lakes",
    "eastern great lakes": "Eastern Great Lakes",
    "southwestern": "Southwestern",
    "northwest pacific": "Northwest Pacific",
    "central pacific": "Central Pacific",
    "southwest pacific": "Southwest Pacific",
}


def _normalize_for_region_lookup(region) -> str:
    """Normalize directory `region` strings for lookup (handles extra words / spacing)."""
    if region is None or (isinstance(region, float) and pd.isna(region)):
        return ""
    s = str(region).strip().lower()
    if not s or s == "nan":
        return ""
    s = re.sub(r"[\s,;/]+", " ", s).strip()
    for suffix in (" region", " section", " sectional", " sec"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    return s


def section_macro_from_region(region) -> str:
    """Eastern / Midwestern / Pacific Coast / (Other) / (Unspecified) — used for filter + Section column."""
    nk = _normalize_for_region_lookup(region)
    if not nk:
        return "(Unspecified)"
    if nk in _REGION_TO_MACRO:
        return _REGION_TO_MACRO[nk]
    # Subsection phrase embedded in longer label
    for sub_k in sorted(_SUBSECTION_DISPLAY.keys(), key=len, reverse=True):
        if sub_k in nk:
            return _REGION_TO_MACRO[sub_k]
    return "(Other)"


def section_subsection_label(region) -> str:
    """Canonical subsection name when `region` matches a known subsection; else empty string."""
    nk = _normalize_for_region_lookup(region)
    if not nk:
        return ""
    if nk in _SUBSECTION_DISPLAY:
        return _SUBSECTION_DISPLAY[nk]
    for sub_k in sorted(_SUBSECTION_DISPLAY.keys(), key=len, reverse=True):
        if sub_k in nk:
            return _SUBSECTION_DISPLAY[sub_k]
    return ""


def section_display_from_region(region) -> str:
    """
    Value for the Section column: macro (Eastern / Midwestern / Pacific Coast),
    optionally with subsection in parentheses when we can infer it from `region`
    (so Section is never a blind copy of the raw Region string).
    """
    macro = section_macro_from_region(region)
    if macro in ("(Unspecified)", "(Other)"):
        return macro
    sub = section_subsection_label(region)
    if sub:
        return f"{macro}"
    return macro


def filter_dataframe_by_section(df: pd.DataFrame, *, widget_key: str) -> pd.DataFrame:
    """Filter by macro section derived from `region` (Eastern / Midwestern / Pacific Coast)."""
    if df.empty or "region" not in df.columns:
        return df
    df = df.copy()
    df["_section_key"] = df["region"].map(section_macro_from_region)
    options = sorted(df["_section_key"].unique())
    selected = st.multiselect(
        "Section (from region)",
        options=options,
        default=options,
        help="Regions roll up to Eastern, Midwestern, or Pacific Coast. Clear all to show every section.",
        key=widget_key,
    )
    if len(selected) > 0:
        df = df[df["_section_key"].isin(selected)]
    return df.drop(columns=["_section_key"], errors="ignore")


st.set_page_config(layout="wide", page_title="Officials Activity Tracker",
                   page_icon="⛸️")
st.title("Officials Activity Tracker")

REPORT_CHAMPIONSHIPS_DETAILED = "Championships Detailed Activity"
REPORT_NUMBER_OF_ASSIGNMENTS = "Number of assignments"
REPORT_REFEREE_SERVICE = "Referee service (by competition type)"

report_mode = st.radio(
    "Report",
    options=[
        REPORT_CHAMPIONSHIPS_DETAILED,
        REPORT_NUMBER_OF_ASSIGNMENTS,
        REPORT_REFEREE_SERVICE,
    ],
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

if report_mode == REPORT_CHAMPIONSHIPS_DETAILED:
    show_other_roles = st.checkbox(
        f"Show attendance in other roles  ({OTHER_ROLE_SYMBOL} = present at competition in a different role)"
    )
    show_section_info = st.checkbox(
        "Add information on section",
        help="Show Region and Section columns, section filter, and a count summary by section for the current view.",
    )
else:
    show_other_roles = False
    show_section_info = False

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


@st.cache_data
def load_referee_discipline_options(comp_group_name):
    return get_referee_discipline_options_for_comp_group(comp_group_name)


@st.cache_data
def load_referee_competition_count(
    competition_type_ids_tuple, discipline_id_sentinel, comp_group_name
):
    """Competitions in these types that have ≥1 referee assignment (optional discipline)."""
    did = None if discipline_id_sentinel == -1 else int(discipline_id_sentinel)
    return get_referee_competition_count_for_types(
        list(competition_type_ids_tuple),
        discipline_id=did,
        comp_group_name=comp_group_name,
    )


@st.cache_data
def load_referee_yearly_report(
    competition_type_ids_tuple,
    discipline_id_sentinel,
    window_years,
    comp_group_name,
):
    """discipline_id_sentinel: -1 means overall (all disciplines); else discipline id."""
    did = None if discipline_id_sentinel == -1 else int(discipline_id_sentinel)
    return get_referee_yearly_activity_report(
        list(competition_type_ids_tuple),
        discipline_id=did,
        window_years=int(window_years),
        comp_group_name=comp_group_name,
    )


if report_mode == REPORT_REFEREE_SERVICE:
    comp_group = st.selectbox(
        "Competition Type Group",
        options=list(SUMMARY_COMPETITION_TYPES.keys()),
        index=0,
        key="referee_report_comp_group",
    )
    comp_type_ids = SUMMARY_COMPETITION_TYPES[comp_group]

    window_options = (5, 10, 15, 20)
    window_years = st.selectbox(
        "Year window (rolling, through current year)",
        options=window_options,
        index=window_options.index(10),
        key="referee_report_window_years",
        help="Counts and normalized rate use this many calendar years ending in the current year.",
    )
    win_start = CURRENT_YEAR - int(window_years) + 1

    disc_df = load_referee_discipline_options(comp_group)
    scope = st.radio(
        "Discipline scope",
        options=["Overall", "Single discipline"],
        horizontal=True,
        key="referee_report_disc_scope",
    )
    ref_discipline_sentinel = -1
    if scope == "Single discipline":
        if disc_df.empty:
            ref_cov = load_referee_competition_count(
                tuple(comp_type_ids), -1, comp_group
            )
            st.info(
                "No discipline options found for this competition group "
                "(check discipline names in the database)."
            )
            st.caption(
                f"Data coverage: {ref_cov} competitions in this group with at least one referee."
            )
            st.stop()
        by_name = dict(zip(disc_df["discipline_name"], disc_df["discipline_id"]))
        names = sorted(by_name.keys())
        picked = st.selectbox(
            "Discipline",
            options=names,
            key="referee_report_discipline",
        )
        ref_discipline_sentinel = int(by_name[picked])

    referee_df = load_referee_yearly_report(
        tuple(comp_type_ids),
        ref_discipline_sentinel,
        window_years,
        comp_group,
    )
    ref_competition_count = load_referee_competition_count(
        tuple(comp_type_ids), ref_discipline_sentinel, comp_group
    )
    if referee_df.empty:
        st.info(
            "No eligible referees for this competition group and discipline scope "
            "(see appointment level/discipline rules in the data layer)."
        )
        st.caption(
            f"Data coverage: {ref_competition_count} competitions in this view with at least one referee."
        )
        st.stop()

    norm_label = f"Normalized ({window_years} yr)"
    _sectional_ref_report = comp_group in (
        "SPD Sectionals",
        "Synchronized Sectionals",
    )
    yig_col = (
        "Years in grade (sect. ref.)"
        if _sectional_ref_report
        else "Years in grade (nat. ref.)"
    )
    _grade_caption = (
        "sectional referee achieved date in the report disciplines"
        if _sectional_ref_report
        else "national referee achieved date for the same discipline scope"
    )
    st.markdown(
        f"**{len(referee_df)} eligible officials** — {comp_group} — "
        f"assignment stats in these competition types — {window_years}-year window: {win_start}–{CURRENT_YEAR}"
    )
    st.caption(
        "Includes everyone eligible as a referee for this report (national and/or sectional "
        "rules by competition group), even with zero assignments in the selected types. "
        "Total years = distinct calendar years with at least one referee assignment "
        "in this competition group. In Years served, (C) after a year marks at least "
        "one chief assignment that year. Chief columns count distinct chief-years. "
        f"{norm_label} = years with an assignment in the window ÷ years you "
        f"could have served in that window after {_grade_caption} "
        "(unknown date → full window)."
    )
    st.caption(
        f"Data coverage: {ref_competition_count} competitions in this view with at least one referee."
    )

    display_ref = (
        referee_df.drop(columns=["discipline_label"], errors="ignore")
        .rename(
            columns={
                "full_name": "Name",
                "years_served_csv": "Years served",
                "total_distinct_years": "Total years",
                "chief_distinct_years": "Chief (all years)",
                "years_last_10": f"Years ({win_start}–{CURRENT_YEAR})",
                "chief_years_last_10": f"Chief years ({win_start}–{CURRENT_YEAR})",
                "years_in_grade": yig_col,
                "eligible_years_last_10": "Eligible yrs in window",
                "normalized_last_10": norm_label,
            }
        )
        .drop(columns=["official_id"], errors="ignore")
    )

    col_order = [
        c
        for c in [
            "Name",
            "Years served",
            "Total years",
            "Chief (all years)",
            f"Years ({win_start}–{CURRENT_YEAR})",
            f"Chief years ({win_start}–{CURRENT_YEAR})",
            yig_col,
            "Eligible yrs in window",
            norm_label,
        ]
        if c in display_ref.columns
    ]
    display_ref = display_ref[col_order]

    def _fmt_years_in_grade(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(int(v))

    _fmt_map = {}
    if yig_col in display_ref.columns:
        _fmt_map[yig_col] = _fmt_years_in_grade
    if norm_label in display_ref.columns:
        _fmt_map[norm_label] = "{:.3f}"
    styled_ref = (
        display_ref.style.format(_fmt_map, na_rep="") if _fmt_map else display_ref.style
    )

    ref_col_cfg = {"Name": st.column_config.Column(pinned=True)}
    ref_col_cfg["Years served"] = st.column_config.TextColumn(
        "Years served",
        help=(
            "Calendar years with a referee assignment in this view; "
            "(C) after a year means at least one chief assignment that year."
        ),
        width="large",
    )
    for num_col in (
        "Total years",
        "Chief (all years)",
        f"Years ({win_start}–{CURRENT_YEAR})",
        f"Chief years ({win_start}–{CURRENT_YEAR})",
        yig_col,
        "Eligible yrs in window",
    ):
        if num_col in display_ref.columns:
            ref_col_cfg[num_col] = st.column_config.NumberColumn(num_col, format="%.0f")
    if norm_label in display_ref.columns:
        ref_col_cfg[norm_label] = st.column_config.NumberColumn(norm_label, format="%.3f")

    st.dataframe(
        styled_ref,
        width="stretch",
        height=700,
        hide_index=True,
        column_config=ref_col_cfg,
    )
    appt_date = load_appt_data_date()
    if appt_date is not None:
        date_str = pd.Timestamp(appt_date).strftime("%-m/%-d/%Y")
        st.caption(f"Appointment data current as of {date_str}")
    st.stop()


if report_mode == REPORT_NUMBER_OF_ASSIGNMENTS:
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

    if not show_section_info:
        summary_df = summary_df.drop(columns=["region"], errors="ignore")

    if show_section_info:
        summary_df["section"] = summary_df["region"].map(section_display_from_region)
        summary_df = filter_dataframe_by_section(
            summary_df, widget_key="section_filter_assigned_summary"
        )
        if summary_df.empty:
            st.info("No officials match the current section filter.")
            st.stop()

    summary_rename = {
        "full_name": "Name",
        "competitions_assigned": "Competitions Assigned",
        "most_recent_year": "Most Recent",
    }
    if show_section_info:
        summary_rename["region"] = "Region"
        summary_rename["section"] = "Section"

    summary_df = summary_df.rename(columns=summary_rename)
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
    _order_base = ["Name", "Competitions Assigned", "Most Recent"]
    if show_section_info:
        _order_base = ["Name", "Region", "Section", "Competitions Assigned", "Most Recent"]
    _order = [c for c in _order_base if c in summary_df.columns]
    _rest = [c for c in summary_df.columns if c not in _order]
    summary_df = summary_df[_order + _rest]

    def _fmt_summary_recent(val):
        return "" if pd.isna(val) or int(val) == SENTINEL_LOW else str(int(val))

    styled_summary = summary_df.drop(columns=["official_id"], errors="ignore").style.format(
        {"Most Recent": _fmt_summary_recent},
        na_rep="",
    )
    st.markdown(f"**{len(summary_df)} officials** — {comp_group}")
    st.caption(f"Data coverage: {competition_count} unique competitions in this group.")
    summary_col_cfg = {"Name": st.column_config.Column(pinned=True)}
    if show_section_info:
        summary_col_cfg["Region"] = st.column_config.TextColumn("Region")
        summary_col_cfg["Section"] = st.column_config.TextColumn("Section")
    summary_col_cfg["Competitions Assigned"] = st.column_config.NumberColumn(
        "Competitions Assigned", format="%.0f"
    )
    summary_col_cfg["Most Recent"] = st.column_config.NumberColumn("Most Recent", format="%.0f")

    st.dataframe(
        styled_summary,
        width="stretch",
        height=700,
        hide_index=True,
        column_config=summary_col_cfg,
    )
    if show_section_info and "Section" in summary_df.columns:
        sec_counts = summary_df["Section"].value_counts().sort_index()
        st.caption(
            "By section (this view): "
            + " · ".join(f"{k}: {int(v)}" for k, v in sec_counts.items())
        )
    appt_date = load_appt_data_date()
    if appt_date is not None:
        date_str = pd.Timestamp(appt_date).strftime("%-m/%-d/%Y")
        st.caption(f"Appointment data current as of {date_str}")
    st.stop()


df = load_matrix(discipline_id, appointment_type_id, competition_type_id)
df, year_cols = normalize_df(df)
if show_section_info:
    df["section"] = df["region"].map(section_display_from_region)
    df = filter_dataframe_by_section(
        df, widget_key="section_filter_activity_tracker"
    )
else:
    df = df.drop(columns=["region", "section"], errors="ignore")
appt_has_chiefs = load_appt_has_chiefs(appointment_type_id)

if df.empty:
    st.info(
        "No officials found for the selected filters"
        + (" (or no rows match the section filter)." if show_section_info else ".")
    )
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
if show_section_info:
    rename["region"] = "Region"
    rename["section"] = "Section"
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
default_sort = [c for c in ["Yrs Since Last", "Yrs in Grade"] if c in sortable_cols]
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
if show_section_info:
    if "Region" in display.columns:
        col_cfg["Region"] = st.column_config.TextColumn("Region")
    if "Section" in display.columns:
        col_cfg["Section"] = st.column_config.TextColumn("Section")
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
if show_section_info and "Section" in display.columns:
    sec_counts = display["Section"].value_counts().sort_index()
    st.caption(
        "By section (this view): "
        + " · ".join(f"{k}: {int(v)}" for k, v in sec_counts.items())
    )
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
