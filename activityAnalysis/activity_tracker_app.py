import re

import streamlit as st
from load_activity_data import (
    get_assigned_competition_counts,
    get_competition_count_for_types,
    get_activity_matrix,
    get_official_ids_with_isu_appointment,
    national_appointment_type_has_isu_mapping,
    get_activity_matrix_sectionals,
    get_sectional_assignment_region_rows,
    SECTIONAL_ACTIVITY_COMPETITION_TYPES,
    SECTIONAL_SPD_COMPETITION_TYPES,
    SECTIONAL_SYNCHRO_COMPETITION_TYPES,
    sectional_qualified_level_ids,
    build_sectional_year_allowed_type_map,
    sectional_epm_letters_from_competition_type_id,
    get_any_role_years,
    appointment_type_has_chiefs,
    get_chief_years,
    get_years_all_lower_level_only_in_role,
    get_engine,
    get_referee_competition_count_for_types,
    get_referee_discipline_options_for_comp_group,
    get_referee_yearly_activity_report,
    get_officials_with_assignments,
    get_official_assignment_detail_rows,
    get_official_appointment_rows,
    get_official_segment_official_activity_detail,
    get_competitions_for_report_dropdown,
    get_competition_assignment_rows,
    get_nqs_detailed_activity_report_df,
    activity_database_is_postgresql,
    NQS_REPORT_LEVEL_FILTER_BY_LABEL,
)
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, or_, select
from officials_analysis_models import (
    Disciplines,
    AppointmentTypes,
    Appointments,
    Assignment,
)
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

# Championships detailed: attendance as % of eligible years (shown after "# In Role")
PCT_ELIGIBLE_ATTENDED_COL = "% eligible attended"
PCT_ELIGIBLE_ATTENDED_HEADER = "% eligible\nattended"

# Detailed activity: latest calendar year with chief=True (roles that use chiefs only)
MOST_RECENT_CHIEF_COL = "Most recent chief"

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


def _competition_type_sort_key(competition_type_id) -> int:
    """Within a calendar year: US Synchro Champs, US Champs, SYS sectionals, SPD sectionals."""
    tid = int(competition_type_id)
    if tid == 8:
        return 0
    if tid == 4:
        return 1
    if tid in (5, 6, 7, 9):
        return 2
    if tid in (1, 2, 3):
        return 3
    return 99


# For per-person / per-competition assignment text: omit discipline suffix when this id.
_ASSIGNMENT_LABEL_OMIT_DISCIPLINE_ID = 7


def _discipline_id_int(discipline_id):
    if discipline_id is None or (isinstance(discipline_id, float) and pd.isna(discipline_id)):
        return None
    try:
        return int(discipline_id)
    except (TypeError, ValueError):
        return None


def _judging_results_index_url(url: str) -> str:
    """Ensure IJS results base URL opens the index page (…/index.asp)."""
    u = (url or "").strip()
    if not u:
        return u
    u = u.rstrip("/")
    if u.lower().endswith("index.asp"):
        return u
    return f"{u}/index.asp"


def _segment_official_disciplines_summary(series: pd.Series) -> str:
    """Comma-separated unique non-empty discipline labels (sorted, case-insensitive)."""
    parts = sorted(
        {str(x).strip() for x in series.dropna() if str(x).strip()},
        key=str.lower,
    )
    return ", ".join(parts)


def _panel_role_summary_label(role: str) -> str:
    """Map ``segment_official.role`` to a short label for per-competition deduping."""
    r = (role or "").strip()
    if not r:
        return ""
    rl = r.lower()
    if rl.startswith("judge"):
        return "Judge"
    if "referee" in rl:
        return "Referee"
    if "technical controller" in rl:
        return "Technical Controller"
    if "assistant technical specialist" in rl or "technical specialist" in rl:
        return "Technical Specialist"
    if "data operator" in rl or "replay operator" in rl:
        return "Data/Replay operator"
    return r


def _segment_official_panel_roles_summary(series: pd.Series) -> str:
    """Comma-separated unique IJS-style panel role labels for one competition."""
    labels: set[str] = set()
    for raw in series.dropna():
        lab = _panel_role_summary_label(str(raw))
        if lab:
            labels.add(lab)
    if not labels:
        return ""
    return ", ".join(sorted(labels, key=str.lower))


def _render_additional_segment_activity_slice(
    panel_detail: pd.DataFrame,
    *,
    section_subheader: str,
    empty_message: str,
    expander_widget_key: str,
) -> None:
    """Per-competition summary + segment rows for one qualifying slice of protocol activity."""
    st.subheader(section_subheader)
    if panel_detail.empty:
        st.info(empty_message)
        return
    summary = (
        panel_detail.groupby("competition_id", sort=False)
        .agg(
            year=("year", "first"),
            competition_name=("competition_name", "first"),
            results_url=("results_url", "first"),
            start_date=("start_date", "first"),
            end_date=("end_date", "first"),
            panel_segment_count=("segment_id", "nunique"),
            discipline=("discipline", _segment_official_disciplines_summary),
            panel_roles=("role", _segment_official_panel_roles_summary),
        )
        .reset_index()
    )
    pc = summary.copy()
    for col in ("start_date", "end_date"):
        if col in pc.columns:
            pc[col] = pd.to_datetime(pc[col], errors="coerce").dt.strftime("%Y-%m-%d")
    pc["results_url"] = pc["results_url"].map(_judging_results_index_url)
    display_cols = [
        "competition_name",
        "year",
        "start_date",
        "end_date",
        "discipline",
        "panel_roles",
        "panel_segment_count",
        "results_url",
    ]
    pc = pc[[c for c in display_cols if c in pc.columns]]
    st.dataframe(
        pc,
        width="stretch",
        hide_index=True,
        column_config={
            "competition_name": st.column_config.TextColumn(
                "Competition",
                width="large",
            ),
            "year": st.column_config.TextColumn("Season"),
            "start_date": st.column_config.TextColumn("Start"),
            "end_date": st.column_config.TextColumn("End"),
            "discipline": st.column_config.TextColumn(
                "Discipline",
                width="medium",
                help="From segment ``discipline_type`` (public.discipline_type). "
                "Multiple values if panels span categories at this competition.",
            ),
            "panel_roles": st.column_config.TextColumn(
                "Panel roles (IJS)",
                width="medium",
                help="Distinct protocol panel roles for this official at this competition "
                "(short labels from ``segment_official.role``).",
            ),
            "panel_segment_count": st.column_config.NumberColumn(
                "Segments",
                help="Distinct segments where this official is listed on a panel.",
                format="%d",
            ),
            "results_url": st.column_config.LinkColumn(
                "Results",
                display_text="Open",
                help="IJS results index (index.asp) for this competition.",
            ),
        },
    )
    with st.expander("Segments by competition", expanded=False, key=expander_widget_key):
        comp_ids = panel_detail["competition_id"].drop_duplicates().tolist()
        for idx, cid in enumerate(comp_ids):
            sub = panel_detail[panel_detail["competition_id"] == cid].copy()
            sub = sub.sort_values(
                ["segment_name", "discipline", "role"], kind="mergesort"
            )
            title = f"{sub.iloc[0]['competition_name']} ({sub.iloc[0]['year']})"
            st.markdown(f"**{title}**")
            sub_show = sub.assign(
                discipline=sub["discipline"].fillna("").astype(str)
            )
            st.dataframe(
                sub_show[["segment_name", "discipline", "role"]],
                width="stretch",
                hide_index=True,
                column_config={
                    "segment_name": st.column_config.TextColumn(
                        "Segment",
                        width="large",
                    ),
                    "discipline": st.column_config.TextColumn(
                        "Discipline",
                        width="small",
                    ),
                    "role": st.column_config.TextColumn(
                        "Role",
                        width="medium",
                    ),
                },
            )
            if idx < len(comp_ids) - 1:
                st.divider()


def _is_no_discipline_selection(discipline_id) -> bool:
    """``(No Discipline)`` in the picklist or stored ``discipline_id`` 7 (not applicable / legacy)."""
    if discipline_id is None or (isinstance(discipline_id, float) and pd.isna(discipline_id)):
        return True
    try:
        return int(discipline_id) == 7
    except (TypeError, ValueError):
        return False


def assignment_label_for_tables(
    appt_name: str,
    discipline_name: str,
    discipline_id,
    chief: bool,
    lower_levels_only: bool = False,
) -> str:
    """Full appointment type name for per-person / per-competition assignment columns."""
    an = (appt_name or "").strip()
    if chief:
        role = f"Chief {an}" if an else "Chief"
    else:
        role = an or "Assignment"
    disc_id = _discipline_id_int(discipline_id)
    if disc_id == _ASSIGNMENT_LABEL_OMIT_DISCIPLINE_ID:
        out = role
    else:
        dn = (discipline_name or "").strip()
        if dn:
            out = f"{role} – {dn}"
        else:
            out = role
    if lower_levels_only:
        return f"{out} (lower)"
    return out


def _assignment_lower_only_flag(val) -> bool:
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    return bool(val)


def build_person_assignments_display(detail_df: pd.DataFrame) -> pd.DataFrame:
    """One row per competition; assignments aggregated; sort year then event category."""
    if detail_df.empty:
        return pd.DataFrame(
            columns=["competition_id", "Competition", "Assignments"]
        )
    d = detail_df.copy()
    d["label"] = d.apply(
        lambda r: assignment_label_for_tables(
            str(r.get("appt_type_name") or ""),
            str(r.get("discipline_name") or "")
            if pd.notna(r.get("discipline_name"))
            else "",
            r.get("discipline_id"),
            bool(r.get("chief")),
            _assignment_lower_only_flag(r.get("lower_levels_only")),
        ),
        axis=1,
    )
    rows = []
    for cid, g in d.groupby("competition_id", sort=False):
        r0 = g.iloc[0]
        year = int(r0["year"])
        cname = str(r0["competition_name"] or "")
        ctid = int(r0["competition_type_id"])
        labels = sorted(g["label"].unique().tolist(), key=str.lower)
        rows.append(
            {
                "competition_id": int(cid),
                "_year": year,
                "_ctid": ctid,
                "_o": _competition_type_sort_key(ctid),
                "Competition": f"{year} {cname}".strip(),
                "Assignments": ", ".join(labels),
            }
        )
    out = pd.DataFrame(rows)
    out = out.sort_values(
        by=["_year", "_o", "_ctid", "Competition"],
        ascending=[False, True, True, True],
        kind="mergesort",
    )
    return out[
        ["competition_id", "Competition", "Assignments"]
    ].reset_index(drop=True)


def merge_panel_roles_into_person_assignment_summary(
    display_assign: pd.DataFrame, panel_detail: pd.DataFrame
) -> pd.DataFrame:
    """
    Join IJS protocol roles (``segment_official``) onto the per-competition assignments
    summary, one comma-separated ``Panel roles`` column per competition.
    """
    if display_assign.empty:
        return pd.DataFrame(columns=["Competition", "Assignments", "Panel roles"])
    out = display_assign.copy()
    if panel_detail.empty or "competition_id" not in panel_detail.columns:
        out["Panel roles"] = ""
    else:
        role_map: dict[int, str] = {}
        for cid, g in panel_detail.groupby("competition_id", sort=False):
            labels: set[str] = set()
            for raw in g["role"]:
                if pd.isna(raw):
                    continue
                lab = _panel_role_summary_label(str(raw))
                if lab:
                    labels.add(lab)
            if labels:
                role_map[int(cid)] = ", ".join(sorted(labels, key=str.lower))
        out["Panel roles"] = (
            out["competition_id"].map(lambda x: role_map.get(int(x), "")).fillna("")
        )
    return out[["Competition", "Assignments", "Panel roles"]].reset_index(drop=True)


def build_competition_assignments_display(roster_df: pd.DataFrame) -> pd.DataFrame:
    """One row per official with combined assignment descriptions (full role names)."""
    if roster_df.empty:
        return pd.DataFrame(columns=["Official", "Assignments"])
    d = roster_df.copy()
    d["label"] = d.apply(
        lambda r: assignment_label_for_tables(
            str(r.get("appt_type_name") or ""),
            str(r.get("discipline_name") or "")
            if pd.notna(r.get("discipline_name"))
            else "",
            r.get("discipline_id"),
            bool(r.get("chief")),
            _assignment_lower_only_flag(r.get("lower_levels_only")),
        ),
        axis=1,
    )
    parts = []
    for name, g in d.groupby("full_name", sort=False):
        labels = sorted(g["label"].unique().tolist(), key=str.lower)
        parts.append({"Official": name, "Assignments": ", ".join(labels)})
    out = pd.DataFrame(parts)
    return out.sort_values("Official", kind="mergesort").reset_index(drop=True)


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
REPORT_SECTIONALS_DETAILED = "Sectionals detailed activity"
REPORT_NUMBER_OF_ASSIGNMENTS = "Number of assignments"
REPORT_REFEREE_SERVICE = "Referee service (by competition type)"
REPORT_PERSON_ASSIGNMENTS = "Per-person assignments"
REPORT_COMPETITION_ASSIGNMENTS = "Per-competition assignments"
REPORT_NQS_DETAILED = "NQS detailed activity"

report_mode = st.radio(
    "Report",
    options=[
        REPORT_CHAMPIONSHIPS_DETAILED,
        REPORT_SECTIONALS_DETAILED,
        REPORT_NUMBER_OF_ASSIGNMENTS,
        REPORT_REFEREE_SERVICE,
        REPORT_PERSON_ASSIGNMENTS,
        REPORT_COMPETITION_ASSIGNMENTS,
        REPORT_NQS_DETAILED,
    ],
    horizontal=True,
)

active_appointments_only = True
if report_mode in (
    REPORT_CHAMPIONSHIPS_DETAILED,
    REPORT_SECTIONALS_DETAILED,
    REPORT_NQS_DETAILED,
):
    active_appointments_only = st.checkbox(
        "Active appointments only",
        value=True,
        help="When checked, only directory appointments with active = true (current USFS status) are used for who appears in the matrix and for the discipline list. "
        "Uncheck to include ended or removed roles that are still stored.",
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
def load_all_appt_types(active_appointments_only: bool = True):
    with Session(engine) as session:
        q = (
            select(AppointmentTypes.id, AppointmentTypes.name)
            .join(Appointments, Appointments.appointment_type_id == AppointmentTypes.id)
            .where(Appointments.level_id == NATIONAL_LEVEL_ID)
        )
        if active_appointments_only:
            q = q.where(Appointments.active.is_(True))
        rows = session.execute(
            q.distinct().order_by(AppointmentTypes.name)
        ).all()
    return {name: id_ for id_, name in rows}


@st.cache_data
def load_disciplines_for_appt_type(
    appointment_type_id,
    include_sectional_appointment_levels: bool,
    active_appointments_only: bool = True,
):
    """
    ``include_sectional_appointment_levels`` (sectionals report): include appointments at
    sectional level (id 2 for most roles, id 8 for Scoring Official / Scoring System Tech)
    in addition to national, so discipline picklists and no-discipline options match the matrix.
    """
    if include_sectional_appointment_levels:
        level_ids = tuple(sectional_qualified_level_ids(appointment_type_id))
    else:
        level_ids = (NATIONAL_LEVEL_ID,)

    with Session(engine) as session:
        w = [
            Appointments.appointment_type_id == appointment_type_id,
            Appointments.level_id.in_(level_ids),
            Assignment.appointment_type_id == appointment_type_id,
        ]
        if active_appointments_only:
            w.append(Appointments.active.is_(True))
        rows = session.execute(
            select(Disciplines.id, Disciplines.name)
            .join(Appointments, Appointments.discipline_id == Disciplines.id)
            .join(Assignment, Assignment.discipline_id == Disciplines.id)
            .where(*w)
            .distinct()
            .order_by(Disciplines.name)
        ).all()

    result = {}
    for id_, name in rows:
        result[name] = id_

    # Competition Judge / Referee: directory and assignments may be Singles (1) or
    # Singles/Pairs (9) on different rows. The join above only sees officials where
    # appointment and assignment share the *same* discipline_id; add Singles/Pairs to
    # the list when any official has qualifying activity in {1, 9} for both tables.
    if include_sectional_appointment_levels and int(appointment_type_id) in (1, 4):
        with Session(engine) as session:
            _sw = [
                Appointments.appointment_type_id == appointment_type_id,
                Appointments.level_id.in_(level_ids),
                Appointments.discipline_id.in_((1, 9)),
                Assignment.discipline_id.in_((1, 9)),
            ]
            if active_appointments_only:
                _sw.append(Appointments.active.is_(True))
            has_singles_umbrella = session.execute(
                select(1)
                .select_from(Appointments)
                .join(
                    Assignment,
                    (Assignment.official_id == Appointments.official_id)
                    & (Assignment.appointment_type_id == appointment_type_id),
                )
                .where(*_sw)
                .limit(1)
            ).first()
        if has_singles_umbrella and 9 not in result.values():
            with Session(engine) as session:
                drow = session.execute(
                    select(Disciplines.id, Disciplines.name).where(Disciplines.id == 9)
                ).first()
            if drow:
                _did, dname = drow[0], drow[1]
                if dname:
                    result[str(dname).strip()] = int(_did)

    with Session(engine) as session:
        _nw = [
            Appointments.appointment_type_id == appointment_type_id,
            Appointments.level_id.in_(level_ids),
            or_(
                Appointments.discipline_id.is_(None),
                Appointments.discipline_id == _ASSIGNMENT_LABEL_OMIT_DISCIPLINE_ID,
            ),
        ]
        if active_appointments_only:
            _nw.append(Appointments.active.is_(True))
        null_count = session.execute(
            select(Appointments.id).where(*_nw).limit(1)
        ).first()

    if null_count:
        result = {NO_DISCIPLINE_LABEL: None, **result}

    return result


@st.cache_data
def load_nqs_activity_table(
    official_type: str,
    discipline: str,
    active_only: bool,
    directory_level_ids: tuple[int, ...],
):
    return get_nqs_detailed_activity_report_df(
        official_type,
        discipline,
        active_appointments_only=active_only,
        directory_level_ids=directory_level_ids,
    )


# ---- MAIN PAGE FILTERS (Championships / sectionals activity reports) ----
appt_types_map = load_all_appt_types(active_appointments_only)

if report_mode in (REPORT_CHAMPIONSHIPS_DETAILED, REPORT_SECTIONALS_DETAILED):
    col1, col2 = st.columns(2)
    with col1:
        appt_options = list(appt_types_map.keys())
        default_appt = (
            "Competition Judge" if "Competition Judge" in appt_options else appt_options[0]
        )
        selected_appt_type = st.selectbox(
            "Official Type",
            options=appt_options,
            index=appt_options.index(default_appt),
        )

    appointment_type_id = appt_types_map[selected_appt_type]
    disciplines_map = load_disciplines_for_appt_type(
        appointment_type_id,
        report_mode == REPORT_SECTIONALS_DETAILED,
        active_appointments_only,
    )

    with col2:
        disc_options = list(disciplines_map.keys())
        if not disc_options:
            st.info("No disciplines found for this official type.")
            st.stop()
        default_disc = None
        for candidate in (
            "Singles/Pairs",
            "Singles / Pairs",
            "SP",
        ):
            if candidate in disc_options:
                default_disc = candidate
                break
        if default_disc is None:
            for _lab, _did in disciplines_map.items():
                # Singles/Pairs directory id is 9 (8 is Pairs as its own discipline).
                if _did is not None and int(_did) == 9 and _lab in disc_options:
                    default_disc = _lab
                    break
        if default_disc is None:
            default_disc = (
                "Synchronized" if "Synchronized" in disc_options else disc_options[0]
            )
        selected_discipline = st.selectbox(
            "Discipline",
            options=disc_options,
            index=disc_options.index(default_disc) if default_disc in disc_options else 0,
        )

    discipline_id = disciplines_map[selected_discipline]
    # Sectionals + no real discipline: choose SPD vs Synchro sectionals. Championships + no discipline: Champs.
    if _is_no_discipline_selection(discipline_id):
        if report_mode == REPORT_SECTIONALS_DETAILED:
            comp_choice = st.radio(
                "Competition",
                options=[
                    "Combined (all sectionals)",
                    "SPD sectionals",
                    "Synchro sectionals",
                ],
                index=0,
                horizontal=True,
            )
            if comp_choice == "Combined (all sectionals)":
                sectional_competition_type_ids = SECTIONAL_ACTIVITY_COMPETITION_TYPES
            elif comp_choice == "SPD sectionals":
                sectional_competition_type_ids = SECTIONAL_SPD_COMPETITION_TYPES
            else:
                sectional_competition_type_ids = SECTIONAL_SYNCHRO_COMPETITION_TYPES
            competition_type_id = DEFAULT_COMPETITION_TYPE
        else:
            comp_choice = st.radio(
                "Competition",
                options=["US Championships", "US Synchro Championships"],
                horizontal=True,
            )
            competition_type_id = (
                8
                if comp_choice == "US Synchro Championships"
                else DEFAULT_COMPETITION_TYPE
            )
            sectional_competition_type_ids = None
    else:
        competition_type_id = COMPETITION_TYPE_MAP.get(
            discipline_id, DEFAULT_COMPETITION_TYPE
        )
        if report_mode == REPORT_SECTIONALS_DETAILED:
            sectional_competition_type_ids = SECTIONAL_ACTIVITY_COMPETITION_TYPES
        else:
            sectional_competition_type_ids = None
else:
    # Unused on other reports (each branch stops); placeholders keep names defined.
    appointment_type_id = (
        appt_types_map["Competition Judge"]
        if "Competition Judge" in appt_types_map
        else next(iter(appt_types_map.values()))
    )
    discipline_id = SYNCHRO_DISCIPLINE_ID
    competition_type_id = DEFAULT_COMPETITION_TYPE
    sectional_competition_type_ids = None

if report_mode in (REPORT_CHAMPIONSHIPS_DETAILED, REPORT_SECTIONALS_DETAILED):
    show_other_roles = st.checkbox(
        f"Show attendance in other roles  ({OTHER_ROLE_SYMBOL} = present at competition in a different role)"
    )
    show_section_info = st.checkbox(
        "Add information on section",
        help="Show Region and Section columns, section filter, and a count summary by section for the current view.",
    )
    if report_mode == REPORT_CHAMPIONSHIPS_DETAILED:
        include_lower_levels = st.checkbox(
            "Include Lower Levels",
            value=True,
            help="When checked, all assignments count, including lower-levels-only. "
            "When unchecked, only assignments that are not lower-levels-only.",
        )
    else:
        include_lower_levels = None
else:
    show_other_roles = False
    show_section_info = False
    include_lower_levels = None

st.divider()


def normalize_df(df):
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    year_cols = [c for c in df.columns if c.isdigit()]
    for c in year_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df, year_cols


@st.cache_data
def load_matrix(
    discipline_id,
    appointment_type_id,
    competition_type_id,
    include_lower_levels,
    active_appointments_only: bool,
):
    return get_activity_matrix(
        discipline_id,
        appointment_type_id,
        [NATIONAL_LEVEL_ID],
        competition_type_id,
        include_lower_levels=include_lower_levels,
        active_appointments_only=active_appointments_only,
    )


@st.cache_data
def load_matrix_sectionals(
    discipline_id,
    appointment_type_id,
    sectional_competition_type_ids,
    active_appointments_only: bool,
):
    return get_activity_matrix_sectionals(
        discipline_id,
        appointment_type_id,
        sectional_competition_type_ids,
        active_appointments_only=active_appointments_only,
    )


@st.cache_data
def load_sectional_region_rows(
    discipline_id,
    appointment_type_id,
    official_ids_tuple,
    sectional_competition_type_ids,
):
    return get_sectional_assignment_region_rows(
        discipline_id,
        appointment_type_id,
        list(official_ids_tuple),
        sectional_competition_type_ids,
    )


@st.cache_data
def load_any_role_data(official_ids_tuple, competition_scope, include_lower_levels):
    return get_any_role_years(
        list(official_ids_tuple),
        competition_scope,
        include_lower_levels=include_lower_levels,
    )


@st.cache_data
def load_chief_data(
    official_ids_tuple,
    discipline_id,
    appointment_type_id,
    competition_scope,
    include_lower_levels,
):
    return get_chief_years(
        list(official_ids_tuple),
        discipline_id,
        appointment_type_id,
        competition_scope,
        include_lower_levels=include_lower_levels,
    )


@st.cache_data
def load_all_lower_level_only_years(
    official_ids_tuple,
    discipline_id,
    appointment_type_id,
    competition_scope,
):
    return get_years_all_lower_level_only_in_role(
        list(official_ids_tuple),
        discipline_id,
        appointment_type_id,
        competition_scope,
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


@st.cache_data
def load_officials_with_assignments():
    return get_officials_with_assignments()


@st.cache_data
def load_person_assignment_rows(official_id: int):
    return get_official_assignment_detail_rows(int(official_id))


@st.cache_data
def load_person_segment_official_activity_detail(official_id: int):
    return get_official_segment_official_activity_detail(int(official_id))


@st.cache_data
def load_person_appointment_rows(official_id: int):
    return get_official_appointment_rows(int(official_id))


@st.cache_data
def load_competitions_report_dropdown():
    return get_competitions_for_report_dropdown()


@st.cache_data
def load_competition_assignment_rows(competition_id: int):
    return get_competition_assignment_rows(int(competition_id))


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
        "Years in grade"
        if _sectional_ref_report
        else "Years in grade (nat. ref.)"
    )
    if comp_group == "SPD Sectionals" and scope == "Overall":
        _grade_caption = (
            "the date that gives the longest time in grade between Dance and Singles/Pairs "
            "(for each, sectional referee achieved date when you have one there, "
            "otherwise national referee there)"
        )
    else:
        _grade_caption = (
            "your earliest sectional referee achieved date in the report disciplines when you "
            "have one, otherwise your earliest national referee date there"
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


if report_mode == REPORT_PERSON_ASSIGNMENTS:
    officials_df = load_officials_with_assignments()
    if officials_df.empty:
        st.info("No officials with assignments were found.")
        st.stop()
    id_to_name = dict(
        zip(officials_df["official_id"].astype(int), officials_df["full_name"])
    )
    oid_list = sorted(id_to_name.keys(), key=lambda i: (str(id_to_name[i]).lower(), i))
    pick_oid = st.selectbox(
        "Official",
        options=oid_list,
        format_func=lambda i: id_to_name.get(int(i), str(i)),
        key="per_person_official_select",
    )
    pick_oid = int(pick_oid)
    detail = load_person_assignment_rows(pick_oid)
    panel_detail = load_person_segment_official_activity_detail(pick_oid)
    display_assign = build_person_assignments_display(detail)
    display_assign = merge_panel_roles_into_person_assignment_summary(
        display_assign, panel_detail
    )
    st.subheader("Assignments")
    if display_assign.empty:
        st.info("No assignments for this official.")
    else:
        st.dataframe(
            display_assign,
            width="stretch",
            hide_index=True,
            column_config={
                "Competition": st.column_config.TextColumn(
                    "Competition",
                    help="Year and competition name (newest year first; within a year: "
                    "US Synchronized Championships, US Championships, synchronized sectionals, "
                    "then singles/pairs & dance sectionals).",
                    width="large",
                ),
                "Assignments": st.column_config.TextColumn(
                    "Assignments",
                    width="large",
                    help="Full appointment type names; discipline omitted when not applicable. "
                    "Suffix (lower) means the assignment is lower-levels only.",
                ),
                "Panel roles": st.column_config.TextColumn(
                    "Panel roles (IJS)",
                    width="medium",
                    help="Roles from IJS protocol panels (``public.segment_official``) "
                    "where this official is listed for that competition. "
                    "Distinct from USFS directory assignments in the previous column.",
                ),
            },
        )
    appts = load_person_appointment_rows(pick_oid)
    st.subheader("Appointments")
    if appts.empty:
        st.info("No appointment records for this official.")
    else:
        appts_show = appts.copy()
        if "achieved_date" in appts_show.columns:
            appts_show["achieved_date"] = pd.to_datetime(
                appts_show["achieved_date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        st.dataframe(appts_show, width="stretch", hide_index=True)
    st.caption(
        "Competitions where this official appears on an IJS protocol. Sorted by event dates when present, then season year. "
        "Compiled independently of assignments above. This should not be considered a "
        "full record of activity; non-qualifying competitions in particular are non-exhaustive."
    )
    if panel_detail.empty:
        st.info(
            "No protocol/panel rows for this official, or the database is not PostgreSQL "
            "with judging ``public`` tables available."
        )
    else:
        is_qualifying = panel_detail["qualifying"].fillna(False).astype(bool)
        qual_detail = panel_detail[is_qualifying]
        nonqual_detail = panel_detail[~is_qualifying]
        _render_additional_segment_activity_slice(
            qual_detail,
            section_subheader="Additional Qualifying Activity",
            empty_message="No qualifying protocol activity for this official.",
            expander_widget_key="person_seg_official_qual_exp",
        )
        _render_additional_segment_activity_slice(
            nonqual_detail,
            section_subheader="Additional Nonqualifying Activity",
            empty_message="No non-qualifying protocol activity for this official.",
            expander_widget_key="person_seg_official_nonqual_exp",
        )
    st.stop()


if report_mode == REPORT_COMPETITION_ASSIGNMENTS:
    comps_df = load_competitions_report_dropdown()
    if comps_df.empty:
        st.info("No competitions found in the database.")
        st.stop()
    label_map = {
        int(row["competition_id"]): f"{int(row['year'])} — {row['name']}"
        for _, row in comps_df.iterrows()
    }
    cid_list = [int(x) for x in comps_df["competition_id"].tolist()]
    pick_cid = st.selectbox(
        "Competition",
        options=cid_list,
        format_func=lambda cid: label_map.get(int(cid), str(cid)),
        key="per_competition_select",
    )
    pick_cid = int(pick_cid)
    comp_assign_rows = load_competition_assignment_rows(pick_cid)
    display_comp_assign = build_competition_assignments_display(comp_assign_rows)
    st.subheader("Assignments")
    if display_comp_assign.empty:
        st.info("No assignments at this competition.")
    else:
        st.dataframe(
            display_comp_assign,
            width="stretch",
            hide_index=True,
            column_config={
                "Official": st.column_config.TextColumn("Official", pinned=True),
                "Assignments": st.column_config.TextColumn(
                    "Assignments",
                    width="large",
                    help="Full appointment type names; discipline omitted when not applicable. "
                    "Suffix (lower) means the assignment is lower-levels only.",
                ),
            },
        )
    st.stop()


if report_mode == REPORT_NQS_DETAILED:
    if not activity_database_is_postgresql():
        st.info(
            "NQS panel activity needs PostgreSQL with judging ``public`` tables and a "
            "``competition.nqs`` column (see migration ``005_public_competition_nqs.sql``)."
        )
        st.stop()
    nqs_official_type = st.selectbox(
        "Official type",
        options=[
            "Judge",
            "Referee",
            "Technical Controller",
            "Technical Specialist",
        ],
        key="nqs_official_type",
    )
    if nqs_official_type in ("Judge", "Referee"):
        nqs_discipline = st.selectbox(
            "Discipline",
            options=["Singles/Pairs", "Dance"],
            key="nqs_discipline_jr",
        )
    else:
        nqs_discipline = st.selectbox(
            "Discipline",
            options=["Singles", "Pairs", "Dance"],
            key="nqs_discipline_tc_ts",
        )
    nqs_level_labels = st.multiselect(
        "Official level (directory)",
        options=list(NQS_REPORT_LEVEL_FILTER_BY_LABEL.keys()),
        default=list(NQS_REPORT_LEVEL_FILTER_BY_LABEL.keys()),
        key="nqs_level_filter",
    )
    if not nqs_level_labels:
        st.info("Select at least one appointment level.")
        st.stop()
    nqs_directory_level_ids = tuple(
        sorted(NQS_REPORT_LEVEL_FILTER_BY_LABEL[lbl] for lbl in nqs_level_labels)
    )
    nqs_table = load_nqs_activity_table(
        nqs_official_type,
        nqs_discipline,
        active_appointments_only,
        nqs_directory_level_ids,
    )
    if nqs_table.empty:
        st.info(
            "No officials match the directory filters, the appointment type was not found, "
            "or NQS query failed (e.g. missing ``nqs`` column)."
        )
    else:
        nqs_col_cfg = {
            "Name": st.column_config.TextColumn("Name", width="large"),
            "Level": st.column_config.TextColumn("Level", width="medium"),
            "Total": st.column_config.NumberColumn("Total", format="%d"),
        }
        for _c in nqs_table.columns:
            if _c not in ("Name", "Level", "Total"):
                nqs_col_cfg[_c] = st.column_config.NumberColumn(str(_c), format="%d")
        st.dataframe(
            nqs_table,
            width="stretch",
            hide_index=True,
            column_config=nqs_col_cfg,
        )
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


is_sectionals_detailed = report_mode == REPORT_SECTIONALS_DETAILED
is_championships_detailed = report_mode == REPORT_CHAMPIONSHIPS_DETAILED
championships_lower_levels_filter = (
    None if is_sectionals_detailed else include_lower_levels
)
matrix_competition_scope = (
    tuple(sectional_competition_type_ids)
    if is_sectionals_detailed
    else competition_type_id
)
# Same competition-type scope as the matrix (other roles limited to the selected sectional set).
any_role_competition_scope = matrix_competition_scope

if is_sectionals_detailed:
    df = load_matrix_sectionals(
        discipline_id,
        appointment_type_id,
        tuple(sectional_competition_type_ids),
        active_appointments_only,
    )
else:
    df = load_matrix(
        discipline_id,
        appointment_type_id,
        competition_type_id,
        championships_lower_levels_filter,
        active_appointments_only,
    )
df, year_cols = normalize_df(df)
if is_sectionals_detailed:
    if (
        _is_no_discipline_selection(discipline_id)
        and tuple(sectional_competition_type_ids) == tuple(SECTIONAL_SPD_COMPETITION_TYPES)
    ):
        _sec_caption = (
            "This view is **SPD sectionals** only (Eastern, Midwestern, Pacific Coast). "
            "Year columns are calendar years. After the checkmark, E, M, and P come from event type. "
        )
    elif (
        _is_no_discipline_selection(discipline_id)
        and tuple(sectional_competition_type_ids) == tuple(SECTIONAL_SYNCHRO_COMPETITION_TYPES)
    ):
        _sec_caption = (
            "This view is **synchro sectionals** only. "
            "Year columns are calendar years. After the checkmark, E, M, and P come from event type. "
        )
    else:
        _sec_caption = (
            "Year columns combine singles/pairs & dance sectionals and synchronized sectionals "
            "in the same calendar year. After the checkmark, E, M, and P show which macro section "
            "each assignment is for (Eastern, Midwestern, or Pacific Coast). Combined "
            "Midwestern/Pacific synchro sectionals list both M and P."
        )
    st.caption(_sec_caption)
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
chief_year_set: set = set()
all_lower_level_year_set: set = set()
region_letter_sets: dict[tuple[int, int], set] = {}


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
        tuple(official_ids),
        discipline_id,
        appointment_type_id,
        matrix_competition_scope,
        championships_lower_levels_filter,
    )
    chief_year_set = {
        (int(row["official_id"]), int(row["year"])) for _, row in chief_df.iterrows()
    }
    all_lower_level_year_set = {
        (int(r["official_id"]), int(r["year"]))
        for _, r in load_all_lower_level_only_years(
            tuple(sorted(set(official_ids))),
            discipline_id,
            appointment_type_id,
            matrix_competition_scope,
        ).iterrows()
    }
    if appt_has_chiefs:
        chief_recent = (
            chief_df.groupby("official_id")["year"]
            .max()
            .reset_index(name="chief_recent_year")
        )
        display = display.merge(chief_recent, on="official_id", how="left")

    if is_sectionals_detailed:
        reg_df = load_sectional_region_rows(
            discipline_id,
            appointment_type_id,
            tuple(sorted(set(official_ids))),
            tuple(sectional_competition_type_ids),
        )
        for _, row in reg_df.iterrows():
            rk = (int(row["official_id"]), int(row["year"]))
            ctid = pd.to_numeric(row.get("competition_type_id"), errors="coerce")
            if pd.isna(ctid):
                continue
            for letter in sectional_epm_letters_from_competition_type_id(int(ctid)):
                region_letter_sets.setdefault(rk, set()).add(letter)


def _region_suffix(key: tuple) -> str:
    letters = region_letter_sets.get(key)
    if not letters:
        return ""
    order = {"E": 0, "M": 1, "P": 2}
    return " " + ",".join(sorted(letters, key=lambda x: order.get(x, 9)))


def _chief_lower_prefix(key: tuple) -> str:
    """Prefix like ``C L `` for chief and/or all-lower-level-only years (championships + sectionals)."""
    parts = []
    if key in chief_year_set:
        parts.append("C")
    if key in all_lower_level_year_set:
        parts.append("L")
    return (" ".join(parts) + " ") if parts else ""


if show_other_roles and "official_id" in display.columns:
    any_role_df = load_any_role_data(
        tuple(official_ids),
        any_role_competition_scope,
        championships_lower_levels_filter,
    )
    if is_sectionals_detailed:
        prim = load_sectional_region_rows(
            discipline_id,
            appointment_type_id,
            tuple(sorted(set(official_ids))),
            tuple(sectional_competition_type_ids),
        )
        allowed_by_year = build_sectional_year_allowed_type_map(prim)
        if not any_role_df.empty:
            oids = any_role_df["official_id"].astype(int)
            yrs = any_role_df["year"].astype(int)
            cts = pd.to_numeric(
                any_role_df["competition_type_id"], errors="coerce"
            ).fillna(-1).astype(int)
            mask = [
                (allowed_by_year.get((int(o), int(y))) is not None)
                and (
                    int(ct)
                    in allowed_by_year.get((int(o), int(y)), frozenset())
                )
                for o, y, ct in zip(oids, yrs, cts)
            ]
            any_role_df = any_role_df.loc[mask].reset_index(drop=True)

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

    # Totals: championships use distinct years; sectionals use sum over years of distinct
    # competition counts (same-bucket other roles only).
    base_off = pd.DataFrame({"official_id": official_ids}).drop_duplicates()
    if is_sectionals_detailed:
        if any_role_df.empty:
            total_any = base_off.assign(total_any_championships=0)
        else:
            per_y = (
                any_role_df.groupby(["official_id", "year"])["competition_id"]
                .nunique()
                .reset_index(name="_n")
            )
            summed = (
                per_y.groupby("official_id")["_n"]
                .sum()
                .reset_index(name="total_any_championships")
            )
            total_any = base_off.merge(summed, on="official_id", how="left").fillna(
                {"total_any_championships": 0}
            )
        total_any["total_any_championships"] = (
            pd.to_numeric(total_any["total_any_championships"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
    else:
        total_any = (
            any_role_df.groupby("official_id")["year"]
            .nunique()
            .reset_index(name="total_any_championships")
        )
        total_any = base_off.merge(total_any, on="official_id", how="left").fillna(
            {"total_any_championships": 0}
        )
        total_any["total_any_championships"] = (
            pd.to_numeric(total_any["total_any_championships"], errors="coerce")
            .fillna(0)
            .astype(int)
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
                selected_symbol = f"{_chief_lower_prefix(key)}{SELECTED_ROLE_SYMBOL}"
                other = [(r, d) for r, d in entries if r != selected_appt_type]
                if other:
                    abbrevs = ", ".join(_role_label(r, d, discipline_id) for r, d in other)
                    return f"{selected_symbol} {OTHER_ROLE_SYMBOL}{abbrevs}{_region_suffix(key)}"
                return f"{selected_symbol}{_region_suffix(key)}"
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
                sym = f"{_chief_lower_prefix(key)}{SELECTED_ROLE_SYMBOL}"
                return f"{sym}{_region_suffix(key)}"
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
    rename["chief_recent_year"] = MOST_RECENT_CHIEF_COL
if is_sectionals_detailed and "appointment_level" in display.columns:
    rename["appointment_level"] = "Level"

display = display.rename(columns={k: v for k, v in rename.items() if k in display.columns})

if (
    is_championships_detailed
    and national_appointment_type_has_isu_mapping(appointment_type_id)
    and "official_id" in display.columns
    and "Name" in display.columns
):
    _oids = display["official_id"].dropna().astype(int).unique().tolist()
    _isu = get_official_ids_with_isu_appointment(
        _oids,
        appointment_type_id,
        discipline_id,
        active_appointments_only=active_appointments_only,
    )
    display["ISU"] = display["official_id"].map(
        lambda x: "Yes" if pd.notna(x) and int(x) in _isu else "No"
    )
    _cols = list(display.columns)
    _cols.remove("ISU")
    _cols.insert(_cols.index("Name") + 1, "ISU")
    display = display[_cols]

if is_sectionals_detailed and "Level" in display.columns and "Name" in display.columns:
    _cols = list(display.columns)
    _cols.remove("Level")
    _cols.insert(_cols.index("Name") + 1, "Level")
    display = display[_cols]

# Drop internal eligibility column for sectionals; championships get a display metric instead.
if is_sectionals_detailed:
    display = display.drop(columns=["eligible_years"], errors="ignore")

if is_championships_detailed and "eligible_years" in display.columns:
    if "# In Role" not in display.columns:
        display = display.drop(columns=["eligible_years"], errors="ignore")
    else:
        te = pd.to_numeric(display["# In Role"], errors="coerce").fillna(0)
        el = pd.to_numeric(display["eligible_years"], errors="coerce")
        valid = el.notna() & (el > 0)
        display[PCT_ELIGIBLE_ATTENDED_COL] = (
            (100.0 * te / el).where(valid).clip(upper=100)
        )
        display = display.drop(columns=["eligible_years"])
        _cols = [c for c in display.columns if c != PCT_ELIGIBLE_ATTENDED_COL]
        if PCT_ELIGIBLE_ATTENDED_COL in display.columns:
            _cols.insert(_cols.index("# In Role") + 1, PCT_ELIGIBLE_ATTENDED_COL)
        display = display[_cols]

if show_other_roles and "# Total" in display.columns and "# In Role" in display.columns:
    _cols = list(display.columns)
    _cols.remove("# Total")
    if PCT_ELIGIBLE_ATTENDED_COL in _cols:
        _insert_at = _cols.index(PCT_ELIGIBLE_ATTENDED_COL) + 1
    else:
        _insert_at = _cols.index("# In Role") + 1
    _cols.insert(_insert_at, "# Total")
    display = display[_cols]

if MOST_RECENT_CHIEF_COL in display.columns:
    _cols = [c for c in display.columns if c != MOST_RECENT_CHIEF_COL]
    if "Most Recent" in _cols:
        _cols.insert(_cols.index("Most Recent") + 1, MOST_RECENT_CHIEF_COL)
    elif "Name" in _cols:
        _cols.insert(_cols.index("Name") + 1, MOST_RECENT_CHIEF_COL)
    else:
        _cols.append(MOST_RECENT_CHIEF_COL)
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
if MOST_RECENT_CHIEF_COL in display.columns:
    _v = pd.to_numeric(display[MOST_RECENT_CHIEF_COL], errors="coerce")
    # No chief / never: show empty (not 0) in the table
    display[MOST_RECENT_CHIEF_COL] = _v.mask(_v.isna() | (_v == 0), other=pd.NA).astype(
        "Int64"
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
if is_championships_detailed:
    default_sort = [
        c for c in ("Yrs Since Last", "Yrs in Grade") if c in sortable_cols
    ]
else:
    default_sort = [c for c in ("Yrs Since Last", "Yrs in Grade") if c in sortable_cols]
sort_cols = st.multiselect(
    "Sort by (priority left to right)",
    options=sortable_cols,
    default=default_sort,
    help="Choose multiple columns to sort. First selection has highest priority. "
    f"{PCT_ELIGIBLE_ATTENDED_COL} sorts by the numeric percent (higher first when Descending).",
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
    display = display.sort_values(
        by=sort_cols,
        ascending=sort_orders,
        kind="mergesort",
        na_position="last",
    )

# Championships: light gray in year cells in the appointment year or earlier (calendar year of
# national appointment; same-year championships count since they were not yet appointed).
# (`achieved_year` comes from the activity matrix; dropped before render.)
champs_pre_appointment = None
if is_championships_detailed and "achieved_year" in display.columns:
    _ystrs = [str(c) for c in year_cols_sorted if str(c) in display.columns]
    A = pd.to_numeric(display["achieved_year"], errors="coerce")
    if _ystrs and A is not None:
        champs_pre_appointment = pd.DataFrame(
            {yc: (int(yc) <= A) & A.notna() for yc in _ystrs},
            index=display.index,
        )
    display = display.drop(columns=["achieved_year"], errors="ignore")
elif "achieved_year" in display.columns:
    display = display.drop(columns=["achieved_year"], errors="ignore")

# Streamlit NumberColumn shows pandas NA as the word "none"; use blank strings + TextColumn.
if MOST_RECENT_CHIEF_COL in display.columns:
    def _chief_year_display_cell(x):
        if x is None:
            return ""
        try:
            if pd.isna(x):
                return ""
        except TypeError:
            pass
        try:
            i = int(x)
        except (TypeError, ValueError):
            return ""
        return "" if i <= 0 else str(i)

    display[MOST_RECENT_CHIEF_COL] = display[MOST_RECENT_CHIEF_COL].map(
        _chief_year_display_cell
    )

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
if show_other_roles:
    fmt_map["Last (Any Role)"] = _fmt_hide_low
    fmt_map["Yrs Since (Any)"] = _fmt_hide_high


def _fmt_pct_eligible(val):
    return "" if pd.isna(val) else f"{int(round(float(val)))}%"


if PCT_ELIGIBLE_ATTENDED_COL in display.columns:
    fmt_map[PCT_ELIGIBLE_ATTENDED_COL] = _fmt_pct_eligible

fmt_map_filtered = {k: v for k, v in fmt_map.items() if k in display.columns}
styled = display.style.format(fmt_map_filtered, na_rep="")

if champs_pre_appointment is not None and not champs_pre_appointment.empty:
    _shade_cols = [c for c in champs_pre_appointment.columns if c in display.columns]

    def _shade_championships_pre_appointment_year_col(s):
        col = s.name
        if col not in champs_pre_appointment.columns:
            return [""] * len(s)
        return [
            "background-color: #ececec" if bool(champs_pre_appointment.loc[idx, col]) else ""
            for idx in s.index
        ]

    if _shade_cols:
        styled = styled.apply(
            _shade_championships_pre_appointment_year_col, axis=0, subset=_shade_cols
        )

# ---- RENDER ----
col_cfg = {"Name": st.column_config.Column(pinned=True)}
if is_championships_detailed and "ISU" in display.columns:
    col_cfg["ISU"] = st.column_config.TextColumn(
        "ISU",
        width="small",
        help=(
            "Whether this official has an active international (ISU) **appointment type** "
            "that matches this report: International Judge for Competition Judge, "
            "International Referee / ITS / ITC for the same national role, or "
            "**International Data / Video Operator in any discipline** when the report role is Data Operator. "
            "Any directory **level** qualifies for the international row. "
            "Discipline otherwise matches the filter (including Singles/Pairs)."
        ),
    )
if is_sectionals_detailed and "Level" in display.columns:
    col_cfg["Level"] = st.column_config.TextColumn(
        "Level",
        help="USFS appointment level for the earliest qualifying national or sectional history row in this report.",
    )
if show_section_info:
    if "Region" in display.columns:
        col_cfg["Region"] = st.column_config.TextColumn("Region")
    if "Section" in display.columns:
        col_cfg["Section"] = st.column_config.TextColumn("Section")
if appt_has_chiefs and MOST_RECENT_CHIEF_COL in display.columns:
    col_cfg[MOST_RECENT_CHIEF_COL] = st.column_config.TextColumn(
        MOST_RECENT_CHIEF_COL,
        width="small",
        help=(
            "Most recent **calendar year** with **chief** on an assignment in this report’s "
            "official type, discipline, and competition scope (same rules as the **C** prefix on year cells)."
        ),
    )
if "# In Role" in display.columns:
    col_cfg["# In Role"] = st.column_config.NumberColumn(
        "# In Role",
        help=(
            "Per calendar year, how many distinct sectional competitions in the selected role; "
            "summed across years (singles/pairs & dance sectionals and synchro sectionals share the same year columns)."
            if is_sectionals_detailed
            else (
                "Years assigned in the selected official type at this championship (once per year); "
                "the appointment calendar year is excluded (not eligible to have attended that year's event)."
                if is_championships_detailed
                else "Years assigned in the selected official type at this championship (once per year)"
            )
        ),
        format="%.0f",
    )
if is_championships_detailed and PCT_ELIGIBLE_ATTENDED_COL in display.columns:
    col_cfg[PCT_ELIGIBLE_ATTENDED_COL] = st.column_config.NumberColumn(
        PCT_ELIGIBLE_ATTENDED_HEADER,
        width="small",
        help=(
            "Percent of eligible calendar years—**starting the year after** your national appointment "
            "(same appointment year is not eligible)—and not before the earliest year in this data, "
            "in which you were assigned in this role at the selected championship(s). "
            "Years in **# In Role** exclude the appointment year for the same reason."
        ),
        format="%.0f%%",
    )
if show_other_roles and "# Total" in display.columns:
    col_cfg["# Total"] = st.column_config.NumberColumn(
        "# Total",
        help=(
            "Per calendar year, distinct sectional competitions in any role, same macro "
            "section family as your selected-role work that year (summed across years)."
            if is_sectionals_detailed
            else "Distinct years at this championship type counting any appointment type / discipline"
        ),
        format="%.0f",
    )

_level_scope = "Sectional and national" if is_sectionals_detailed else "National level"
_appt_scope = "active directory appointments only" if active_appointments_only else "including inactive directory rows"
st.markdown(
    f"**{len(display)} officials** — {_level_scope} · {selected_appt_type} · {selected_discipline} · {_appt_scope}"
)
st.dataframe(styled, width="stretch", height=700, hide_index=True, column_config=col_cfg)
if show_section_info and "Section" in display.columns:
    sec_counts = display["Section"].value_counts().sort_index()
    st.caption(
        "By section (this view): "
        + " · ".join(f"{k}: {int(v)}" for k, v in sec_counts.items())
    )
legend_parts = []
if appt_has_chiefs:
    legend_parts.append("C = Chief in selected role")
if report_mode in (REPORT_CHAMPIONSHIPS_DETAILED, REPORT_SECTIONALS_DETAILED):
    legend_parts.append(
        "L = In selected role that year, every assignment is lower-levels only"
    )
if is_championships_detailed:
    legend_parts.append(
        "Gray = Championship year is on or before your appointment year (this role & report)"
    )
if is_championships_detailed:
    legend_parts.append(
        f"{PCT_ELIGIBLE_ATTENDED_COL} = attended ÷ eligible yrs (first eligible yr = year after appointment; aligned with data start)"
    )
legend_parts.append(f"{SELECTED_ROLE_SYMBOL} = Assigned in selected role")
if show_other_roles:
    legend_parts.append(f"{OTHER_ROLE_SYMBOL} = Present in other role(s)")
st.caption("Legend: " + " · ".join(legend_parts))

appt_date = load_appt_data_date()
if appt_date is not None:
    date_str = pd.Timestamp(appt_date).strftime("%-m/%-d/%Y")
    st.caption(f"Appointment data current as of {date_str}")
