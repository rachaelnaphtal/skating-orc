"""
International officials activity tracker — panel work at ISU / international
competitions matched to directory international appointments.

Run:
    streamlit run activityAnalysis/international_officials_app.py
"""

from __future__ import annotations

import os
import sys

import pandas as pd
import streamlit as st

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from database import ensure_database_for_streamlit

ensure_database_for_streamlit()

from app_query_params import (
    apply_bool_param,
    apply_int_select_param,
    mark_query_params_applied,
    qp_get,
    query_params_changed,
    sync_query_params,
)
from activityAnalysis.international_officials_data import (
    COUNTABLE_SEGMENT_LEVELS,
    DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL,
    INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
    _nullable_int_for_sql,
    get_international_appointment_type_options,
    get_international_discipline_options,
    get_international_official_activity_detail,
    get_international_official_activity_summary,
    get_international_officials_for_filters,
    load_international_panel_segments_bulk,
)
from activityAnalysis.international_listing_seasons import (
    REPORT_LISTING_SEASON_DEFAULT,
    REPORT_LISTING_SEASON_OPTIONS,
    REPORT_SEASON_WINDOW_DEFAULT,
    REPORT_SEASON_WINDOW_OPTIONS,
    format_usfs_season_code,
    season_codes_preceding_listing,
)
from activityAnalysis.international_officials_detail import (
    appointment_detail_url,
    clear_appointment_detail_params,
    parse_appointment_detail_params,
    render_appointment_detail_report,
)
from activityAnalysis.international_officials_report import (
    build_bulk_appointment_reports_zip,
    bulk_reports_zip_filename,
)
from activityAnalysis.international_requirements import evaluate_requirements_summary_df
from activityAnalysis.load_activity_data import activity_database_is_postgresql, get_engine
from activityAnalysis.officials_analysis_models import Appointments
from sqlalchemy import func as sqlfunc, select
from sqlalchemy.orm import Session

st.set_page_config(
    page_title="International Officials Activity",
    page_icon="🌍",
    layout="wide",
)

_CACHE_TTL_SEC = 120
_ALL_LABEL = "(All)"
_INTL_QP_FLAG = "_intl_officials_qp_tuple"


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_appointment_type_options():
    return get_international_appointment_type_options()


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_discipline_options(
    appointment_type_id: int | None,
    active_only: bool,
):
    return get_international_discipline_options(
        appointment_type_id=appointment_type_id,
        active_appointments_only=active_only,
    )


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_official_options(
    appointment_type_id: int | None,
    discipline_id: int | None,
    active_only: bool,
):
    df = get_international_officials_for_filters(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        active_appointments_only=active_only,
    )
    if df.empty:
        return df
    return (
        df[["official_id", "official_name"]]
        .drop_duplicates(subset=["official_id"])
        .sort_values("official_name", na_position="last")
        .reset_index(drop=True)
    )


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_summary(
    appointment_type_id: int | None,
    discipline_id: int | None,
    official_id: int | None,
    active_only: bool,
    include_requirements: bool,
    listing_season_code: int,
    report_season_window: int,
):
    """Summary table: one panel query, shared appointments list."""
    report_season_codes = season_codes_preceding_listing(
        listing_season_code, report_season_window
    )
    appointments = get_international_officials_for_filters(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_only,
    )
    if appointments.empty:
        return get_international_official_activity_summary(
            appointment_type_id=appointment_type_id,
            discipline_id=discipline_id,
            official_id=official_id,
            active_appointments_only=active_only,
            season_codes=report_season_codes,
        ), report_season_codes

    official_ids = appointments["official_id"].astype(int).unique().tolist()
    panel = load_international_panel_segments_bulk(
        official_ids, season_codes=report_season_codes
    )
    summary = get_international_official_activity_summary(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_only,
        appointments=appointments,
        panel_bulk=panel,
        season_codes=report_season_codes,
    )
    if include_requirements and not summary.empty:
        summary = evaluate_requirements_summary_df(
            summary,
            panel_bulk=panel,
            listing_season_code=listing_season_code,
        )
    return summary, report_season_codes


def _build_bulk_appointment_reports_zip(
    summary: pd.DataFrame,
    report_season_codes: list[int],
    *,
    listing_season_code: int,
    report_season_window: int,
    active_only: bool,
) -> tuple[bytes, str]:
    """One PDF per summary row, packaged as a ZIP (requirements always included)."""
    zip_name = bulk_reports_zip_filename(
        listing_season_code=listing_season_code,
        report_season_window=report_season_window,
    )
    if summary.empty:
        return b"", zip_name

    official_ids = summary["official_id"].astype(int).unique().tolist()
    panel = load_international_panel_segments_bulk(
        official_ids, season_codes=report_season_codes
    )
    zip_bytes = build_bulk_appointment_reports_zip(
        summary,
        listing_season_code=listing_season_code,
        report_season_codes=report_season_codes,
        report_season_window=report_season_window,
        active_only=active_only,
        panel_bulk=panel,
    )
    return zip_bytes, zip_name


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_segment_detail(
    appointment_type_id: int | None,
    discipline_id: int | None,
    official_id: int | None,
    active_only: bool,
    listing_season_code: int,
    report_season_window: int,
):
    """Segment-level detail table (loaded on demand)."""
    report_season_codes = season_codes_preceding_listing(
        listing_season_code, report_season_window
    )
    appointments = get_international_officials_for_filters(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_only,
    )
    if appointments.empty:
        return get_international_official_activity_detail(
            appointment_type_id=appointment_type_id,
            discipline_id=discipline_id,
            official_id=official_id,
            active_appointments_only=active_only,
            season_codes=report_season_codes,
        )

    official_ids = appointments["official_id"].astype(int).unique().tolist()
    panel = load_international_panel_segments_bulk(
        official_ids, season_codes=report_season_codes
    )
    return get_international_official_activity_detail(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_only,
        appointments=appointments,
        panel_bulk=panel,
        season_codes=report_season_codes,
    )


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_detail_nav_appointments(active_only: bool):
    return get_international_officials_for_filters(
        active_appointments_only=active_only,
    )


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_appointment_data_date():
    with Session(get_engine()) as session:
        return session.execute(select(sqlfunc.max(Appointments.achieved_date))).scalar()


def _sentinel(value, *, all_label: str = _ALL_LABEL) -> int | None:
    if value is None or value == all_label:
        return None
    return int(value)


def _summary_row_for_appointment(
    summary: pd.DataFrame,
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: int | None,
) -> pd.Series | None:
    if summary.empty:
        return None
    matches = summary.loc[
        (summary["official_id"] == int(official_id))
        & (summary["appointment_type_id"] == int(appointment_type_id))
    ]
    if matches.empty:
        return None
    disc_vals = matches["discipline_id"].apply(_nullable_int_for_sql)
    if discipline_id is None:
        matches = matches.loc[disc_vals.isna()]
    else:
        matches = matches.loc[disc_vals == int(discipline_id)]
    if matches.empty:
        return None
    return matches.iloc[0]


def _render_appointment_detail_from_query_params() -> bool:
    """If URL points at an appointment report, render it and return True."""
    params = parse_appointment_detail_params()
    if not params:
        return False

    listing_season_code = int(
        st.session_state.get("intl_listing_season")
        or params["listing_season_code"]
        or REPORT_LISTING_SEASON_DEFAULT
    )
    report_season_window = int(
        st.session_state.get("intl_report_season_window")
        or params["report_season_window"]
        or REPORT_SEASON_WINDOW_DEFAULT
    )
    active_only = bool(
        st.session_state.get("intl_active_only", params["active_only"])
    )
    summary, report_season_codes = _load_summary(
        params["appointment_type_id"],
        params["discipline_id"],
        params["official_id"],
        active_only,
        False,
        listing_season_code,
        report_season_window,
    )
    row = _summary_row_for_appointment(
        summary,
        official_id=params["official_id"],
        appointment_type_id=params["appointment_type_id"],
        discipline_id=params["discipline_id"],
    )
    if row is None:
        st.error("Could not find this appointment in the current data.")
        if st.button("← Back to summary", type="secondary"):
            clear_appointment_detail_params()
            st.rerun()
        return True

    official_ids = [int(params["official_id"])]
    panel = load_international_panel_segments_bulk(
        official_ids, season_codes=report_season_codes
    )
    nav_appointments = _load_detail_nav_appointments(params["active_only"])
    render_appointment_detail_report(
        summary_row=row,
        listing_season_code=listing_season_code,
        report_season_window=report_season_window,
        report_season_codes=report_season_codes,
        active_only=active_only,
        panel_bulk=panel,
        nav_appointments=nav_appointments,
    )
    appt_date = _load_appointment_data_date()
    if appt_date is not None:
        st.caption(
            f"Directory appointment data current as of {pd.Timestamp(appt_date):%-m/%-d/%Y}"
        )
    return True


if not activity_database_is_postgresql():
    st.warning(
        "This app requires PostgreSQL with ``public.segment_official`` and international "
        "competition data. Configure ``DATABASE_URL`` / ``.streamlit/secrets.toml`` "
        "(``USE_CLOUD_DATABASE``, ``CLOUD_DATABASE_URL``) like the judging analysis app."
    )
    st.stop()

if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

_on_detail_page = qp_get("view") == "appointment"

if query_params_changed(_INTL_QP_FLAG):
    apply_int_select_param(
        "listing", "intl_listing_season", REPORT_LISTING_SEASON_OPTIONS
    )
    apply_int_select_param(
        "seasons", "intl_report_season_window", REPORT_SEASON_WINDOW_OPTIONS
    )
    apply_bool_param("active", "intl_active_only")
    if qp_get("req") is not None:
        apply_bool_param("req", "intl_include_requirements")
    mark_query_params_applied(_INTL_QP_FLAG)

if "intl_listing_season" not in st.session_state:
    st.session_state["intl_listing_season"] = REPORT_LISTING_SEASON_DEFAULT
if "intl_report_season_window" not in st.session_state:
    st.session_state["intl_report_season_window"] = REPORT_SEASON_WINDOW_DEFAULT
if "intl_active_only" not in st.session_state:
    st.session_state["intl_active_only"] = (
        qp_get("active") != "0" if qp_get("active") is not None else True
    )
if "intl_include_requirements" not in st.session_state:
    if _on_detail_page:
        st.session_state["intl_include_requirements"] = True
    elif qp_get("req") is not None:
        st.session_state["intl_include_requirements"] = qp_get("req") == "1"
    else:
        st.session_state["intl_include_requirements"] = False

active_only = st.sidebar.checkbox(
    "Active appointments only",
    key="intl_active_only",
    help="When checked, only directory appointments with active = true are included.",
)

include_requirements = st.sidebar.checkbox(
    "Include ISU maintain / promote checks",
    key="intl_include_requirements",
    disabled=_on_detail_page,
    help=(
        "Adds Maintain and Promote columns on the summary list (slower). "
        "Appointment detail pages always show full requirement breakdowns."
        if not _on_detail_page
        else "Maintain and promote checks are always shown on appointment detail pages."
    ),
)

listing_season_code = st.sidebar.selectbox(
    "Listing season",
    options=list(REPORT_LISTING_SEASON_OPTIONS),
    key="intl_listing_season",
    format_func=lambda c: f"{format_usfs_season_code(c)} ({c})",
    help="ISU listing cycle anchor. Service seasons are the USFS seasons immediately before this one.",
)

report_season_window = st.sidebar.selectbox(
    "Seasons in report",
    options=list(REPORT_SEASON_WINDOW_OPTIONS),
    key="intl_report_season_window",
    help="Filter competition/segment counts and detail to this many seasons before the listing season.",
)

report_season_codes = season_codes_preceding_listing(listing_season_code, report_season_window)
st.sidebar.caption(
    "Report seasons: "
    + ", ".join(format_usfs_season_code(c) for c in report_season_codes)
)

sync_query_params(
    listing=str(int(listing_season_code)),
    seasons=str(int(report_season_window)),
    req="1" if include_requirements else "0",
    active="1" if active_only else "0",
)

if _render_appointment_detail_from_query_params():
    st.stop()

st.title("International Officials Activity")
st.caption(
    "Tracks panel assignments at ISU / international competitions and "
    "US **qualifying** competitions (excluding adult/collegiate). "
    f"Only **{'** and **'.join(sorted(COUNTABLE_SEGMENT_LEVELS))}** segments count toward activity and requirements. "
    f"**International Data / Video Operator** combines SPD and Synchronized appointments "
    f"and all Singles, Pairs, Dance, and Synchronized panel work. "
    "Maintain/promote checks use each rule's season window before the selected listing season; "
    "seminars, examinations, and ISU Communications inclusion are not automated. "
    "Use **Open** in the summary table for a full appointment report."
)

appt_df = _load_appointment_type_options()

appt_options = [_ALL_LABEL] + appt_df["appointment_type_id"].astype(int).tolist()
appt_labels = {_ALL_LABEL: _ALL_LABEL}
for row in appt_df.itertuples(index=False):
    appt_labels[int(row.appointment_type_id)] = row.appointment_type

col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    pick_appt = st.selectbox(
        "Appointment type",
        options=appt_options,
        format_func=lambda x: appt_labels.get(x, str(x)),
        index=0,
    )

appt_id = _sentinel(pick_appt)
idvo_only = appt_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID

disc_df = _load_discipline_options(appt_id, active_only)
disc_options = [_ALL_LABEL]
disc_labels = {_ALL_LABEL: _ALL_LABEL}
if not idvo_only and not disc_df.empty:
    disc_options.extend(disc_df["discipline_id"].astype(int).tolist())
    for row in disc_df.itertuples(index=False):
        disc_labels[int(row.discipline_id)] = (
            row.discipline or f"Discipline {row.discipline_id}"
        )

with col_f2:
    if idvo_only:
        st.selectbox(
            "Discipline",
            options=[_ALL_LABEL],
            index=0,
            disabled=True,
            help=(
                f"IDVO appointments are combined as "
                f"“{DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL}”."
            ),
        )
        pick_disc = _ALL_LABEL
    else:
        pick_disc = st.selectbox(
            "Discipline",
            options=disc_options,
            format_func=lambda x: disc_labels.get(x, str(x)),
            index=0,
            help="Only disciplines with at least one international appointment.",
        )

disc_id = _sentinel(pick_disc)

officials_df = _load_official_options(appt_id, disc_id, active_only)
with col_f3:
    if officials_df.empty:
        pick_official = _ALL_LABEL
        st.selectbox("Official", options=[_ALL_LABEL], index=0, disabled=True)
    else:
        off_options = [_ALL_LABEL] + officials_df["official_id"].astype(int).tolist()
        off_labels = {_ALL_LABEL: _ALL_LABEL}
        for row in officials_df.itertuples(index=False):
            off_labels[int(row.official_id)] = (
                (row.official_name or "").strip() or f"Id {row.official_id}"
            )
        pick_official = st.selectbox(
            "Official",
            options=off_options,
            format_func=lambda x: off_labels.get(x, str(x)),
            index=0,
        )

official_id = _sentinel(pick_official)

with st.spinner("Loading activity..."):
    summary, report_season_codes = _load_summary(
        appt_id,
        disc_id,
        official_id,
        active_only,
        include_requirements,
        int(listing_season_code),
        int(report_season_window),
    )

if summary.empty:
    st.info("No international appointments match the current filters.")
    st.stop()

total_officials = summary["official_id"].nunique()
total_appointments = len(summary)
total_competitions = int(summary["competition_count"].sum())
total_segments = int(summary["segment_count"].sum())

m1, m2, m3, m4 = st.columns(4)
m1.metric("Officials", total_officials)
m2.metric("Appointment rows", total_appointments)
m3.metric("Competitions (panel)", total_competitions)
m4.metric("Segments (panel)", total_segments)

st.subheader("Summary by appointment")
dl_col, _ = st.columns([2, 5])
with dl_col:
    bulk_zip_key = (
        appt_id,
        disc_id,
        official_id,
        active_only,
        int(listing_season_code),
        int(report_season_window),
        total_appointments,
    )
    if st.session_state.get("intl_bulk_zip_key") != bulk_zip_key:
        st.session_state.pop("intl_bulk_zip", None)
        st.session_state.pop("intl_bulk_zip_name", None)

    try:
        if st.button(
            "Download all PDFs (ZIP)",
            type="primary",
            use_container_width=True,
            help=(
                f"Build one PDF per appointment row ({total_appointments} report"
                f"{'s' if total_appointments != 1 else ''}). "
                "Includes full requirement breakdowns."
            ),
        ):
            with st.spinner("Building PDF reports…"):
                zip_bytes, zip_name = _build_bulk_appointment_reports_zip(
                    summary,
                    report_season_codes,
                    listing_season_code=int(listing_season_code),
                    report_season_window=int(report_season_window),
                    active_only=active_only,
                )
            if zip_bytes:
                st.session_state["intl_bulk_zip"] = zip_bytes
                st.session_state["intl_bulk_zip_name"] = zip_name
                st.session_state["intl_bulk_zip_key"] = bulk_zip_key
            else:
                st.session_state.pop("intl_bulk_zip", None)
                st.session_state.pop("intl_bulk_zip_name", None)
                st.session_state.pop("intl_bulk_zip_key", None)

        zip_bytes = st.session_state.get("intl_bulk_zip")
        zip_name = st.session_state.get("intl_bulk_zip_name")
        if (
            zip_bytes
            and zip_name
            and st.session_state.get("intl_bulk_zip_key") == bulk_zip_key
        ):
            st.download_button(
                "Save ZIP file",
                data=zip_bytes,
                file_name=zip_name,
                mime="application/zip",
                use_container_width=True,
                on_click="ignore",
            )
    except RuntimeError as exc:
        st.caption(str(exc))

summary_display = summary.rename(
    columns={
        "official_name": "Official",
        "mbr_number": "Member #",
        "appointment_type": "Appointment",
        "appointment_level": "Level",
        "discipline": "Discipline",
        "competition_count": "Competitions",
        "segment_count": "Segments",
        "maintain": "Maintain",
        "maintain_note": "Maintain detail",
        "promote": "Promote",
        "promote_note": "Promote detail",
    }
)
summary_display["Report"] = summary.apply(
    lambda r: appointment_detail_url(
        official_id=int(r["official_id"]),
        appointment_type_id=int(r["appointment_type_id"]),
        discipline_id=r.get("discipline_id"),
        listing_season_code=int(listing_season_code),
        report_season_window=int(report_season_window),
        active_only=active_only,
    ),
    axis=1,
)
summary_display = summary_display.drop(
    columns=["official_id", "appointment_type_id", "discipline_id"], errors="ignore"
)

show_cols = [
    c
    for c in [
        "Official",
        "Member #",
        "Appointment",
        "Level",
        "Discipline",
        "Competitions",
        "Segments",
        *(
            [
                "Maintain",
                "Maintain detail",
                "Promote",
                "Promote detail",
            ]
            if include_requirements
            else []
        ),
        "Report",
    ]
    if c in summary_display.columns
]

st.dataframe(
    summary_display[show_cols],
    width="stretch",
    hide_index=True,
    column_config={
        "Official": st.column_config.TextColumn("Official", width="medium"),
        "Member #": st.column_config.TextColumn("Member #", width="small"),
        "Appointment": st.column_config.TextColumn("Appointment", width="medium"),
        "Level": st.column_config.TextColumn(
            "Level",
            help="Directory appointment level (International or ISU).",
            width="small",
        ),
        "Discipline": st.column_config.TextColumn("Discipline", width="small"),
        "Competitions": st.column_config.NumberColumn(
            "Competitions",
            help=(
                "Distinct international or national qualifying competitions with at least one "
                f"matching Junior/Senior segment in seasons "
                f"{', '.join(format_usfs_season_code(c) for c in report_season_codes)}."
            ),
        ),
        "Segments": st.column_config.NumberColumn(
            "Segments",
            help=(
                "Distinct Junior/Senior segments on panel in the selected role and discipline "
                f"for seasons {', '.join(format_usfs_season_code(c) for c in report_season_codes)}."
            ),
        ),
        "Maintain": st.column_config.TextColumn(
            "Maintain",
            help="Re-list service requirements for this appointment's level (International or ISU).",
            width="small",
        ),
        "Maintain detail": st.column_config.TextColumn(
            "Maintain detail",
            width="large",
        ),
        "Promote": st.column_config.TextColumn(
            "Promote",
            help="Promotion toward ISU listing (International level appointments only).",
            width="small",
        ),
        "Promote detail": st.column_config.TextColumn(
            "Promote detail",
            width="large",
        ),
        "Report": st.column_config.LinkColumn(
            "Report",
            display_text="Open",
            help="Open a detailed activity and requirements report for this appointment.",
        ),
    },
)

show_segment_detail = st.checkbox(
    "Show all segment detail",
    value=False,
    help="Load every matching segment row for the filtered list (slower).",
)

if show_segment_detail:
    with st.spinner("Loading segment detail..."):
        detail = _load_segment_detail(
            appt_id,
            disc_id,
            official_id,
            active_only,
            int(listing_season_code),
            int(report_season_window),
        )
    st.subheader("All segment detail (filtered list)")
    if detail.empty:
        st.info(
            "No matching panel segments yet for these appointments at international "
            "competitions (Junior/Senior only)."
        )
    else:
        detail_display = detail.rename(
            columns={
                "official_name": "Official",
                "appointment_type": "Appointment",
                "discipline": "Appt discipline",
                "competition_year": "Season",
                "competition_name": "Competition",
                "competition_scope": "Comp scope",
                "competition_type": "Comp type",
                "segment_name": "Segment",
                "segment_level": "Level",
                "segment_discipline": "Segment discipline",
            }
        )
        show_cols = [
            c
            for c in [
                "Official",
                "Appointment",
                "Appt discipline",
                "Season",
                "Competition",
                "Comp scope",
                "Comp type",
                "Segment",
                "Level",
                "Segment discipline",
                "start_date",
                "end_date",
                "results_url",
            ]
            if c in detail_display.columns
        ]
        st.dataframe(
            detail_display[show_cols],
            width="stretch",
            hide_index=True,
            column_config={
                "Official": st.column_config.TextColumn("Official", width="medium"),
                "Competition": st.column_config.TextColumn("Competition", width="large"),
                "Segment": st.column_config.TextColumn("Segment", width="medium"),
                "results_url": st.column_config.LinkColumn("Results", display_text="Open"),
            },
        )

appt_date = _load_appointment_data_date()
if appt_date is not None:
    st.caption(f"Directory appointment data current as of {pd.Timestamp(appt_date):%-m/%-d/%Y}")
