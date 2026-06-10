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
import plotly.express as px
import streamlit as st

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from database import ensure_database_for_streamlit

ensure_database_for_streamlit()

from app_query_params import (
    apply_bool_param,
    apply_choice_param,
    apply_int_select_param,
    mark_query_params_applied,
    qp_get,
    qp_get_int,
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
    get_international_level_options,
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
    age_listing_column_label,
    age_out_column_label,
    calendar_year_for_listing_milestone,
    format_age_out_display,
    format_listing_reference_july1,
    format_promote_first_eligible_display,
    format_usfs_season_code,
    histogram_counts_by_current_age_bins,
    histogram_counts_by_year_bins,
    promote_first_eligible_column_label,
    years_in_grade_listing_column_label,
    season_codes_preceding_listing,
)
from activityAnalysis.international_official_demographics import OFFICIAL_AGE_OUT_ON_JULY1
from activityAnalysis.international_official_demographics import (
    enrich_summary_with_listing_demographics,
)
from activityAnalysis.international_major_events import (
    MAJOR_ISU_EVENT_KEYS,
    MAJOR_ISU_EVENT_LABELS,
    format_major_event_matrix_for_display,
    get_international_major_event_matrix,
    major_event_matrix_legend,
    style_major_event_matrix_display,
)
from activityAnalysis.international_officials_detail import (
    INTL_VIEW_DETAIL,
    INTL_VIEW_MAJOR_EVENTS,
    INTL_VIEW_OPTIONS,
    INTL_VIEW_SUMMARY,
    appointment_detail_url,
    discipline_id_from_param,
    discipline_id_to_param,
    intl_view_mode_from_query_param,
    intl_view_query_slug_for_mode,
    open_last_appointment_detail,
    parse_appointment_detail_params,
    remember_last_appointment_detail,
    render_appointment_detail_report,
    render_detail_appointment_nav,
    switch_to_summary_view,
)
from activityAnalysis.international_officials_report import (
    build_bulk_appointment_reports_zip,
    bulk_reports_zip_filename,
)
from activityAnalysis.international_segment_eligibility import (
    enrich_panel_with_rule411_eligibility,
)
from activityAnalysis.international_requirements import (
    DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
    enrich_summary_with_promote_first_eligible,
    evaluate_requirements_summary_df,
)
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


def _mark_intl_query_params() -> None:
    mark_query_params_applied(_INTL_QP_FLAG)


def _sync_intl_query_params(**params) -> None:
    """Mirror filters to the URL and snapshot them so widget changes are not reverted."""
    sync_query_params(**params)
    _mark_intl_query_params()


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_appointment_type_options():
    return get_international_appointment_type_options()


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_discipline_options(
    appointment_type_id: int | None,
    level_id: int | None,
    active_only: bool,
):
    return get_international_discipline_options(
        appointment_type_id=appointment_type_id,
        level_id=level_id,
        active_appointments_only=active_only,
    )


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_level_options(
    appointment_type_id: int | None,
    discipline_id: int | None,
    active_only: bool,
):
    return get_international_level_options(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        active_appointments_only=active_only,
    )


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_official_options(
    appointment_type_id: int | None,
    discipline_id: int | None,
    level_id: int | None,
    active_only: bool,
):
    df = get_international_officials_for_filters(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        level_id=level_id,
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
    level_id: int | None,
    official_id: int | None,
    active_only: bool,
    include_maintain_promote: bool,
    include_seminar_columns: bool,
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
        level_id=level_id,
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
    if not summary.empty:
        summary = enrich_summary_with_listing_demographics(
            summary, listing_season_code=listing_season_code
        )
        summary = enrich_summary_with_promote_first_eligible(
            summary, listing_season_code=listing_season_code
        )
    if (include_maintain_promote or include_seminar_columns) and not summary.empty:
        summary = evaluate_requirements_summary_df(
            summary,
            panel_bulk=panel if include_maintain_promote else None,
            listing_season_code=listing_season_code,
            include_maintain_promote=include_maintain_promote,
            include_seminar_columns=include_seminar_columns,
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
    level_id: int | None,
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
        level_id=level_id,
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


@st.cache_data(ttl=_CACHE_TTL_SEC)
def _load_major_event_matrix(
    event_key: str,
    appointment_type_id: int | None,
    discipline_id: int | None,
    active_only: bool,
):
    return get_international_major_event_matrix(
        event_key=event_key,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        isu_level_id=DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
        active_appointments_only=active_only,
    )


def _sentinel(value, *, all_label: str = _ALL_LABEL) -> int | None:
    if value is None or value == all_label:
        return None
    return int(value)


def _apply_major_events_filters_from_query() -> None:
    apply_choice_param("event", "intl_major_event_key", MAJOR_ISU_EVENT_KEYS)
    appt_raw = qp_get("appt")
    if appt_raw is not None:
        if appt_raw.strip().lower() == "all":
            st.session_state["intl_major_event_appt"] = _ALL_LABEL
        else:
            appt_id = qp_get_int("appt")
            if appt_id is not None:
                st.session_state["intl_major_event_appt"] = appt_id
    did_raw = qp_get("did")
    if did_raw is not None:
        if did_raw.strip().lower() == "none":
            st.session_state["intl_major_event_disc"] = _ALL_LABEL
        else:
            disc_id = discipline_id_from_param(did_raw)
            if disc_id is not None:
                st.session_state["intl_major_event_disc"] = disc_id


def _major_events_query_params(
    *,
    event_key: str,
    major_appt_id: int | None,
    major_disc_id: int | None,
    active_only: bool,
) -> dict[str, str | None]:
    return {
        "view": intl_view_query_slug_for_mode(INTL_VIEW_MAJOR_EVENTS),
        "active": "1" if active_only else "0",
        "event": event_key,
        "appt": str(major_appt_id) if major_appt_id is not None else "all",
        "did": discipline_id_to_param(major_disc_id),
        "oid": None,
        "atid": None,
        "listing": None,
        "seasons": None,
        "req": None,
        "activity_detail": None,
        "scope_counts": None,
        "sem_maintain": None,
        "sem_promote": None,
        "level": None,
    }


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
        None,
        params["official_id"],
        active_only,
        False,
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
            switch_to_summary_view()
            st.rerun()
        return True

    remember_last_appointment_detail(
        official_id=int(params["official_id"]),
        appointment_type_id=int(params["appointment_type_id"]),
        discipline_id=params["discipline_id"],
    )
    official_ids = [int(params["official_id"])]
    panel = load_international_panel_segments_bulk(
        official_ids, season_codes=report_season_codes
    )
    render_appointment_detail_report(
        summary_row=row,
        listing_season_code=listing_season_code,
        report_season_window=report_season_window,
        report_season_codes=report_season_codes,
        active_only=active_only,
        panel_bulk=panel,
        nav_appointments=_load_detail_nav_appointments(active_only),
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

_intl_url_changed = query_params_changed(_INTL_QP_FLAG)

if _intl_url_changed:
    apply_int_select_param(
        "listing", "intl_listing_season", REPORT_LISTING_SEASON_OPTIONS
    )
    apply_int_select_param(
        "seasons", "intl_report_season_window", REPORT_SEASON_WINDOW_OPTIONS
    )
    apply_bool_param("active", "intl_active_only")
    if qp_get("req") is not None:
        apply_bool_param("req", "intl_include_requirements")
    if qp_get("activity_detail") is not None:
        apply_bool_param("activity_detail", "intl_show_activity_detail")
    elif qp_get("scope_counts") is not None:
        apply_bool_param("scope_counts", "intl_show_activity_detail")
    if qp_get("sem_maintain") is not None:
        apply_bool_param("sem_maintain", "intl_show_seminar_maintain")
    if qp_get("sem_promote") is not None:
        apply_bool_param("sem_promote", "intl_show_seminar_promote")
    level_qp = qp_get_int("level")
    if level_qp is not None:
        st.session_state["intl_level_pick"] = level_qp
    view_from_url = intl_view_mode_from_query_param(qp_get("view"))
    if view_from_url is not None:
        st.session_state["intl_view_mode"] = view_from_url

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
if "intl_show_activity_detail" not in st.session_state:
    if qp_get("activity_detail") is not None:
        st.session_state["intl_show_activity_detail"] = qp_get("activity_detail") == "1"
    elif qp_get("scope_counts") is not None:
        st.session_state["intl_show_activity_detail"] = qp_get("scope_counts") == "1"
    else:
        st.session_state["intl_show_activity_detail"] = False
if "intl_show_seminar_maintain" not in st.session_state:
    st.session_state["intl_show_seminar_maintain"] = (
        qp_get("sem_maintain") == "1" if qp_get("sem_maintain") is not None else False
    )
if "intl_show_seminar_promote" not in st.session_state:
    st.session_state["intl_show_seminar_promote"] = (
        qp_get("sem_promote") == "1" if qp_get("sem_promote") is not None else False
    )
if "intl_major_event_key" not in st.session_state:
    st.session_state["intl_major_event_key"] = MAJOR_ISU_EVENT_KEYS[0]
if "intl_major_event_appt" not in st.session_state:
    st.session_state["intl_major_event_appt"] = _ALL_LABEL
if "intl_major_event_disc" not in st.session_state:
    st.session_state["intl_major_event_disc"] = _ALL_LABEL

view_mode = st.sidebar.radio(
    "View",
    options=list(INTL_VIEW_OPTIONS),
    key="intl_view_mode",
    horizontal=True,
)

_prev_view_mode = st.session_state.get("_intl_prev_view_mode")
if view_mode == INTL_VIEW_MAJOR_EVENTS and _prev_view_mode != INTL_VIEW_MAJOR_EVENTS:
    if qp_get("appt") is None:
        st.session_state["intl_major_event_appt"] = _ALL_LABEL
    if qp_get("did") is None:
        st.session_state["intl_major_event_disc"] = _ALL_LABEL
st.session_state["_intl_prev_view_mode"] = view_mode

_detail_nav_params = parse_appointment_detail_params()
_detail_listing_season = int(st.session_state["intl_listing_season"])
_detail_report_seasons = int(st.session_state["intl_report_season_window"])
_detail_active_only = bool(st.session_state["intl_active_only"])

if view_mode == INTL_VIEW_SUMMARY and _on_detail_page:
    switch_to_summary_view()
    st.rerun()

if view_mode == INTL_VIEW_DETAIL and not _on_detail_page:
    if open_last_appointment_detail(
        listing_season_code=_detail_listing_season,
        report_season_window=_detail_report_seasons,
        active_only=_detail_active_only,
    ):
        st.rerun()

active_only = st.sidebar.checkbox(
    "Active appointments only",
    key="intl_active_only",
    help="When checked, only directory appointments with active = true are included.",
)

if view_mode != INTL_VIEW_MAJOR_EVENTS:
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

    show_activity_detail = st.sidebar.checkbox(
        "Show activity detail",
        key="intl_show_activity_detail",
        disabled=_on_detail_page,
        help=(
            "Adds international vs national competition and segment counts to the summary "
            "table and top metrics. Appointment detail pages always show activity breakdown."
            if not _on_detail_page
            else "Activity counts are always shown on appointment detail pages."
        ),
    )

    show_seminar_maintain = st.sidebar.checkbox(
        "Seminar maintain column",
        key="intl_show_seminar_maintain",
        disabled=_on_detail_page,
        help=(
            "Adds a column showing whether seminar maintain requirements are met. "
            "Empty when no seminar requirement applies."
            if not _on_detail_page
            else "Seminar columns are only shown on the summary table."
        ),
    )

    show_seminar_promote = st.sidebar.checkbox(
        "Seminar promote column",
        key="intl_show_seminar_promote",
        disabled=_on_detail_page,
        help=(
            "Adds a column showing whether seminar promote requirements are met. "
            "Empty when no seminar requirement applies."
            if not _on_detail_page
            else "Seminar columns are only shown on the summary table."
        ),
    )
else:
    include_requirements = False
    show_activity_detail = False
    show_seminar_maintain = False
    show_seminar_promote = False

if view_mode != INTL_VIEW_MAJOR_EVENTS:
    listing_season_code = st.sidebar.selectbox(
        "Listing season",
        options=list(REPORT_LISTING_SEASON_OPTIONS),
        key="intl_listing_season",
        format_func=lambda c: f"{format_usfs_season_code(c)} ({c})",
        help=(
            "ISU listing cycle anchor. Service seasons are the USFS seasons immediately before this one. "
            "Age and years in grade use the listing reference date "
            f"(e.g. {format_listing_reference_july1(2627)} for 26-27)."
        ),
    )

    report_season_window = st.sidebar.selectbox(
        "Seasons in report",
        options=list(REPORT_SEASON_WINDOW_OPTIONS),
        key="intl_report_season_window",
        help="Filter competition/segment counts and detail to this many seasons before the listing season.",
    )

    report_season_codes = season_codes_preceding_listing(
        listing_season_code, report_season_window
    )
    st.sidebar.caption(
        "Report seasons: "
        + ", ".join(format_usfs_season_code(c) for c in report_season_codes)
    )
    st.sidebar.caption(
        f"Age / years in grade reference: {format_listing_reference_july1(listing_season_code)}"
    )

    _sync_intl_query_params(
        view=intl_view_query_slug_for_mode(INTL_VIEW_SUMMARY),
        listing=str(int(listing_season_code)),
        seasons=str(int(report_season_window)),
        req="1" if include_requirements else "0",
        active="1" if active_only else "0",
        activity_detail="1" if show_activity_detail else "0",
        sem_maintain="1" if show_seminar_maintain else "0",
        sem_promote="1" if show_seminar_promote else "0",
        oid=None,
        atid=None,
        event=None,
        appt=None,
        did=None,
    )
else:
    listing_season_code = int(st.session_state.get("intl_listing_season", REPORT_LISTING_SEASON_DEFAULT))
    report_season_window = int(
        st.session_state.get("intl_report_season_window", REPORT_SEASON_WINDOW_DEFAULT)
    )
    report_season_codes = season_codes_preceding_listing(
        listing_season_code, report_season_window
    )

if view_mode == INTL_VIEW_DETAIL and not _on_detail_page:
    _sync_intl_query_params(
        view=intl_view_query_slug_for_mode(INTL_VIEW_DETAIL),
        active="1" if active_only else "0",
        oid=None,
        atid=None,
        did=None,
        listing=None,
        seasons=None,
        req=None,
        event=None,
        appt=None,
    )
    st.title("International Officials Activity")
    st.caption("Choose an appointment to open its detail report.")
    render_detail_appointment_nav(
        nav_appointments=_load_detail_nav_appointments(_detail_active_only),
        listing_season_code=_detail_listing_season,
        report_season_window=_detail_report_seasons,
        active_only=_detail_active_only,
        picker_only=True,
    )
    st.stop()

if view_mode == INTL_VIEW_DETAIL and _render_appointment_detail_from_query_params():
    st.stop()

if view_mode == INTL_VIEW_MAJOR_EVENTS:
    st.title("Major ISU Events Activity")
    st.caption(
        "Calendar-year matrix of panel service at ISU Championship-tier events "
        "(Worlds, Olympics, Junior Worlds, Four Continents, Europeans) and the Grand Prix Final."
    )

    appt_df = _load_appointment_type_options()
    appt_options = [_ALL_LABEL] + [
        int(x)
        for x in appt_df["appointment_type_id"].astype(int).tolist()
        if int(x) != INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID
    ]
    appt_labels = {_ALL_LABEL: _ALL_LABEL}
    for row in appt_df.itertuples(index=False):
        if int(row.appointment_type_id) != INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
            appt_labels[int(row.appointment_type_id)] = row.appointment_type

    disc_df = _load_discipline_options(None, DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP, active_only)
    disc_options = [_ALL_LABEL]
    disc_labels = {_ALL_LABEL: _ALL_LABEL}
    if not disc_df.empty:
        disc_options.extend(disc_df["discipline_id"].astype(int).tolist())
        for row in disc_df.itertuples(index=False):
            disc_labels[int(row.discipline_id)] = (
                row.discipline or f"Discipline {row.discipline_id}"
            )

    if _intl_url_changed:
        _apply_major_events_filters_from_query()
    else:
        _sync_intl_query_params(
            **_major_events_query_params(
                event_key=st.session_state.get(
                    "intl_major_event_key", MAJOR_ISU_EVENT_KEYS[0]
                ),
                major_appt_id=_sentinel(
                    st.session_state.get("intl_major_event_appt", _ALL_LABEL)
                ),
                major_disc_id=_sentinel(
                    st.session_state.get("intl_major_event_disc", _ALL_LABEL)
                ),
                active_only=active_only,
            )
        )

    col_e, col_a, col_d = st.columns(3)
    with col_e:
        event_key = st.selectbox(
            "Event",
            options=list(MAJOR_ISU_EVENT_KEYS),
            format_func=lambda k: MAJOR_ISU_EVENT_LABELS.get(k, k),
            key="intl_major_event_key",
        )
    with col_a:
        pick_appt = st.selectbox(
            "Appointment type",
            options=appt_options,
            format_func=lambda x: appt_labels.get(x, str(x)),
            key="intl_major_event_appt",
        )
    with col_d:
        pick_disc = st.selectbox(
            "Discipline",
            options=disc_options,
            format_func=lambda x: disc_labels.get(x, str(x)),
            key="intl_major_event_disc",
        )

    major_appt_id = _sentinel(pick_appt)
    major_disc_id = _sentinel(pick_disc)

    _sync_intl_query_params(
        **_major_events_query_params(
            event_key=event_key,
            major_appt_id=major_appt_id,
            major_disc_id=major_disc_id,
            active_only=active_only,
        )
    )

    with st.spinner("Loading event matrix..."):
        matrix = _load_major_event_matrix(
            event_key,
            major_appt_id,
            major_disc_id,
            active_only,
        )

    if matrix.empty:
        st.info("No ISU-appointed officials match the current filters.")
        st.stop()

    display, year_cols = format_major_event_matrix_for_display(matrix)
    if "Years since last" in display.columns:
        display = display.sort_values(
            "Years since last",
            ascending=True,
            na_position="last",
        )
    total_officials = int(matrix["official_id"].nunique())
    times_col = "Times at event" if "Times at event" in display.columns else "total_championships"
    total_assignments = (
        int(pd.to_numeric(display[times_col], errors="coerce").fillna(0).sum())
        if times_col in display.columns
        else 0
    )

    m1, m2 = st.columns(2)
    m1.metric("Officials", total_officials)
    m2.metric("Total event assignments", total_assignments)

    show_cols = [
        c
        for c in [
            "Official",
            "Years eligible",
            "Years since last",
            "Most recent year",
            "Times at event",
            *year_cols,
        ]
        if c in display.columns
    ]
    styled_display = style_major_event_matrix_display(display, year_cols)
    hide_cols = [c for c in display.columns if c not in show_cols]
    if hide_cols:
        styled_display = styled_display.hide(subset=hide_cols, axis="columns")
    st.dataframe(
        styled_display,
        width="stretch",
        hide_index=True,
        column_config={
            "Official": st.column_config.TextColumn("Official", width="medium", pinned=True),
            "Years eligible": st.column_config.NumberColumn(
                "Years eligible",
                format="%d",
                help=(
                    "Current calendar year minus the year of their earliest ISU "
                    "Singles/Pairs/Dance appointment."
                ),
                width="small",
            ),
            "Years since last": st.column_config.NumberColumn(
                "Years since last",
                format="%d",
                help="Blank if never at this event.",
                width="small",
            ),
            "Most recent year": st.column_config.NumberColumn(
                "Most recent year",
                format="%d",
                width="small",
            ),
            "Times at event": st.column_config.NumberColumn(
                "Times at event",
                format="%d",
                help="Distinct calendar years with panel service at this event (selected role and discipline).",
            ),
            **{
                y: st.column_config.TextColumn(
                    y,
                    help=f"Panel role(s) at {MAJOR_ISU_EVENT_LABELS.get(event_key, event_key)}",
                    width="small",
                )
                for y in year_cols
            },
        },
    )
    st.caption(major_event_matrix_legend())

    appt_date = _load_appointment_data_date()
    if appt_date is not None:
        st.caption(
            f"Directory appointment data current as of {pd.Timestamp(appt_date):%-m/%-d/%Y}"
        )
    st.stop()

st.title("International Officials Activity")
st.caption(
    "Tracks panel assignments at ISU / international competitions and "
    "US **qualifying** competitions (excluding adult/collegiate). "
    f"Only **{'** and **'.join(sorted(COUNTABLE_SEGMENT_LEVELS))}** segments count toward activity and requirements. "
    f"**International Data / Video Operator** combines SPD and Synchronized appointments "
    f"and all Singles, Pairs, Dance, and Synchronized panel work. "
    "Maintain/promote checks use each rule's season window before the selected listing season; "
    "Seminar requirements are evaluated when attendance is recorded in "
    "``isu_official_seminar``; examinations and some ISU Communications "
    "alternatives (e.g. Initial Judges Meeting) are not automated. "
    "Use **Open** in the summary table for a full appointment report."
)

appt_df = _load_appointment_type_options()

appt_options = [_ALL_LABEL] + appt_df["appointment_type_id"].astype(int).tolist()
appt_labels = {_ALL_LABEL: _ALL_LABEL}
for row in appt_df.itertuples(index=False):
    appt_labels[int(row.appointment_type_id)] = row.appointment_type

col_f1, col_f2, col_f3, col_f4 = st.columns(4)
with col_f1:
    pick_appt = st.selectbox(
        "Appointment type",
        options=appt_options,
        format_func=lambda x: appt_labels.get(x, str(x)),
        index=0,
    )

appt_id = _sentinel(pick_appt)
idvo_only = appt_id == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID

level_df = _load_level_options(appt_id, None, active_only)
level_options = [_ALL_LABEL]
level_labels = {_ALL_LABEL: _ALL_LABEL}
if not level_df.empty:
    level_options.extend(level_df["appointment_level_id"].astype(int).tolist())
    for row in level_df.itertuples(index=False):
        level_labels[int(row.appointment_level_id)] = row.appointment_level

saved_level = st.session_state.get("intl_level_pick", _ALL_LABEL)
if saved_level not in level_options:
    st.session_state["intl_level_pick"] = _ALL_LABEL

with col_f2:
    pick_level = st.selectbox(
        "Level",
        options=level_options,
        format_func=lambda x: level_labels.get(x, str(x)),
        key="intl_level_pick",
        help="Directory appointment level (International or ISU Championship).",
    )

level_id = _sentinel(pick_level)

disc_df = _load_discipline_options(appt_id, level_id, active_only)
disc_options = [_ALL_LABEL]
disc_labels = {_ALL_LABEL: _ALL_LABEL}
if not idvo_only and not disc_df.empty:
    disc_options.extend(disc_df["discipline_id"].astype(int).tolist())
    for row in disc_df.itertuples(index=False):
        disc_labels[int(row.discipline_id)] = (
            row.discipline or f"Discipline {row.discipline_id}"
        )

saved_disc = st.session_state.get("intl_disc_pick", _ALL_LABEL)
if saved_disc not in disc_options:
    st.session_state["intl_disc_pick"] = _ALL_LABEL

with col_f3:
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
            key="intl_disc_pick",
            help="Only disciplines with at least one international appointment.",
        )

disc_id = _sentinel(pick_disc)

officials_df = _load_official_options(appt_id, disc_id, level_id, active_only)
with col_f4:
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

_sync_intl_query_params(level=str(level_id) if level_id is not None else None)

with st.spinner("Loading activity..."):
    summary, report_season_codes = _load_summary(
        appt_id,
        disc_id,
        level_id,
        official_id,
        active_only,
        include_requirements,
        show_seminar_maintain or show_seminar_promote,
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
total_intl_competitions = int(summary["competition_count_international"].sum())
total_nat_competitions = int(summary["competition_count_national"].sum())
total_intl_segments = int(summary["segment_count_international"].sum())
total_nat_segments = int(summary["segment_count_national"].sum())

if show_activity_detail:
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Officials", total_officials)
    m2.metric("Appointment rows", total_appointments)
    m3.metric("Intl competitions", total_intl_competitions)
    m4.metric("National competitions", total_nat_competitions)
    m5.metric("Intl segments", total_intl_segments)
    m6.metric("National segments", total_nat_segments)
else:
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
        level_id,
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

def _format_demographic_column(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return str(int(val))


_age_col_label = age_listing_column_label(listing_season_code)
_yrs_in_grade_col_label = years_in_grade_listing_column_label(listing_season_code)
_promote_first_col_label = promote_first_eligible_column_label()
_age_out_col_label = age_out_column_label()

summary_display = summary.rename(
    columns={
        "official_name": "Official",
        "mbr_number": "Member #",
        "appointment_type": "Appointment",
        "appointment_level": "Level",
        "discipline": "Discipline",
        "competition_count": "Competitions",
        "segment_count": "Segments",
        "competition_count_international": "Intl competitions",
        "competition_count_national": "National competitions",
        "segment_count_international": "Intl segments",
        "segment_count_national": "National segments",
        "age_as_of_listing": _age_col_label,
        "years_in_grade": _yrs_in_grade_col_label,
        "promote_first_eligible_listing": _promote_first_col_label,
        "age_out_listing": _age_out_col_label,
        "maintain": "Maintain",
        "maintain_note": "Maintain detail",
        "promote": "Promote",
        "promote_note": "Promote detail",
        "seminar_maintain": "Seminar maintain",
        "seminar_promote": "Seminar promote",
    }
)
for col in (_age_col_label, _yrs_in_grade_col_label):
    if col in summary_display.columns:
        summary_display[col] = summary_display[col].map(_format_demographic_column)

if _promote_first_col_label in summary_display.columns:
    summary_display[_promote_first_col_label] = summary.apply(
        lambda r: format_promote_first_eligible_display(
            None
            if pd.isna(r.get("promote_first_eligible_listing"))
            else int(r["promote_first_eligible_listing"]),
            current_listing_season_code=int(listing_season_code),
        ),
        axis=1,
    )

if _age_out_col_label in summary_display.columns:
    summary_display[_age_out_col_label] = summary.apply(
        lambda r: format_age_out_display(
            None
            if pd.isna(r.get("age_out_listing"))
            else int(r["age_out_listing"]),
            current_listing_season_code=int(listing_season_code),
        ),
        axis=1,
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

_activity_count_cols = (
    [
        "Intl competitions",
        "National competitions",
        "Intl segments",
        "National segments",
    ]
    if show_activity_detail
    else []
)

show_cols = [
    c
    for c in [
        "Official",
        "Member #",
        "Appointment",
        "Level",
        "Discipline",
        *_activity_count_cols,
        _age_col_label,
        _yrs_in_grade_col_label,
        _promote_first_col_label,
        _age_out_col_label,
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
        *(
            ["Seminar maintain"]
            if show_seminar_maintain
            else []
        ),
        *(
            ["Seminar promote"]
            if show_seminar_promote
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
        "Official": st.column_config.TextColumn("Official", width="medium", pinned=True),
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
        "Intl competitions": st.column_config.NumberColumn(
            "Intl competitions",
            help=(
                "Distinct ISU / international competitions with at least one "
                f"matching Junior/Senior segment in seasons "
                f"{', '.join(format_usfs_season_code(c) for c in report_season_codes)}."
            ),
        ),
        "National competitions": st.column_config.NumberColumn(
            "National competitions",
            help=(
                "Distinct US national qualifying competitions with at least one matching "
                f"Junior/Senior segment in seasons "
                f"{', '.join(format_usfs_season_code(c) for c in report_season_codes)}."
            ),
        ),
        "Intl segments": st.column_config.NumberColumn(
            "Intl segments",
            help=(
                "Junior/Senior segments at international competitions in the selected role "
                f"and discipline ({', '.join(format_usfs_season_code(c) for c in report_season_codes)})."
            ),
        ),
        "National segments": st.column_config.NumberColumn(
            "National segments",
            help=(
                "Junior/Senior segments at national qualifying competitions in the selected role "
                f"and discipline ({', '.join(format_usfs_season_code(c) for c in report_season_codes)})."
            ),
        ),
        _promote_first_col_label: st.column_config.TextColumn(
            _promote_first_col_label,
            help=(
                "First listing season when promote year-in-grade requirements are met "
                "(July 1 reference). Competition requirements may still apply."
            ),
            width="small",
        ),
        _age_out_col_label: st.column_config.TextColumn(
            _age_out_col_label,
            help=(
                "First listing July 1 when the official's age on that date is at least 70. "
                "Uses date of birth from the directory."
            ),
            width="small",
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
        "Seminar maintain": st.column_config.TextColumn(
            "Seminar maintain",
            help=(
                "Whether seminar maintain requirements are met for this appointment. "
                "Blank when no seminar requirement applies."
            ),
            width="small",
        ),
        "Seminar promote": st.column_config.TextColumn(
            "Seminar promote",
            help=(
                "Whether seminar promote requirements are met for this appointment. "
                "Blank when no seminar requirement applies."
            ),
            width="small",
        ),
        "Report": st.column_config.LinkColumn(
            "Report",
            display_text="Open",
            help="Open a detailed activity and requirements report for this appointment.",
        ),
    },
)

st.subheader("Timeline distribution")
_hist_mode = st.radio(
    "Chart",
    options=["Promotion", "Aging out (age 70)"],
    horizontal=True,
    key="intl_summary_timeline_chart",
    help=(
        "Promotion: one row per appointment in the filtered summary. "
        "Aging out: one row per distinct official (date of birth)."
    ),
)
if _hist_mode == "Promotion":
    _hist_years = summary["promote_first_eligible_listing"].map(
        lambda s: calendar_year_for_listing_milestone(
            None if pd.isna(s) else int(s),
            current_listing_season_code=int(listing_season_code),
        )
    )
    _hist_df = histogram_counts_by_year_bins(_hist_years, bin_width=1)
    if _hist_df.empty:
        st.caption("No rows with a known year for this chart under the current filters.")
    else:
        _hist_fig = px.bar(
            _hist_df,
            x="bin_label",
            y="count",
            title="First promote year (filtered appointment rows)",
            labels={"bin_label": "Calendar year", "count": "Appointment rows"},
        )
        _hist_fig.update_layout(bargap=0.15, xaxis_title="Calendar year")
        st.plotly_chart(_hist_fig, width="stretch")
        st.caption(
            f"{int(_hist_years.notna().sum())} of {len(summary)} appointment row"
            f"{'s' if len(summary) != 1 else ''} have a promote year."
        )
else:
    _hist_people = summary.drop_duplicates(subset=["official_id"], keep="first")
    _hist_ages = _hist_people["age_as_of_listing"]
    _hist_df = histogram_counts_by_current_age_bins(
        _hist_ages,
        bin_width=5,
        age_out_at=OFFICIAL_AGE_OUT_ON_JULY1,
    )
    if _hist_df.empty:
        st.caption("No officials with a known age for this chart under the current filters.")
    else:
        _hist_x_label = "Age (years on July 1)"
        _hist_fig = px.bar(
            _hist_df,
            x="bin_label",
            y="count",
            title=(
                f"Current age at listing — "
                f"{format_listing_reference_july1(listing_season_code)} (distinct officials)"
            ),
            labels={"bin_label": _hist_x_label, "count": "Officials"},
        )
        _hist_fig.update_layout(bargap=0.15, xaxis_title=_hist_x_label)
        _hist_fig.update_xaxes(
            categoryorder="array",
            categoryarray=list(_hist_df["bin_label"]),
        )
        st.plotly_chart(_hist_fig, width="stretch")
        _people_total = summary["official_id"].nunique()
        st.caption(
            f"{int(_hist_ages.notna().sum())} of {_people_total} official"
            f"{'s' if _people_total != 1 else ''} have a known age. "
            f"Bands align to age-out at {OFFICIAL_AGE_OUT_ON_JULY1} (e.g. 65–69, 60–64, 70+)."
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
            level_id,
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
        detail = enrich_panel_with_rule411_eligibility(detail)
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
                "rule411_entry_count": "Entries",
                "rule411_distinct_noc_count": "Nations",
                "rule411_status": "Rule 411",
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
                "Entries",
                "Nations",
                "Rule 411",
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
                "Rule 411": st.column_config.TextColumn(
                    "Rule 411",
                    help="Whether this international segment meets ISU entry/member minimums.",
                    width="small",
                ),
                "results_url": st.column_config.LinkColumn("Results", display_text="Open"),
            },
        )

appt_date = _load_appointment_data_date()
if appt_date is not None:
    st.caption(f"Directory appointment data current as of {pd.Timestamp(appt_date):%-m/%-d/%Y}")
