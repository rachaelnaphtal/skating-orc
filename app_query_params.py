"""
Deep-link helpers for Streamlit apps via ``st.query_params``.

Example (judging analytics)::

    ?page=individual&competition_scope=qualifying&year=2526

Example (activity tracker)::

    ?report=person&official_id=12345
"""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable, Mapping
from urllib.parse import unquote_plus


def qp_get(name: str) -> str | None:
    """First value for a query key (Streamlit may expose list-like values)."""
    import streamlit as st

    if name not in st.query_params:
        return None
    raw = st.query_params.get(name)
    if raw is None:
        return None
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if raw is None:
        return None
    text = unquote_plus(str(raw)).strip()
    return text or None


def qp_get_int(name: str) -> int | None:
    text = qp_get(name)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def qp_get_list(name: str) -> list[str]:
    """Comma-separated list in one query value."""
    text = qp_get(name)
    if not text:
        return []
    return [p.strip() for p in text.split(",") if p.strip()]


def query_params_snapshot() -> tuple[tuple[str, str], ...]:
    import streamlit as st

    return tuple(sorted((k, str(v)) for k, v in st.query_params.items()))


def query_params_changed(session_flag: str = "_analysis_qp_tuple") -> bool:
    """
    True when the browser query string changed since the last run.

    Call ``mark_query_params_applied()`` after syncing the URL so widget
    changes are not overwritten on the next rerun.
    """
    import streamlit as st

    return st.session_state.get(session_flag) != query_params_snapshot()


def mark_query_params_applied(session_flag: str = "_analysis_qp_tuple") -> None:
    import streamlit as st

    st.session_state[session_flag] = query_params_snapshot()


def apply_multiselect_param(
    param_name: str,
    session_key: str,
    allowed: Iterable[str],
) -> bool:
    """Set a multiselect session key from a comma-separated query value."""
    names = qp_get_list(param_name)
    if not names:
        return False
    allowed_set = set(allowed)
    valid = [n for n in names if n in allowed_set]
    if not valid:
        return False
    import streamlit as st

    st.session_state[session_key] = valid
    return True


def apply_page_slug(
    slug: str | None,
    slug_to_label: Mapping[str, str],
    session_key: str,
    allowed_labels: Iterable[str],
) -> bool:
    """Set sidebar/page widget from ``?page=<slug>`` if valid."""
    if not slug:
        return False
    label = slug_to_label.get(slug)
    if not label or label not in allowed_labels:
        return False
    import streamlit as st

    st.session_state[session_key] = label
    return True


def apply_choice_param(
    param_name: str,
    session_key: str,
    choices: Iterable[str],
    *,
    slug_map: Mapping[str, str] | None = None,
) -> bool:
    """
    Set a selectbox/radio session key from a query value.

    ``slug_map`` maps URL tokens to display labels (e.g. ``qualifying`` → ``Qualifying only``).
    """
    raw = qp_get(param_name)
    if not raw:
        return False
    label = slug_map.get(raw, raw) if slug_map else raw
    if label not in choices:
        return False
    import streamlit as st

    st.session_state[session_key] = label
    return True


def apply_int_select_param(
    param_name: str,
    session_key: str,
    options: Iterable[int],
) -> bool:
    value = qp_get_int(param_name)
    if value is None:
        return False
    opts = [int(x) for x in options]
    if value not in opts:
        return False
    import streamlit as st

    st.session_state[session_key] = value
    return True


def sync_query_params(**params: Any) -> None:
    """Write non-empty params to the browser URL; omit None/empty."""
    import streamlit as st

    for key, value in params.items():
        if value is None or value == "" or value == []:
            if key in st.query_params:
                del st.query_params[key]
            continue
        if isinstance(value, (list, tuple)):
            st.query_params[key] = ",".join(str(v) for v in value)
        else:
            st.query_params[key] = str(value)


def render_query_help(lines: list[str]) -> None:
    """Sidebar snippet documenting supported query parameters."""
    import streamlit as st

    with st.sidebar.expander("URL query parameters", expanded=False):
        st.markdown("\n".join(lines))


# --- Judging analytics (analysis_app.py) ---

ANALYSIS_PAGE_SLUG_TO_LABEL = {
    "individual": "Individual Judge Analysis",
    "cross-judge": "Cross-Judge Benchmarking",
    "temporal": "Temporal Trend Analysis",
    "panel-size-benchmarks": "Panel size benchmarks",
    "panel-benchmarks": "Panel size benchmarks",
    "rule-errors": "Rule Errors Analysis",
    "competition": "Competition Analysis",
    "load-competition": "Load Competition",
}
ANALYSIS_LABEL_TO_PAGE_SLUG = {
    v: k for k, v in ANALYSIS_PAGE_SLUG_TO_LABEL.items()
}

COMPETITION_SCOPE_SLUG_TO_LABEL = {
    "all": "All competitions",
    "qualifying": "Qualifying only",
    "nqs": "NQS only",
    "sectionals": "Sectionals & championships",
    "championships": "Championships only",
}
COMPETITION_SCOPE_LABEL_TO_SLUG = {
    v: k for k, v in COMPETITION_SCOPE_SLUG_TO_LABEL.items()
}

# Mirrors analysis_app ``_COMPETITION_SCOPE_LABEL_TO_KEY`` (scope label → analytics key).
from officials_competition_types import (  # noqa: E402
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_NQS,
    COMPETITION_SCOPE_QUALIFYING,
    COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
)

COMPETITION_SCOPE_LABEL_TO_KEY = {
    "All competitions": COMPETITION_SCOPE_ALL,
    "Qualifying only": COMPETITION_SCOPE_QUALIFYING,
    "NQS only": COMPETITION_SCOPE_NQS,
    "Sectionals & championships": COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    "Championships only": COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
}

_CROSS_JUDGE_METRICS = (
    "throwout_rate",
    "anomaly_rate",
    "rule_error_rate",
    "avg_deviation",
    "excess_anomalies",
    "rule_errors",
)
_SCORE_TYPES = ("both", "pcs", "element")
_CROSS_JUDGE_VIEWS = ("Judge Overview", "Judge vs Competition")
_TEMPORAL_ANALYSIS_TYPES = (
    "Individual Judge Trends",
    "Overall System Trends",
    "Judge Consistency Ranking",
)


def _discipline_names_for_scope(analytics, scope_label: str) -> list[str]:
    scope_key = COMPETITION_SCOPE_LABEL_TO_KEY.get(
        scope_label, COMPETITION_SCOPE_ALL
    )
    if scope_key != COMPETITION_SCOPE_ALL:
        types = analytics.qualifying_event_segment_discipline_types()
    else:
        types = analytics.get_discipline_types()
    return [name for _dt_id, name in types]


def _apply_date_param(param_name: str, session_key: str) -> bool:
    text = qp_get(param_name)
    if not text:
        return False
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return False
    import streamlit as st

    st.session_state[session_key] = parsed
    return True


def _apply_year_param(
    param_name: str,
    session_key: str,
    analytics,
) -> None:
    year = qp_get(param_name)
    if not year or year == "all":
        return
    years = analytics.get_years()
    choices = ["All Years"] + [str(y) for y in years]
    if year not in choices:
        return
    import streamlit as st

    st.session_state[session_key] = year


def init_analysis_app_from_query(nav_pages: list[str], *, from_url: bool = True) -> None:
    """Apply ``?page=`` before the sidebar radio renders."""
    if not from_url:
        return
    apply_page_slug(
        qp_get("page"),
        ANALYSIS_PAGE_SLUG_TO_LABEL,
        "primary_nav_page",
        nav_pages,
    )


def _scope_session_key_for_page(page: str) -> str | None:
    return {
        "Individual Judge Analysis": "individual_judge_competition_scope",
        "Temporal Trend Analysis": "temporal_competition_scope",
        "Cross-Judge Benchmarking": "cross_judge_competition_scope",
        "Rule Errors Analysis": "rule_errors_qualifying",
        "Panel size benchmarks": "panel_benchmarks_scope",
    }.get(page)


def apply_analysis_filters_for_page(
    page: str, analytics, *, from_url: bool = True
) -> None:
    """Seed widget session state from URL when the query string changes."""
    import streamlit as st

    if not from_url:
        return

    scope_key = _scope_session_key_for_page(page)
    if scope_key:
        apply_choice_param(
            "competition_scope",
            scope_key,
            COMPETITION_SCOPE_SLUG_TO_LABEL.values(),
            slug_map=COMPETITION_SCOPE_SLUG_TO_LABEL,
        )

    if page == "Individual Judge Analysis":
        ig_labels, _ = _identity_group_options(analytics)
        apply_choice_param("judge", "individual_judge_select", ig_labels)
        _apply_year_param("year", "individual_judge_year", analytics)
        scope_label = st.session_state.get(
            "individual_judge_competition_scope", "All competitions"
        )
        apply_multiselect_param(
            "disciplines",
            "individual_judge_disciplines",
            _discipline_names_for_scope(analytics, scope_label),
        )
        if _apply_date_param("start_date", "individual_judge_start_date") or _apply_date_param(
            "end_date", "individual_judge_end_date"
        ):
            import streamlit as st

            st.session_state["individual_judge_use_event_dates"] = True

    elif page == "Cross-Judge Benchmarking":
        apply_choice_param("view", "cross_judge_view", _CROSS_JUDGE_VIEWS)
        apply_choice_param("metric", "cross_judge_metric", _CROSS_JUDGE_METRICS)
        apply_choice_param("score_type", "cross_judge_score_type", _SCORE_TYPES)
        _apply_year_param("year", "cross_judge_year", analytics)
        scope_label = st.session_state.get(
            "cross_judge_competition_scope", "All competitions"
        )
        apply_multiselect_param(
            "disciplines",
            "cross_judge_disciplines",
            _discipline_names_for_scope(analytics, scope_label),
        )
        if _apply_date_param("start_date", "cross_judge_start_date") or _apply_date_param(
            "end_date", "cross_judge_end_date"
        ):
            import streamlit as st

            st.session_state["cross_judge_use_event_dates"] = True

    elif page == "Temporal Trend Analysis":
        apply_choice_param(
            "analysis_type", "temporal_analysis_type", _TEMPORAL_ANALYSIS_TYPES
        )
        apply_choice_param("metric", "temporal_metric", _CROSS_JUDGE_METRICS)
        apply_choice_param("score_type", "temporal_score_type", _SCORE_TYPES)
        ig_labels, _ = _identity_group_options(analytics)
        apply_choice_param("judge", "temporal_judge_select", ig_labels)

    elif page == "Rule Errors Analysis":
        _apply_year_param("year", "rule_errors_year", analytics)
        comps = qp_get_list("competitions")
        if comps:
            st.session_state["rule_errors_comps"] = comps
        judges = qp_get_list("judges")
        if judges:
            st.session_state["rule_errors_judges"] = judges

    elif page == "Competition Analysis":
        cid = qp_get_int("competition_id")
        if cid is not None:
            competitions = analytics.get_competitions()
            for comp_id, name, year in competitions:
                if int(comp_id) == cid:
                    st.session_state["competition_analysis_select"] = (
                        f"{name} ({year})"
                    )
                    break


def _identity_group_options(analytics):
    groups = analytics.get_judge_analysis_identity_groups()
    labels = [g["label"] for g in groups]
    return labels, {g["label"]: g["judge_ids"] for g in groups}


def sync_analysis_app_query_params(page: str) -> None:
    """Mirror current page and common filters into the browser URL."""
    import streamlit as st

    params: dict[str, Any] = {}
    slug = ANALYSIS_LABEL_TO_PAGE_SLUG.get(page)
    if slug:
        params["page"] = slug

    scope_ss = _scope_session_key_for_page(page)
    if scope_ss and scope_ss in st.session_state:
        label = st.session_state[scope_ss]
        params["competition_scope"] = COMPETITION_SCOPE_LABEL_TO_SLUG.get(label)

    if page == "Individual Judge Analysis":
        if st.session_state.get("individual_judge_select"):
            params["judge"] = st.session_state["individual_judge_select"]
        y = st.session_state.get("individual_judge_year")
        params["year"] = str(y) if y and y != "All Years" else None
        disc = st.session_state.get("individual_judge_disciplines")
        params["disciplines"] = disc if disc else None
        if st.session_state.get("individual_judge_use_event_dates"):
            start = st.session_state.get("individual_judge_start_date")
            end = st.session_state.get("individual_judge_end_date")
            if start is not None:
                params["start_date"] = start.isoformat()
            if end is not None:
                params["end_date"] = end.isoformat()

    elif page == "Cross-Judge Benchmarking":
        for qp_name, ss_key in (
            ("view", "cross_judge_view"),
            ("metric", "cross_judge_metric"),
            ("score_type", "cross_judge_score_type"),
        ):
            if st.session_state.get(ss_key):
                params[qp_name] = st.session_state[ss_key]
        y = st.session_state.get("cross_judge_year")
        params["year"] = str(y) if y and y != "All Years" else None
        disc = st.session_state.get("cross_judge_disciplines")
        params["disciplines"] = disc if disc else None
        if st.session_state.get("cross_judge_use_event_dates"):
            start = st.session_state.get("cross_judge_start_date")
            end = st.session_state.get("cross_judge_end_date")
            if start is not None:
                params["start_date"] = start.isoformat()
            if end is not None:
                params["end_date"] = end.isoformat()

    elif page == "Temporal Trend Analysis":
        for qp_name, ss_key in (
            ("analysis_type", "temporal_analysis_type"),
            ("metric", "temporal_metric"),
            ("score_type", "temporal_score_type"),
        ):
            if st.session_state.get(ss_key):
                params[qp_name] = st.session_state[ss_key]
        if st.session_state.get("temporal_judge_select"):
            params["judge"] = st.session_state["temporal_judge_select"]

    elif page == "Rule Errors Analysis":
        y = st.session_state.get("rule_errors_year")
        params["year"] = str(y) if y and y != "All Years" else None
        comps = st.session_state.get("rule_errors_comps")
        params["competitions"] = comps if comps else None
        judges = st.session_state.get("rule_errors_judges")
        params["judges"] = judges if judges else None

    elif page == "Competition Analysis":
        cid = st.session_state.get("competition_analysis_id")
        if cid is not None:
            params["competition_id"] = int(cid)

    sync_query_params(**params)


# --- Officials activity tracker ---

ACTIVITY_REPORT_SLUG_TO_LABEL = {
    "championships": "Championships Detailed Activity",
    "sectionals": "Sectionals detailed activity",
    "assignments": "Number of assignments",
    "referee": "Referee service (by competition type)",
    "person": "Per-person assignments",
    "competition": "Per-competition assignments",
    "nqs": "NQS detailed activity",
    "synchro": "Synchro Activity",
    "appointments": "Appointments by achieved date",
}
ACTIVITY_LABEL_TO_REPORT_SLUG = {
    v: k for k, v in ACTIVITY_REPORT_SLUG_TO_LABEL.items()
}

_ACTIVITY_REPORT_OPTIONS = tuple(ACTIVITY_REPORT_SLUG_TO_LABEL.values())


def init_activity_tracker_from_query() -> None:
    apply_page_slug(
        qp_get("report") or qp_get("page"),
        ACTIVITY_REPORT_SLUG_TO_LABEL,
        "activity_report_mode",
        _ACTIVITY_REPORT_OPTIONS,
    )


def apply_activity_entity_ids_from_query(
    official_options: list[int] | None = None,
    competition_options: list[int] | None = None,
) -> None:
    """Run after dropdown option lists are known (person / competition reports)."""
    if official_options is not None:
        apply_int_select_param(
            "official_id", "per_person_official_select", official_options
        )
    if competition_options is not None:
        apply_int_select_param(
            "competition_id", "per_competition_select", competition_options
        )


def sync_activity_tracker_query_params(report_mode: str) -> None:
    import streamlit as st

    params: dict[str, Any] = {}
    slug = ACTIVITY_LABEL_TO_REPORT_SLUG.get(report_mode)
    if slug:
        params["report"] = slug
    if report_mode == REPORT_PERSON_ASSIGNMENTS_LABEL:
        oid = st.session_state.get("per_person_official_select")
        if oid is not None:
            params["official_id"] = int(oid)
    elif report_mode == REPORT_COMPETITION_ASSIGNMENTS_LABEL:
        cid = st.session_state.get("per_competition_select")
        if cid is not None:
            params["competition_id"] = int(cid)
    sync_query_params(**params)


REPORT_PERSON_ASSIGNMENTS_LABEL = "Per-person assignments"
REPORT_COMPETITION_ASSIGNMENTS_LABEL = "Per-competition assignments"
