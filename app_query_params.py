"""
Deep-link helpers for Streamlit apps via ``st.query_params``.

Example (judging analytics)::

    ?page=individual&competition_scope=qualifying&year=2526

Example (activity tracker)::

    ?report=sectionals&official_type=competition-judge&discipline=no-discipline
        &competition_group=spd-sectionals&show_other_roles=1
    ?report=championships&include_lower_levels=0
    ?report=person&official=12345
    ?report=competition&competition=99
    ?report=nqs&level=sectional,national
    ?report=appointments&achieved_from=2024-01-01&achieved_to=2024-12-31&active_only=1
    ?report=availability&form=1&competition=3&appointment=judge&discipline=synchronized
        &level=any&show_available=1&show_no_reply=0&show_unavailable=0
"""

from __future__ import annotations

import re
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


def apply_int_select_param_aliases(
    param_names: Iterable[str],
    session_key: str,
    options: Iterable[int],
) -> bool:
    for name in param_names:
        if apply_int_select_param(name, session_key, options):
            return True
    return False


def apply_bool_param(param_name: str, session_key: str) -> bool:
    raw = qp_get(param_name)
    if raw is None:
        return False
    import streamlit as st

    token = raw.strip().lower()
    if token in ("1", "true", "yes", "on"):
        st.session_state[session_key] = True
        return True
    if token in ("0", "false", "no", "off"):
        st.session_state[session_key] = False
        return True
    return False


def apply_date_param(param_name: str, session_key: str) -> bool:
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


def label_to_query_slug(label: str) -> str:
    """Stable URL token from a display label (e.g. ``Competition Judge`` → ``competition-judge``)."""
    text = str(label).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "item"


def slug_map_for_labels(labels: Iterable[str]) -> dict[str, str]:
    """Map URL slugs to selectbox labels; duplicate slugs keep the first label."""
    out: dict[str, str] = {}
    for lab in labels:
        slug = label_to_query_slug(lab)
        out.setdefault(slug, lab)
    return out


def _query_param_list_matches(key: str, values: list[str]) -> bool:
    """True when the URL list param matches ``values`` (order-sensitive)."""
    current = qp_get_list(key)
    expected = [str(v) for v in values]
    return current == expected


def sync_query_params(**params: Any) -> None:
    """Write non-empty params to the browser URL; omit None/empty.

    Skips assignments when the URL already matches so ``st.download_button``
    clicks are not interrupted by a no-op query-string update + extra rerun.
    """
    import streamlit as st

    for key, value in params.items():
        if value is None or value == "" or value == []:
            if key in st.query_params:
                del st.query_params[key]
            continue
        if isinstance(value, (list, tuple)):
            values = [str(v) for v in value]
            if _query_param_list_matches(key, values):
                continue
            st.query_params[key] = ",".join(values)
        else:
            new = str(value)
            if qp_get(key) == new:
                continue
            st.query_params[key] = new


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
    "element-deviation-ranking": "Element Deviation Ranking Analysis",
    "pcs-quality": "PCS Quality Analysis",
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
    "international": "International",
}
COMPETITION_SCOPE_LABEL_TO_SLUG = {
    v: k for k, v in COMPETITION_SCOPE_SLUG_TO_LABEL.items()
}

# Mirrors analysis_app ``_COMPETITION_SCOPE_LABEL_TO_KEY`` (scope label → analytics key).
from officials_competition_types import (  # noqa: E402
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_INTERNATIONAL,
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
    "International": COMPETITION_SCOPE_INTERNATIONAL,
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
        "Element Deviation Ranking Analysis": "element_ranking_competition_scope",
        "PCS Quality Analysis": "pcs_quality_competition_scope",
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

    elif page == "PCS Quality Analysis":
        apply_choice_param(
            "competition_scope",
            "pcs_quality_competition_scope",
            COMPETITION_SCOPE_SLUG_TO_LABEL.values(),
            slug_map=COMPETITION_SCOPE_SLUG_TO_LABEL,
        )
        start_sy = qp_get("start_season")
        end_sy = qp_get("end_season")
        if start_sy:
            import streamlit as st

            st.session_state["pcs_quality_start_season"] = start_sy
        if end_sy:
            import streamlit as st

            st.session_state["pcs_quality_end_season"] = end_sy
        if _apply_date_param("start_date", "pcs_quality_start_date") or _apply_date_param(
            "end_date", "pcs_quality_end_date"
        ):
            import streamlit as st

            st.session_state["pcs_quality_use_event_dates"] = True
        min_pcs = qp_get("min_pcs_marks")
        if min_pcs:
            try:
                import streamlit as st

                st.session_state["pcs_quality_min_pcs_marks"] = int(min_pcs)
            except ValueError:
                pass
        scope_label = st.session_state.get(
            "pcs_quality_competition_scope", "All competitions"
        )
        disc_names = _discipline_names_for_scope(analytics, scope_label)
        if not apply_multiselect_param(
            "disciplines",
            "pcs_quality_disciplines",
            disc_names,
        ):
            import streamlit as st

            if "pcs_quality_disciplines" not in st.session_state:
                if "Singles" in disc_names:
                    st.session_state["pcs_quality_disciplines"] = ["Singles"]
                elif disc_names:
                    st.session_state["pcs_quality_disciplines"] = [disc_names[0]]
        apply_bool_param("us_officials_only", "pcs_quality_us_officials_only")

    elif page == "Element Deviation Ranking Analysis":
        apply_choice_param(
            "competition_scope",
            "element_ranking_competition_scope",
            COMPETITION_SCOPE_SLUG_TO_LABEL.values(),
            slug_map=COMPETITION_SCOPE_SLUG_TO_LABEL,
        )
        start_sy = qp_get("start_season")
        end_sy = qp_get("end_season")
        if start_sy:
            import streamlit as st

            st.session_state["element_ranking_start_season"] = start_sy
        if end_sy:
            import streamlit as st

            st.session_state["element_ranking_end_season"] = end_sy
        scope_label = st.session_state.get(
            "element_ranking_competition_scope", "All competitions"
        )
        from element_deviation_ranking import element_ranking_discipline_names_for_scope

        scope_key = COMPETITION_SCOPE_LABEL_TO_KEY.get(
            scope_label, COMPETITION_SCOPE_ALL
        )
        apply_multiselect_param(
            "disciplines",
            "element_ranking_disciplines",
            element_ranking_discipline_names_for_scope(analytics, scope_key),
        )
        if _apply_date_param("start_date", "element_ranking_start_date") or _apply_date_param(
            "end_date", "element_ranking_end_date"
        ):
            import streamlit as st

            st.session_state["element_ranking_use_event_dates"] = True
        min_m = qp_get("min_marks")
        if min_m:
            try:
                import streamlit as st

                st.session_state["element_ranking_min_marks"] = int(min_m)
            except ValueError:
                pass
        apply_bool_param("us_officials_only", "element_ranking_us_officials_only")

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
        apply_bool_param("us_officials_only", "cross_judge_us_officials_only")

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

    elif page == "PCS Quality Analysis":
        scope_ss = st.session_state.get("pcs_quality_competition_scope")
        if scope_ss:
            params["competition_scope"] = COMPETITION_SCOPE_LABEL_TO_SLUG.get(scope_ss)
        start_sy = st.session_state.get("pcs_quality_start_season")
        if start_sy and start_sy != "Any":
            params["start_season"] = str(start_sy)
        end_sy = st.session_state.get("pcs_quality_end_season")
        if end_sy and end_sy != "Any":
            params["end_season"] = str(end_sy)
        if st.session_state.get("pcs_quality_use_event_dates"):
            start = st.session_state.get("pcs_quality_start_date")
            end = st.session_state.get("pcs_quality_end_date")
            if start is not None:
                params["start_date"] = start.isoformat()
            if end is not None:
                params["end_date"] = end.isoformat()
        min_pcs = st.session_state.get("pcs_quality_min_pcs_marks")
        if min_pcs and int(min_pcs) > 0:
            params["min_pcs_marks"] = int(min_pcs)
        disc = st.session_state.get("pcs_quality_disciplines")
        params["disciplines"] = disc if disc else None
        if st.session_state.get("pcs_quality_us_officials_only"):
            params["us_officials_only"] = "1"

    elif page == "Element Deviation Ranking Analysis":
        scope_ss = st.session_state.get("element_ranking_competition_scope")
        if scope_ss:
            params["competition_scope"] = COMPETITION_SCOPE_LABEL_TO_SLUG.get(scope_ss)
        start_sy = st.session_state.get("element_ranking_start_season")
        if start_sy and start_sy != "Any":
            params["start_season"] = str(start_sy)
        end_sy = st.session_state.get("element_ranking_end_season")
        if end_sy and end_sy != "Any":
            params["end_season"] = str(end_sy)
        disc = st.session_state.get("element_ranking_disciplines")
        params["disciplines"] = disc if disc else None
        if st.session_state.get("element_ranking_use_event_dates"):
            start = st.session_state.get("element_ranking_start_date")
            end = st.session_state.get("element_ranking_end_date")
            if start is not None:
                params["start_date"] = start.isoformat()
            if end is not None:
                params["end_date"] = end.isoformat()
        min_m = st.session_state.get("element_ranking_min_marks")
        if min_m and int(min_m) > 0:
            params["min_marks"] = int(min_m)
        if st.session_state.get("element_ranking_us_officials_only"):
            params["us_officials_only"] = "1"

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
        if st.session_state.get("cross_judge_us_officials_only"):
            params["us_officials_only"] = "1"

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
    "availability": "Qualifying availability",
}
ACTIVITY_LABEL_TO_REPORT_SLUG = {
    v: k for k, v in ACTIVITY_REPORT_SLUG_TO_LABEL.items()
}

_ACTIVITY_REPORT_OPTIONS = tuple(ACTIVITY_REPORT_SLUG_TO_LABEL.values())


def init_activity_tracker_from_query() -> None:
    """Apply ``?report=`` / ``?page=`` once per URL change (see ``query_params_changed``)."""
    slug = qp_get("report") or qp_get("page")
    if slug == "qualifying":
        slug = "availability"  # legacy deep links
    apply_page_slug(
        slug,
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
        apply_int_select_param_aliases(
            ("official", "official_id"),
            "per_person_official_select",
            official_options,
        )
    if competition_options is not None:
        apply_int_select_param_aliases(
            ("competition", "competition_id"),
            "per_competition_select",
            competition_options,
        )


_ACTIVITY_CHAMPIONSHIP_FILTER_REPORTS = (
    ACTIVITY_REPORT_SLUG_TO_LABEL["championships"],
    ACTIVITY_REPORT_SLUG_TO_LABEL["sectionals"],
)


def apply_activity_official_type_from_query(appt_options: list[str]) -> None:
    """Apply ``?official_type=`` before the Official Type selectbox is drawn."""
    if not appt_options:
        return
    apply_choice_param(
        "official_type",
        "activity_official_type",
        appt_options,
        slug_map=slug_map_for_labels(appt_options),
    )


def apply_activity_discipline_from_query(disc_options: list[str]) -> None:
    """Apply ``?discipline=`` before the Discipline selectbox is drawn."""
    if not disc_options:
        return
    apply_choice_param(
        "discipline",
        "activity_discipline",
        disc_options,
        slug_map=slug_map_for_labels(disc_options),
    )


def apply_activity_championship_filters_from_query(
    appt_options: list[str],
    disc_options: list[str] | None = None,
) -> None:
    """Official type / discipline (both must run before their widgets)."""
    apply_activity_official_type_from_query(appt_options)
    if disc_options is not None:
        apply_activity_discipline_from_query(disc_options)


def apply_activity_competition_group_from_query(
    options: list[str],
    session_key: str = "activity_competition_group",
) -> None:
    apply_choice_param(
        "competition_group",
        session_key,
        options,
        slug_map=slug_map_for_labels(options),
    )


def apply_activity_matrix_checkboxes_from_query(
    *, include_lower_levels: bool = False
) -> None:
    """Other roles / lower levels for championships & sectionals matrix."""
    apply_bool_param("show_other_roles", "activity_show_other_roles")
    if include_lower_levels:
        apply_bool_param("include_lower_levels", "activity_include_lower_levels")


def apply_activity_achieved_dates_from_query() -> None:
    """Appointments-by-date report: achieved_from / achieved_to (ISO), active_only."""
    apply_date_param("achieved_from", "appt_achieved_start")
    apply_date_param("achieved_to", "appt_achieved_end")
    apply_bool_param("active_only", "appt_achieved_active_only")


def apply_multiselect_param_slugs(
    param_name: str,
    session_key: str,
    allowed: Iterable[str],
    slug_map: Mapping[str, str],
) -> bool:
    names = qp_get_list(param_name)
    if not names:
        return False
    allowed_set = set(allowed)
    valid = []
    for token in names:
        label = slug_map.get(token, token)
        if label in allowed_set:
            valid.append(label)
    if not valid:
        return False
    import streamlit as st

    st.session_state[session_key] = valid
    return True


def apply_activity_level_filter_from_query(
    level_options: list[str],
    session_key: str,
) -> None:
    apply_multiselect_param_slugs(
        "level",
        session_key,
        level_options,
        slug_map=slug_map_for_labels(level_options),
    )


QUALIFYING_ANY_LEVEL_LABEL = "(Any level)"
QUALIFYING_ALL_LABEL = "(All)"


def apply_activity_qualifying_form_from_query(form_options: list[int]) -> None:
    """``?form=`` / ``?qualifying_form=`` → ``qualifying_form_select``."""
    apply_int_select_param_aliases(
        ("form", "qualifying_form", "qualifying_form_id"),
        "qualifying_form_select",
        form_options,
    )


def apply_activity_qualifying_competition_from_query(comp_options: list[int]) -> None:
    """``?competition=`` → ``qualifying_competition_select`` (qualifying report only)."""
    apply_int_select_param_aliases(
        ("competition", "qualifying_competition", "qualifying_competition_id"),
        "qualifying_competition_select",
        comp_options,
    )


def apply_activity_qualifying_level_from_query(level_options: list[str]) -> bool:
    """``?level=any`` or a slug matching a configured criterion level."""
    raw = qp_get("level")
    if not raw:
        return False
    token = raw.strip().lower()
    if token == "all":
        if QUALIFYING_ALL_LABEL in level_options:
            import streamlit as st

            st.session_state["qual_report_lvl"] = QUALIFYING_ALL_LABEL
            return True
        return False
    if token == "any":
        if QUALIFYING_ANY_LEVEL_LABEL in level_options:
            import streamlit as st

            st.session_state["qual_report_lvl"] = QUALIFYING_ANY_LEVEL_LABEL
            return True
        return False
    return apply_choice_param(
        "level",
        "qual_report_lvl",
        level_options,
        slug_map=slug_map_for_labels(level_options),
    )


def apply_activity_qualifying_report_filters_from_query(
    *,
    appointment_options: list[str],
    discipline_options: list[str],
    level_options: list[str],
) -> None:
    """
    Report filter selectboxes for qualifying availability (run before widgets).

    Applies ``appointment``, ``discipline``, ``level``, and availability toggles.
    """
    if appointment_options:
        apply_choice_param(
            "appointment",
            "qual_report_at",
            appointment_options,
            slug_map=slug_map_for_labels(appointment_options),
        )
    if discipline_options:
        apply_choice_param(
            "discipline",
            "qual_report_disc",
            discipline_options,
            slug_map=slug_map_for_labels(discipline_options),
        )
    if level_options:
        apply_activity_qualifying_level_from_query(level_options)
    apply_bool_param("show_available", "qual_report_show_available")
    apply_bool_param("show_no_reply", "qual_report_show_no_reply")
    apply_bool_param("show_unavailable", "qual_report_show_unavailable")
    # Legacy URL param
    apply_bool_param("only_available", "qual_report_show_available")


_ACTIVITY_OPTIONAL_QUERY_KEYS = (
    "official",
    "official_id",
    "competition",
    "competition_id",
    "form",
    "qualifying_form",
    "qualifying_form_id",
    "qualifying_competition",
    "qualifying_competition_id",
    "official_type",
    "discipline",
    "appointment",
    "competition_group",
    "show_other_roles",
    "include_lower_levels",
    "level",
    "achieved_from",
    "achieved_to",
    "active_only",
    "only_available",
    "show_available",
    "show_no_reply",
    "show_unavailable",
)

_REPORT_ASSIGNMENTS = ACTIVITY_REPORT_SLUG_TO_LABEL["assignments"]
_REPORT_REFEREE = ACTIVITY_REPORT_SLUG_TO_LABEL["referee"]
_REPORT_NQS = ACTIVITY_REPORT_SLUG_TO_LABEL["nqs"]
_REPORT_SYNCHRO = ACTIVITY_REPORT_SLUG_TO_LABEL["synchro"]
_REPORT_APPOINTMENTS = ACTIVITY_REPORT_SLUG_TO_LABEL["appointments"]
_REPORT_AVAILABILITY = ACTIVITY_REPORT_SLUG_TO_LABEL["availability"]


def _param_or_none(value: Any) -> Any:
    if value is None or value == "" or value == []:
        return None
    return value


def _official_id_from_query() -> int | None:
    return qp_get_int("official") or qp_get_int("official_id")


def _competition_id_from_query() -> int | None:
    return qp_get_int("competition") or qp_get_int("competition_id")


def sync_activity_tracker_query_params(
    report_mode: str, *, url_changed: bool = False
) -> None:
    import streamlit as st

    params: dict[str, Any] = {k: None for k in _ACTIVITY_OPTIONAL_QUERY_KEYS}
    slug = ACTIVITY_LABEL_TO_REPORT_SLUG.get(report_mode)
    if slug:
        params["report"] = slug

    if report_mode == REPORT_PERSON_ASSIGNMENTS_LABEL:
        # Early sync runs before the Official selectbox; keep URL ``official`` on
        # deep-link loads instead of overwriting with stale session state.
        if url_changed:
            oid = _official_id_from_query()
        else:
            oid = st.session_state.get("per_person_official_select")
        params["official"] = int(oid) if oid is not None else None
    elif report_mode == REPORT_COMPETITION_ASSIGNMENTS_LABEL:
        if url_changed:
            cid = _competition_id_from_query()
        else:
            cid = st.session_state.get("per_competition_select")
        params["competition"] = int(cid) if cid is not None else None
    elif report_mode in _ACTIVITY_CHAMPIONSHIP_FILTER_REPORTS:
        appt = st.session_state.get("activity_official_type")
        disc = st.session_state.get("activity_discipline")
        params["official_type"] = (
            label_to_query_slug(str(appt)) if appt else None
        )
        params["discipline"] = (
            label_to_query_slug(str(disc)) if disc else None
        )
        comp_grp = st.session_state.get("activity_competition_group")
        params["competition_group"] = (
            label_to_query_slug(str(comp_grp)) if comp_grp else None
        )
        if st.session_state.get("activity_show_other_roles"):
            params["show_other_roles"] = "1"
        if report_mode == ACTIVITY_REPORT_SLUG_TO_LABEL["championships"]:
            ill = st.session_state.get("activity_include_lower_levels")
            if ill is not None:
                params["include_lower_levels"] = "1" if ill else "0"
    elif report_mode == _REPORT_ASSIGNMENTS:
        cg = st.session_state.get("assignments_report_comp_group")
        params["competition_group"] = (
            label_to_query_slug(str(cg)) if cg else None
        )
    elif report_mode == _REPORT_REFEREE:
        cg = st.session_state.get("referee_report_comp_group")
        params["competition_group"] = (
            label_to_query_slug(str(cg)) if cg else None
        )
    elif report_mode in (_REPORT_NQS, _REPORT_SYNCHRO):
        key = "synchro_level_filter" if report_mode == _REPORT_SYNCHRO else "nqs_level_filter"
        levels = st.session_state.get(key) or []
        if levels:
            params["level"] = ",".join(
                label_to_query_slug(str(l)) for l in levels
            )
    elif report_mode == _REPORT_APPOINTMENTS:
        start = st.session_state.get("appt_achieved_start")
        end = st.session_state.get("appt_achieved_end")
        if start is not None:
            params["achieved_from"] = start.isoformat()
        if end is not None:
            params["achieved_to"] = end.isoformat()
        active = st.session_state.get("appt_achieved_active_only")
        if active is not None:
            params["active_only"] = "1" if active else "0"
    elif report_mode == _REPORT_AVAILABILITY:
        fid = st.session_state.get("qualifying_form_select")
        params["form"] = int(fid) if fid is not None else None
        cid = st.session_state.get("qualifying_competition_select")
        params["competition"] = int(cid) if cid is not None else None
        appt = st.session_state.get("qual_report_at")
        params["appointment"] = (
            label_to_query_slug(str(appt)) if appt else None
        )
        disc = st.session_state.get("qual_report_disc")
        params["discipline"] = (
            label_to_query_slug(str(disc)) if disc else None
        )
        lvl = st.session_state.get("qual_report_lvl")
        if lvl == QUALIFYING_ALL_LABEL:
            params["level"] = "all"
        elif lvl == QUALIFYING_ANY_LEVEL_LABEL:
            params["level"] = "any"
        elif lvl:
            params["level"] = label_to_query_slug(str(lvl))
        sa = st.session_state.get("qual_report_show_available")
        if sa is not None:
            params["show_available"] = "1" if sa else "0"
        sn = st.session_state.get("qual_report_show_no_reply")
        if sn is not None:
            params["show_no_reply"] = "1" if sn else "0"
        su = st.session_state.get("qual_report_show_unavailable")
        if su is not None:
            params["show_unavailable"] = "1" if su else "0"

    sync_query_params(**{k: _param_or_none(v) for k, v in params.items()})


REPORT_PERSON_ASSIGNMENTS_LABEL = "Per-person assignments"
REPORT_COMPETITION_ASSIGNMENTS_LABEL = "Per-competition assignments"
