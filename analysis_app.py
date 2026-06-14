import os
import tempfile
import time

import streamlit as st

from database import ensure_database_for_streamlit, get_database_url

ensure_database_for_streamlit()

import pandas as pd
import traceback
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from scipy import stats

from analytics_connection import (
    get_analytics,
    get_analytics_safe,
    isolated_analytics_session,
    release_analytics_db_connection,
    run_with_isolated_analytics,
    us_linked_identity_labels_for_ui,
)
from analytics import JudgeAnalytics
from models import (
    Competition,
    Segment,
    DisciplineType,
    Judge,
    PcsScorePerJudge,
    ElementScorePerJudge,
    Element,
    SkaterSegment,
)
from sqlalchemy import text, func, case
from report_html import build_judge_report_html
from officials_competition_types import (
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_INTERNATIONAL,
    COMPETITION_SCOPE_NQS,
    COMPETITION_SCOPE_QUALIFYING,
    COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING,
    competition_load_flags_from_officials_type_id,
    format_officials_competition_type_select_label,
)
from event_regex_presets import (
    DISCIPLINE_CHOICES,
    LEVEL_CHOICES,
    effective_event_regex,
)
from scrape_storage import scrape_storage_kwargs_for_load, scrape_storage_summary
from app_query_params import (
    apply_analysis_filters_for_page,
    init_analysis_app_from_query,
    mark_query_params_applied,
    query_params_changed,
    render_query_help,
    sync_analysis_app_query_params,
)
from element_deviation_ranking import (
    FLOOR_SIGMA as _ELEM_RANK_FLOOR_SIGMA,
    MIN_BIN_COUNT as _ELEM_RANK_MIN_BIN_COUNT,
    MIN_ELEMENT_MARKING_EVENT_DATE,
    apply_min_marks_to_ranking_result,
    attach_judge_identities,
    compute_judge_detail_for_identity,
    control_scores_by_element,
    ELEMENT_RANKING_LEVEL_FILTER_ALL,
    ELEMENT_RANKING_LEVEL_FILTER_LABELS,
    element_ranking_discipline_types,
    element_ranking_discipline_names_for_scope,
    filter_element_ranking_season_years,
    unpack_element_ranking_run_params,
    memory_efficient_mode,
    benchmark_competition_scope,
    benchmark_segment_level_preset,
    benchmark_season_bounds,
    run_params_compute_key,
    run_params_ranking_compute_key,
    run_params_same_sigma_and_ranking_scope,
    uses_separate_benchmark_pool,
    validate_element_ranking_scope,
)
from element_ranking_cache import (
    collect_marks_for_run,
    element_ranking_filter_kwargs,
    element_ranking_scope_kwargs,
    load_cached_rankings,
    load_cached_sigma_params_for_run,
    load_control_by_element_for_ranking_scope,
    try_save_element_ranking_cache,
)
from element_deviation_ranking_job import (
    cleanup_ranking_artifacts,
    execute_element_deviation_rankings,
    load_control_by_element,
    load_ranking_params,
    load_ranking_result,
    rehydrate_packaged_ranking_result,
    package_element_ranking_result,
    read_ranking_error,
    start_ranking_subprocess,
    terminate_ranking_subprocess,
)

# Page configuration
st.set_page_config(page_title="Figure Skating Judge Analytics",
                   page_icon="⛸️",
                   layout="wide")

# Initialize session state
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Individual Judge Analysis"


_COMPETITION_SCOPE_LABELS = (
    "All competitions",
    "Qualifying only",
    "NQS only",
    "Sectionals & championships",
    "Championships only",
    "International",
)

_COMPETITION_SCOPE_LABEL_TO_KEY = {
    "All competitions": COMPETITION_SCOPE_ALL,
    "Qualifying only": COMPETITION_SCOPE_QUALIFYING,
    "NQS only": COMPETITION_SCOPE_NQS,
    "Sectionals & championships": COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    "Championships only": COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    "International": COMPETITION_SCOPE_INTERNATIONAL,
}


def _competition_scope_key(scope_label: str) -> str:
    """Map sidebar label to analytics ``competition_scope`` string."""
    return _COMPETITION_SCOPE_LABEL_TO_KEY.get(scope_label, COMPETITION_SCOPE_ALL)


def _streamlit_download(
    label: str,
    *,
    data: bytes,
    file_name: str,
    mime: str,
    key: str,
) -> None:
    """Download without app rerun (avoids races with URL sync and large reports)."""
    st.download_button(
        label=label,
        data=data,
        file_name=file_name,
        mime=mime,
        key=key,
        on_click="ignore",
    )


@st.cache_data(ttl=600, show_spinner=False)
def _cached_pooled_cross_judge_metrics(
    score_type: str,
    year_filter,
    competition_ids_tuple,
    discipline_ids_tuple,
    competition_scope: str,
    event_start_iso: str | None,
    event_end_iso: str | None,
    include_excess: bool,
):
    from datetime import date as _date

    with isolated_analytics_session() as analytics:
        event_start = _date.fromisoformat(event_start_iso) if event_start_iso else None
        event_end = _date.fromisoformat(event_end_iso) if event_end_iso else None
        return analytics.get_pooled_cross_judge_metrics(
            score_type=score_type,
            year_filter=year_filter,
            competition_ids=list(competition_ids_tuple) if competition_ids_tuple else None,
            discipline_type_ids=list(discipline_ids_tuple) if discipline_ids_tuple else None,
            competition_scope=competition_scope,
            include_excess=include_excess,
            event_start_date=event_start,
            event_end_date=event_end,
        )


@st.cache_data(ttl=600, show_spinner=False)
def _cached_cross_judge_heatmap_data(
    metric: str,
    score_type: str,
    year_filter,
    competition_ids_tuple,
    discipline_ids_tuple,
    competition_scope_key: str,
    event_start_iso_key: str | None,
    event_end_iso_key: str | None,
):
    from datetime import date as _date

    with isolated_analytics_session() as analytics:
        event_start = (
            _date.fromisoformat(event_start_iso_key) if event_start_iso_key else None
        )
        event_end = _date.fromisoformat(event_end_iso_key) if event_end_iso_key else None
        return analytics.get_judge_performance_heatmap_data(
            metric=metric,
            score_type=score_type,
            year_filter=year_filter,
            competition_ids=list(competition_ids_tuple) if competition_ids_tuple else None,
            discipline_type_ids=list(discipline_ids_tuple) if discipline_ids_tuple else None,
            competition_scope=competition_scope_key,
            event_start_date=event_start,
            event_end_date=event_end,
        )


@st.cache_data(ttl=600, show_spinner=False)
def _cached_cross_judge_competition_heatmap(
    metric: str,
    score_type: str,
    competition_scope_key: str,
    event_start_iso_key: str | None,
    event_end_iso_key: str | None,
):
    from datetime import date as _date

    with isolated_analytics_session() as analytics:
        event_start = (
            _date.fromisoformat(event_start_iso_key) if event_start_iso_key else None
        )
        event_end = _date.fromisoformat(event_end_iso_key) if event_end_iso_key else None
        return analytics.get_judge_competition_heatmap_data(
            metric=metric,
            score_type=score_type,
            competition_scope=competition_scope_key,
            event_start_date=event_start,
            event_end_date=event_end,
        )


@st.cache_data(ttl=300)
def _cached_competition_segment_officials(competition_id: int):
    with isolated_analytics_session() as analytics:
        return analytics.get_competition_segment_officials_display(competition_id)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_pcs_quality_analysis(
    start_season_year: str | None,
    end_season_year: str | None,
    event_start_iso: str | None,
    event_end_iso: str | None,
    discipline_ids_tuple: tuple[int, ...],
    competition_scope: str,
):
    from datetime import date as _date

    from pcs_quality_analysis import run_pcs_quality_analysis

    with isolated_analytics_session() as analytics:
        event_start = _date.fromisoformat(event_start_iso) if event_start_iso else None
        event_end = _date.fromisoformat(event_end_iso) if event_end_iso else None
        return run_pcs_quality_analysis(
            analytics,
            start_season_year=start_season_year,
            end_season_year=end_season_year,
            event_start_date=event_start,
            event_end_date=event_end,
            discipline_type_ids=list(discipline_ids_tuple),
            competition_scope=competition_scope,
        )


def _streamlit_safe_judge_pivot_display(grid: pd.DataFrame) -> pd.DataFrame:
    """
    Judge × segment pivot tables used to mix int counts with '' after replacing zeros,
    which breaks PyArrow when Streamlit serializes the dataframe (column names are judge names).
    Normalize every cell to str: blank for zero / NA, else decimal digits only.
    """
    def cell(v):
        if pd.isna(v):
            return ""
        try:
            iv = int(v)
            return "" if iv == 0 else str(iv)
        except (TypeError, ValueError):
            s = str(v).strip()
            return "" if s in ("", "0", "0.0") else s

    return grid.apply(lambda col: col.map(cell))


def _competition_results_index_url(results_url: str | None) -> str | None:
    """USFS/IJS results page URL for links (see ``ijs_results_urls.results_page_url``)."""
    from ijs_results_urls import results_page_url

    return results_page_url(results_url)


def _format_competition_event_dates(start, end) -> str | None:
    if start is None and end is None:
        return None
    if start is not None and end is not None and start != end:
        return f"{start.isoformat()} – {end.isoformat()}"
    d = start if start is not None else end
    return d.isoformat() if d is not None else None


def _render_competition_analysis_header(comp: Competition) -> None:
    """Dates, location, and results link when present in the database."""
    meta: list[str] = []
    dates = _format_competition_event_dates(comp.start_date, comp.end_date)
    if dates:
        meta.append(dates)
    location = (comp.location or "").strip()
    if location:
        meta.append(location)
    if meta:
        st.markdown(" · ".join(meta))
    results_href = _competition_results_index_url(comp.results_url)
    if results_href:
        st.markdown(f"[Competition results]({results_href})")


# Main title
st.title("⛸️ Figure Skating Judge Performance Analytics")

# Navigation (paths relative to this file so cwd does not hide "Load Competition")
import os as _os

_REPO_ROOT = _os.path.dirname(_os.path.abspath(__file__))
_DOWNLOAD_RESULTS_PY = _os.path.join(_REPO_ROOT, "downloadResults.py")
_nav_pages = [
    "Individual Judge Analysis",
    "Cross-Judge Benchmarking",
    "PCS Quality Analysis",
    "PCS Deviation Analysis",
    "Element Deviation Ranking Analysis",
    "Temporal Trend Analysis",
    "Panel size benchmarks",
    "Rule Errors Analysis",
    "Competition Analysis",
]
if _os.path.isfile(_DOWNLOAD_RESULTS_PY):
    _nav_pages.append("Load Competition")

# Persisted selectbox state can reference a removed/renamed label after deploy; coerce so the
# sidebar always stays usable (otherwise Streamlit may error or show an empty/wrong selection).
if st.session_state.get("primary_nav_page") == "Panel benchmarks":
    st.session_state.primary_nav_page = "Panel size benchmarks"
if (
    "primary_nav_page" not in st.session_state
    or st.session_state.primary_nav_page not in _nav_pages
):
    st.session_state.primary_nav_page = _nav_pages[0]

_url_params_changed = query_params_changed()

init_analysis_app_from_query(_nav_pages, from_url=_url_params_changed)

# Use radio, not selectbox: when the last item ("Load Competition") is selected, the native
# dropdown often scrolls the menu so the first option sits above the visible area — it looks
# like "Individual Judge Analysis" disappeared even though it is still in the list.
page = st.sidebar.radio(
    "Select Analysis Type",
    _nav_pages,
    key="primary_nav_page",
)

apply_analysis_filters_for_page(page, from_url=_url_params_changed)


def _individual_judge_protocol_competition_pairs(
    judge_competitions,
    pcs_df,
    element_df,
    year_filter,
    competition_ids,
):
    """Pairs for the protocol-roles table: scored comps plus judge comps after year/competition filters."""
    pairs = set()
    for df in (pcs_df, element_df):
        if df is None or df.empty:
            continue
        if "competition_name" not in df.columns or "year" not in df.columns:
            continue
        sub = df[["competition_name", "year"]].drop_duplicates()
        for _, r in sub.iterrows():
            pairs.add((str(r["competition_name"]), str(r["year"])))

    for comp_id, name, year in judge_competitions:
        if year_filter is not None and str(year) != str(year_filter):
            continue
        if competition_ids is not None and comp_id not in competition_ids:
            continue
        pairs.add((str(name), str(year)))

    def sort_key(p):
        try:
            yi = int(p[1])
        except (TypeError, ValueError):
            yi = 0
        return (-yi, p[0].lower())

    return sorted(pairs, key=sort_key)


def _identity_group_options(analytics):
    """Labels and judge-id lists for selects; merges aliases sharing one directory official."""
    groups = analytics.get_judge_analysis_identity_groups()
    labels = [g["label"] for g in groups]
    label_to_ids = {g["label"]: g["judge_ids"] for g in groups}
    return labels, label_to_ids


def _cross_judge_pooled_metrics_df(pm: dict, score_type: str) -> pd.DataFrame:
    """Single-table display for score-weighted pooled benchmark metrics."""
    rows = []
    rows.append(("Total scores", pm["total_scores"]))
    if score_type in ("both", "pcs"):
        rows.append(("PCS scores (in pool)", pm["pcs_scores"]))
    if score_type in ("both", "element"):
        rows.append(("Element scores (in pool)", pm["element_scores"]))
    rows.extend(
        [
            ("Throwouts (count)", pm["throwouts"]),
            ("Throwout rate (%)", round(pm["throwout_rate_pct"], 4)),
            ("Anomalies (count)", pm["anomalies"]),
            ("Anomaly rate (%)", round(pm["anomaly_rate_pct"], 4)),
            ("Rule errors (count)", pm["rule_errors"]),
            ("Rule error rate (%)", round(pm["rule_error_rate_pct"], 4)),
            ("Mean |deviation| per score", round(pm["avg_abs_deviation"], 6)),
        ]
    )
    if "total_excess_anomalies" in pm:
        rows.append(
            (
                "Total excess anomalies (summed across judges)",
                pm["total_excess_anomalies"],
            )
        )
    return pd.DataFrame(rows, columns=["Metric", "Value"])


def _render_pooled_benchmark_block(pm: dict, score_type: str) -> None:
    st.subheader("Pooled metrics (score-weighted)")
    st.caption(
        "Rates use total counts divided by total PCS/element scores in scope — "
        "each score counts equally (judges with more segments weigh more)."
    )
    st.dataframe(
        _cross_judge_pooled_metrics_df(pm, score_type),
        width="stretch",
        hide_index=True,
    )


def _heatmap_metric_distribution_summary(series: pd.Series, count_label: str) -> pd.DataFrame:
    """Descriptive stats for the metric column in cross-judge views."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return pd.DataFrame()
    std = float(s.std(ddof=1)) if len(s) > 1 else 0.0
    rows = [
        (count_label, int(len(s))),
        ("Mean", round(float(s.mean()), 4)),
        ("Median", round(float(s.median()), 4)),
        ("Std dev", round(std, 4)),
        ("Min", round(float(s.min()), 4)),
        ("Max", round(float(s.max()), 4)),
        ("25th percentile", round(float(s.quantile(0.25)), 4)),
        ("75th percentile", round(float(s.quantile(0.75)), 4)),
    ]
    return pd.DataFrame(rows, columns=["Statistic", "Value"])


def _plot_summary_normal_curve(values: pd.Series, metric_label: str) -> None:
    """Normal curve N(sample mean, sample SD) with σ markers and observation rug."""
    s = pd.to_numeric(values, errors="coerce").dropna()
    if s.empty:
        return

    n = int(len(s))
    mu = float(s.mean())
    sigma = float(s.std(ddof=1)) if n > 1 else 0.0
    ymax_ref = 1.0

    fig = go.Figure()

    if n < 2 or sigma <= 0:
        fig.add_trace(
            go.Scatter(
                x=s.values,
                y=np.zeros(n),
                mode="markers",
                marker=dict(size=10, color="#c0392b"),
                name="Values",
            )
        )
        fig.update_layout(
            title=f"{metric_label}: single value or zero spread — no Gaussian fit (n={n})",
            xaxis_title=metric_label,
            yaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
            showlegend=False,
        )
        st.plotly_chart(fig, width="stretch")
        return

    lo = min(float(s.min()), mu - 3.6 * sigma)
    hi = max(float(s.max()), mu + 3.6 * sigma)
    xs = np.linspace(lo, hi, 500)
    ys = stats.norm.pdf(xs, mu, sigma)
    ymax_ref = float(np.max(ys))

    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys,
            mode="lines",
            name="Normal N(μ̂, σ̂)",
            fill="tozeroy",
            fillcolor="rgba(192, 57, 43, 0.14)",
            line=dict(color="#c0392b", width=2.4),
            hovertemplate="x=%{x:.4g}<br>density=%{y:.5g}<extra></extra>",
        )
    )

    # Mean and ±1σ … ±3σ vertical guides (in σ units from μ)
    sigma_styles = [
        (0, "solid", "#2c3e50", 2.6, "μ (mean)"),
        (1, "dot", "#2980b9", 1.35, None),
        (2, "dash", "#8e44ad", 1.0, None),
        (3, "dash", "#7f8c8d", 0.75, None),
    ]
    for k, dash, color, width, label in sigma_styles:
        if k == 0:
            fig.add_shape(
                type="line",
                x0=mu,
                x1=mu,
                y0=0,
                y1=ymax_ref * 1.08,
                line=dict(color=color, width=width, dash=dash),
            )
            fig.add_annotation(
                x=mu,
                y=ymax_ref * 1.06,
                text=label,
                showarrow=False,
                font=dict(size=11, color=color),
                yshift=0,
            )
        else:
            for sign in (-1, 1):
                xv = mu + sign * k * sigma
                fig.add_shape(
                    type="line",
                    x0=xv,
                    x1=xv,
                    y0=0,
                    y1=ymax_ref * 1.02,
                    line=dict(color=color, width=width, dash=dash),
                )
                fig.add_annotation(
                    x=xv,
                    y=ymax_ref * (1.02 + 0.05 * (4 - k)),
                    text=f"{sign * k:+d}σ",
                    showarrow=False,
                    font=dict(size=10, color=color),
                )

    # Rug: actual observations along the x-axis
    rug_y = -0.06 * ymax_ref
    fig.add_trace(
        go.Scatter(
            x=s.values,
            y=np.full(n, rug_y),
            mode="markers",
            marker=dict(
                symbol="line-ns-open",
                line=dict(width=1.5, color="rgba(44, 62, 80, 0.55)"),
                size=14,
            ),
            name="Observations",
        )
    )

    z_min = (float(s.min()) - mu) / sigma
    z_max = (float(s.max()) - mu) / sigma
    z_med = (float(s.median()) - mu) / sigma

    fig.update_layout(
        title=f"Normal approximation — {metric_label} "
        f"(μ̂={mu:.4g}, σ̂={sigma:.4g}, n={n})",
        xaxis_title=metric_label,
        yaxis_title="Probability density",
        yaxis=dict(range=[rug_y * 2.2, ymax_ref * 1.22]),
        showlegend=True,
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
        margin=dict(t=56),
    )

    st.plotly_chart(fig, width="stretch")
    st.caption(
        f"Curve uses the sample mean and sample standard deviation of the values in this view. "
        f"Vertical marks show the mean (μ) and ±1σ … ±3σ. Distance from mean in σ: "
        f"min ≈ {z_min:+.2f}, median ≈ {z_med:+.2f}, max ≈ {z_max:+.2f}."
    )

def cross_judge_benchmarking_page():
    """Cross-judge metrics: judge-level bars or judge×competition matrix."""
    st.header("Cross-judge benchmarking")
    st.caption(
        "Compare judges using the same metric definitions as elsewhere in this app. "
        "Judges linked to the same directory official in **Admin → Judge ↔ directory matcher** "
        "are combined into one row (same as Individual Judge Analysis). "
        "Average deviation uses absolute panel mean deviation per judge "
        "(PCS and elements combined with score-weighted averaging when both are selected). "
        "Optional competition scope filters linked officials types on each competition. "
        "When any scope other than all competitions is selected, discipline filters use "
        "Singles / Pairs / Ice Dance / Synchronized only."
    )

    # Heatmap configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)

    with col1:
        heatmap_type = st.selectbox(
            "View",
            ["Judge Overview", "Judge vs Competition"],
            key="cross_judge_view",
        )

    with col2:
        metric = st.selectbox("Performance Metric", [
            "throwout_rate", "anomaly_rate", "rule_error_rate", "avg_deviation", "excess_anomalies", "rule_errors"
        ],
                              key="cross_judge_metric",
                              format_func=lambda x: {
                                  "throwout_rate": "Throwout Rate (%)",
                                  "anomaly_rate": "Anomaly Rate (%)",
                                  "rule_error_rate": "Rule Error Rate (%)",
                                  "avg_deviation": "Avg deviation (abs panel mean)",
                                  "excess_anomalies": "Total Excess Anomalies",
                                  "rule_errors": "Total Rule Errors"
                              }[x])

    with col3:
        score_type = st.selectbox(
            "Score Type",
            ["both", "pcs", "element"],
            key="cross_judge_score_type",
            format_func=lambda x: {
                                      "both": "Combined (PCS + Elements)",
                                      "pcs": "PCS Only",
                                      "element": "Elements Only"
                                  }[x])

    competition_scope_label = st.selectbox(
        "Competition scope",
        list(_COMPETITION_SCOPE_LABELS),
        index=0,
        key="cross_judge_competition_scope",
        help=(
            "Qualifying: linked officials type is set and not id 11 (nonqualifying). "
            "NQS: linked officials type id 10. "
            "Sectionals & championships: types 1–9 (excludes NQS and nonqualifying). "
            "Championships only: types 4 (US Championships) and 8 (US Synchro Championships)."
        ),
    )
    competition_scope = _competition_scope_key(competition_scope_label)

    use_event_dates = st.checkbox(
        "Filter by competition event dates",
        key="cross_judge_use_event_dates",
        help=(
            "Uses each competition's start date when set, otherwise its end date. "
            "Events with neither date are excluded when this filter is on."
        ),
    )
    cross_event_start = None
    cross_event_end = None
    if use_event_dates:
        analytics_dates = get_analytics_safe()
        date_min, date_max = analytics_dates.get_competition_event_date_bounds(
            competition_scope=competition_scope,
        )
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            cross_event_start = st.date_input(
                "Event on or after",
                value=date_min,
                min_value=date_min,
                max_value=date_max,
                key="cross_judge_start_date",
            )
        with date_col2:
            cross_event_end = st.date_input(
                "Event on or before",
                value=date_max,
                min_value=date_min,
                max_value=date_max,
                key="cross_judge_end_date",
            )
        if cross_event_start > cross_event_end:
            st.warning("Start date is after end date; results may be empty.")

    event_start_iso = (
        cross_event_start.isoformat()
        if use_event_dates and cross_event_start
        else None
    )
    event_end_iso = (
        cross_event_end.isoformat() if use_event_dates and cross_event_end else None
    )

    metric_names = {
        "throwout_rate": "Throwout Rate (%)",
        "anomaly_rate": "Anomaly Rate (%)",
        "rule_error_rate": "Rule Error Rate (%)",
        "avg_deviation": "Avg deviation (abs panel mean)",
        "excess_anomalies": "Total Excess Anomalies",
        "rule_errors": "Total Rule Errors"
    }

    us_officials_only = st.checkbox(
        "US directory officials only",
        key="cross_judge_us_officials_only",
        help=(
            "Charts and tables show only judges linked to a USFS official. "
            "Pooled benchmarks still use the full judge pool."
        ),
    )

    if heatmap_type == "Judge Overview":
        # Filters for judge overview
        st.subheader("Filters")
        col1, col2, col3 = st.columns(3)

        with col1:
            analytics = get_analytics_safe()
            years = analytics.get_years()
            year_filter = st.selectbox(
                "Filter by Year",
                ["All Years"] + years,
                key="cross_judge_year",
            )
            year_filter = None if year_filter == "All Years" else year_filter

        with col2:
            competitions = analytics.get_competitions(
                competition_scope=competition_scope,
                event_start_date=cross_event_start if use_event_dates else None,
                event_end_date=cross_event_end if use_event_dates else None,
            )
            competition_names = [
                f"{name} ({year})" for comp_id, name, year in competitions
            ]
            selected_competitions = st.multiselect(
                "Filter by Competitions",
                competition_names,
                key="cross_judge_competitions",
            )
            competition_ids = [
                comp_id for comp_id, name, year in competitions
                if f"{name} ({year})" in selected_competitions
            ] if selected_competitions else None

        with col3:
            discipline_types = (
                analytics.qualifying_event_segment_discipline_types()
                if competition_scope != COMPETITION_SCOPE_ALL
                else analytics.get_discipline_types()
            )
            discipline_names = [name for dt_id, name in discipline_types]
            selected_disciplines = st.multiselect(
                "Filter by Discipline Type",
                discipline_names,
                key="cross_judge_disciplines",
            )
            discipline_ids = [
                dt_id for dt_id, name in discipline_types
                if name in selected_disciplines
            ] if selected_disciplines else None

        comp_ids_tuple = tuple(competition_ids) if competition_ids else None
        disc_ids_tuple = tuple(discipline_ids) if discipline_ids else None
        include_excess = metric == "excess_anomalies"

        with st.spinner("Loading data…"):
            pm = _cached_pooled_cross_judge_metrics(
                score_type,
                year_filter,
                comp_ids_tuple,
                disc_ids_tuple,
                competition_scope,
                event_start_iso,
                event_end_iso,
                include_excess,
            )
            heatmap_df = _cached_cross_judge_heatmap_data(
                metric,
                score_type,
                year_filter,
                comp_ids_tuple,
                disc_ids_tuple,
                competition_scope,
                event_start_iso,
                event_end_iso,
            )
        _render_pooled_benchmark_block(pm, score_type)

        if us_officials_only:
            from judge_official_display_filter import filter_cross_judge_dataframe

            n_before = len(heatmap_df)
            heatmap_df = filter_cross_judge_dataframe(
                heatmap_df, us_linked_identity_labels_for_ui()
            )
            if n_before and len(heatmap_df) < n_before:
                st.caption(
                    f"US officials display filter: **{len(heatmap_df)}** of {n_before} "
                    "judges are linked to the USFS directory."
                )

        if heatmap_df.empty:
            st.warning("No data found for selected filters")
            return

        # Sort by metric value for better visualization
        heatmap_df_sorted = heatmap_df.sort_values('metric_value',
                                                   ascending=True)

        summary_df = _heatmap_metric_distribution_summary(
            heatmap_df_sorted["metric_value"], "Judges (count)"
        )
        if not summary_df.empty:
            st.subheader("Summary statistics")
            st.dataframe(summary_df, width="stretch", hide_index=True)
            _plot_summary_normal_curve(
                heatmap_df_sorted["metric_value"], metric_names[metric]
            )

        fig = px.bar(
            heatmap_df_sorted,
            x='metric_value',
            y='judge_name',
            orientation='h',
            title=
            f"Judge overview: {metric_names[metric]} ({score_type.upper()})",
            labels={
                'metric_value': metric_names[metric],
                'judge_name': 'Judge'
            },
            color='metric_value',
            color_continuous_scale='Reds')

        fig.update_layout(height=max(400, len(heatmap_df_sorted) * 25))
        st.plotly_chart(fig, width="stretch")

        # Show data table
        st.subheader("Judge-level data")
        display_df = heatmap_df_sorted[[
            'judge_name', 'metric_value', 'total_scores'
        ]].copy()
        display_df.columns = [
            'Judge', metric_names[metric], 'Total Scores'
        ]
        st.dataframe(display_df, width="stretch")

    else:  # Judge vs Competition
        include_excess = metric == "excess_anomalies"
        with st.spinner("Loading judge vs competition data…"):
            pm_jc = _cached_pooled_cross_judge_metrics(
                score_type,
                None,
                None,
                None,
                competition_scope,
                event_start_iso,
                event_end_iso,
                include_excess,
            )
            heatmap_df = _cached_cross_judge_competition_heatmap(
                metric,
                score_type,
                competition_scope,
                event_start_iso,
                event_end_iso,
            )
        _render_pooled_benchmark_block(pm_jc, score_type)

        if us_officials_only:
            from judge_official_display_filter import filter_cross_judge_dataframe

            analytics_jc = get_analytics_safe()
            n_cells_before = len(heatmap_df)
            n_judges_before = (
                int(heatmap_df["judge_name"].nunique()) if not heatmap_df.empty else 0
            )
            heatmap_df = filter_cross_judge_dataframe(
                heatmap_df, us_linked_identity_labels_for_ui()
            )
            n_judges_after = (
                int(heatmap_df["judge_name"].nunique()) if not heatmap_df.empty else 0
            )
            if n_cells_before and n_judges_after < n_judges_before:
                st.caption(
                    f"US officials display filter: **{n_judges_after}** of {n_judges_before} "
                    "judges are linked to the USFS directory."
                )

        if heatmap_df.empty:
            st.warning("No data found for judge vs competition analysis")
            return

        summary_df = _heatmap_metric_distribution_summary(
            heatmap_df["metric_value"], "Judge × competition cells (count)"
        )
        if not summary_df.empty:
            st.subheader("Summary statistics")
            st.dataframe(summary_df, width="stretch", hide_index=True)
            _plot_summary_normal_curve(
                heatmap_df["metric_value"], metric_names[metric]
            )

        # Create pivot table for heatmap
        pivot_df = heatmap_df.pivot(index='judge_name',
                                    columns='competition',
                                    values='metric_value')

        # Create heatmap
        fig = px.imshow(
            pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            aspect='auto',
            color_continuous_scale='Reds',
            title=
            f"Judge vs competition: {metric_names[metric]} ({score_type.upper()})"
        )

        fig.update_xaxes(side="bottom")
        fig.update_layout(xaxis={'categoryorder': 'category ascending'},
                          yaxis={'categoryorder': 'category ascending'},
                          height=max(400,
                                     len(pivot_df.index) * 25))

        st.plotly_chart(fig, width="stretch")

        # Show raw data
        st.subheader("Cell-level data")
        display_df = heatmap_df[[
            'judge_name', 'competition', 'metric_value', 'total_scores'
        ]].copy()
        display_df.columns = [
            'Judge', 'Competition', metric_names[metric], 'Total Scores'
        ]
        st.dataframe(display_df, width="stretch")


def _finalize_element_ranking_job() -> None:
    """If a background ranking process finished, load or record its outcome."""
    proc = st.session_state.get("element_ranking_proc")
    if proc is None or proc.is_alive():
        return

    pickle_path = st.session_state.get("element_ranking_pickle_path")
    exitcode = proc.exitcode
    st.session_state.element_ranking_proc = None

    if exitcode == 0 and pickle_path and os.path.isfile(pickle_path):
        loaded = load_ranking_result(pickle_path)
        rp = st.session_state.get("element_ranking_run_params")
        if rp is not None:
            loaded = run_with_isolated_analytics(
                _enrich_ranking_result_for_drilldown,
                rp,
                loaded,
            )
        st.session_state.element_ranking_result = loaded
        _persist_element_ranking_judge_summary_pool(
            loaded, run_params=rp
        )
        st.session_state.element_ranking_status = "done"
        st.session_state.pop("element_ranking_error_msg", None)
        if rp is not None:

            def _persist_ranking_cache(analytics, run_params, ranking_result):
                return try_save_element_ranking_cache(
                    analytics.session,
                    analytics,
                    run_params,
                    ranking_result,
                )

            ok, err = run_with_isolated_analytics(
                _persist_ranking_cache,
                rp,
                st.session_state.element_ranking_result,
            )
            if ok:
                st.session_state.element_ranking_cache_saved = True
            elif err:
                st.session_state.element_ranking_cache_error = err
    elif exitcode is not None and exitcode < 0:
        st.session_state.element_ranking_status = "cancelled"
        st.session_state.pop("element_ranking_result", None)
    else:
        st.session_state.element_ranking_status = "error"
        st.session_state.pop("element_ranking_result", None)
        err = read_ranking_error(pickle_path) if pickle_path else None
        st.session_state.element_ranking_error_msg = (
            err or f"Analysis process exited with code {exitcode}."
        )

    handle = st.session_state.get("element_ranking_proc")
    params_path = getattr(handle, "params_path", None) if handle else None
    cleanup_ranking_artifacts(pickle_path, params_path)
    st.session_state.element_ranking_proc = None
    st.session_state.element_ranking_pickle_path = None


def _stop_element_ranking_process() -> None:
    """Terminate child process and remove temp files without changing UI status."""
    handle = st.session_state.get("element_ranking_proc")
    terminate_ranking_subprocess(handle)
    pickle_path = st.session_state.get("element_ranking_pickle_path")
    params_path = getattr(handle, "params_path", None) if handle else None
    cleanup_ranking_artifacts(pickle_path, params_path)
    st.session_state.element_ranking_proc = None
    st.session_state.element_ranking_pickle_path = None


def _persist_element_ranking_judge_summary_pool(
    result: dict | None,
    *,
    run_params: tuple | None = None,
) -> None:
    """Keep the unfiltered judge pool for minimum-marks re-filtering without a re-run."""
    if not result:
        return
    judge_all = result.get("judge_summary_all")
    if isinstance(judge_all, pd.DataFrame) and not judge_all.empty:
        st.session_state["element_ranking_judge_summary_all"] = judge_all
        if run_params is not None:
            st.session_state["element_ranking_judge_summary_scope_key"] = (
                run_params_ranking_compute_key(run_params)
            )
        return
    marking = result.get("marking")
    if isinstance(marking, pd.DataFrame) and not marking.empty:
        from element_deviation_ranking import judge_summary_from_marking_display

        rebuilt = judge_summary_from_marking_display(marking)
        if rebuilt is not None and not rebuilt.empty:
            st.session_state["element_ranking_judge_summary_all"] = rebuilt
            if run_params is not None:
                st.session_state["element_ranking_judge_summary_scope_key"] = (
                    run_params_ranking_compute_key(run_params)
                )


def _element_ranking_scoped_judge_summary_pool(
    base_result: dict,
    run_params: tuple,
) -> pd.DataFrame | None:
    """
    Session fallback pool for minimum-marks re-filtering.

    Only used when the saved result lacks ``judge_summary_all`` and the pool was
    recorded for the same ranking scope (segment levels, seasons, etc.).
    """
    existing = base_result.get("judge_summary_all")
    if isinstance(existing, pd.DataFrame) and not existing.empty:
        return None
    pool = st.session_state.get("element_ranking_judge_summary_all")
    if not isinstance(pool, pd.DataFrame) or pool.empty:
        return None
    pool_scope = st.session_state.get("element_ranking_judge_summary_scope_key")
    if pool_scope != run_params_ranking_compute_key(run_params):
        return None
    return pool


def _cancel_element_ranking_job() -> None:
    _stop_element_ranking_process()
    st.session_state.element_ranking_status = "cancelled"
    st.session_state.pop("element_ranking_result", None)
    st.session_state.pop("element_ranking_error_msg", None)


def _element_ranking_use_subprocess() -> bool:
    """
    Run analysis in a child process (keeps Streamlit responsive; child RAM is freed on exit).

    Two Heroku **web** dynos do not add memory for one request — each dyno still has its own
    limit. Set ``ELEMENT_RANKING_NO_SUBPROCESS=1`` to run in-process instead.
    """
    if os.environ.get("ELEMENT_RANKING_NO_SUBPROCESS", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return False
    return True


def _run_element_ranking_compute(run_params: tuple, *, package: bool = True) -> dict:
    """Run rankings in-process; skip ``st.cache_data`` on memory-limited hosts."""
    if memory_efficient_mode():
        result = execute_element_deviation_rankings(run_params)
    else:
        result = _cached_element_deviation_rankings(run_params)
    if package:
        base = st.session_state.get("element_ranking_pickle_path")
        if not base:
            fd, base = tempfile.mkstemp(prefix="elem_rank_", suffix=".pkl")
            os.close(fd)
            st.session_state.element_ranking_pickle_path = base
        return package_element_ranking_result(result, base)
    return result


@st.cache_data(ttl=300)
def _cached_element_deviation_rankings(run_params: tuple):
    from element_deviation_ranking import compute_element_deviation_rankings_from_run_params

    with isolated_analytics_session() as analytics:
        return compute_element_deviation_rankings_from_run_params(analytics, run_params)


def _element_ranking_rankings_column_config() -> dict:
    return {
        "rank": st.column_config.NumberColumn(
            "Rank",
            format="%d",
            help="Order by marking score (1 = lowest M = closest to the control-score model).",
        ),
        "Marking score": st.column_config.NumberColumn(
            "Marking score",
            format="%.4f",
            help=(
                "M = √(mean(m²)) over element marks, with m = (GOE − panel median) / σ̂. "
                "Lower means closer to the panel-median / σ̂ model (not necessarily "
                "“better” judging in absolute terms)."
            ),
        ),
        "Element marks": st.column_config.NumberColumn(
            "Element marks",
            format="%d",
            help="Number of element GOE marks for this judge identity after filters.",
        ),
        "Mean GOE bias": st.column_config.NumberColumn(
            "Mean GOE bias",
            format="%+.3f",
            help=(
                "Signed average GOE − panel median. Positive = tends to mark above "
                "the panel median; negative = tends to mark below."
            ),
        ),
        "Mean |error|": st.column_config.NumberColumn(
            "Mean |error|",
            format="%.3f",
            help=(
                "Average |GOE − panel median GOE| in raw GOE units (before σ̂ normalization)."
            ),
        ),
        "Mean σ̂": st.column_config.NumberColumn(
            "Mean σ̂",
            format="%.3f",
            help=(
                "Average intrinsic spread σ̂ applied to this judge’s marks (fitted bin, "
                "neighbor bin, or fallback)."
            ),
        ),
        "Mean |m|": st.column_config.NumberColumn(
            "Mean |m|",
            format="%.3f",
            help=(
                "Average |m| where m = (GOE − panel median) / σ̂. Complements marking score "
                "(which uses RMS of m)."
            ),
        ),
    }


def _element_ranking_sigma_bins_column_config() -> dict:
    return {
        "Control GOE": st.column_config.NumberColumn(
            "Control GOE",
            format="%d",
            help="Panel median GOE for the bin, rounded to the nearest integer.",
        ),
        "Marks in bin": st.column_config.NumberColumn(
            "Marks in bin",
            format="%d",
            help=(
                "Element marks in the filtered data for this discipline, element type, "
                "and control GOE (all judges combined)."
            ),
        ),
        "Error stdev (all marks)": st.column_config.NumberColumn(
            "Error stdev (all marks)",
            format="%.4f",
            help=(
                "Sample standard deviation of (judge GOE − panel median GOE) within the bin."
            ),
        ),
        "σ̂ used in model": st.column_config.NumberColumn(
            "σ̂ used in model",
            format="%.4f",
            help=(
                "σ̂ fitted for this bin when it meets the minimum mark count (floored at "
                "Floor σ̂). Empty if the bin was not included in the model."
            ),
        ),
        "In model": st.column_config.CheckboxColumn(
            "In model",
            help=(
                "Checked when this bin had enough marks to fit σ̂ and that value is used "
                "for marks in the bin (before neighbor / fallback lookup)."
            ),
        ),
    }


def _element_ranking_control_table(
    result: dict,
    analytics: JudgeAnalytics,
    run_params: tuple,
) -> pd.DataFrame:
    """Panel median GOE per element (sidecar, embedded, or rebuilt from ranking-scope marks)."""
    ctrl = load_control_by_element(result)
    if not ctrl.empty:
        return ctrl

    cache_key = ("element_ranking_control", run_params_compute_key(run_params))
    cached = st.session_state.get(cache_key)
    if isinstance(cached, pd.DataFrame) and not cached.empty:
        return cached

    ctrl = load_control_by_element_for_ranking_scope(
        analytics.session, analytics, run_params
    )
    if not ctrl.empty:
        st.session_state[cache_key] = ctrl
        return ctrl

    marks = collect_marks_for_run(
        analytics,
        **element_ranking_scope_kwargs(run_params),
        cache_only=False,
        persist_shards=False,
    )
    if marks.empty:
        return pd.DataFrame()
    if "judge_id" in marks.columns:
        marks = attach_judge_identities(marks, analytics)
    ctrl = control_scores_by_element(marks)
    if not ctrl.empty:
        st.session_state[cache_key] = ctrl
    return ctrl


def _element_ranking_sigma_params(
    result: dict,
    run_params: tuple | None,
) -> dict:
    """σ̂ bin lookup table for drill-down (inline, sidecar, or DB cache)."""
    params = load_ranking_params(result)
    if params:
        return params
    rehydrated = rehydrate_packaged_ranking_result(result)
    params = load_ranking_params(rehydrated)
    if params:
        return params
    if run_params is None:
        return {}
    cached = run_with_isolated_analytics(
        lambda analytics, rp: load_cached_sigma_params_for_run(
            analytics.session, analytics, rp
        ),
        run_params,
    )
    return cached if isinstance(cached, dict) else {}


def _enrich_ranking_result_for_drilldown(
    analytics: JudgeAnalytics,
    run_params: tuple,
    ranking_result: dict | None,
) -> dict | None:
    """Attach panel control scores when missing (Heroku / summary-cache runs)."""
    if not ranking_result or ranking_result.get("error"):
        return ranking_result
    if not load_control_by_element(ranking_result).empty:
        return ranking_result
    ctrl = load_control_by_element_for_ranking_scope(
        analytics.session, analytics, run_params
    )
    if ctrl.empty:
        return ranking_result
    enriched = dict(ranking_result)
    enriched["control_by_element"] = ctrl
    return enriched


def _element_ranking_breakdown_failure_hint(
    result: dict,
    run_params: tuple,
    pick_judge: str,
) -> str:
    if not _element_ranking_sigma_params(result, run_params):
        return (
            "σ̂ parameters are missing from the saved run. "
            "Click **Run analysis** to refresh results."
        )
    cache_key = ("element_ranking_control", run_params_compute_key(run_params))
    ctrl = load_control_by_element(result)
    cached_ctrl = st.session_state.get(cache_key)
    if ctrl.empty and not (
        isinstance(cached_ctrl, pd.DataFrame) and not cached_ctrl.empty
    ):
        return (
            "Panel control scores could not be loaded for this filter set "
            "(needed for per-judge drill-down). If you use **precomputed cache** on "
            "Heroku, re-run "
            "`precompute_element_ranking_cache.py --all-scopes --sigma-benchmark --summaries`, "
            "then **Run analysis** again."
        )
    return (
        f"No element marks found for **{pick_judge}** under the current filters, "
        "or that identity could not be resolved. "
        "Click **Run analysis** after changing season, scope, or model parameters."
    )


def _element_ranking_load_judge_breakdown(
    result: dict,
    run_params: tuple,
    pick_judge: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """On-demand per-judge tables (discipline, element type, control GOE bins)."""
    empty = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    params = _element_ranking_sigma_params(result, run_params)
    if not params:
        return empty
    if not result.get("params"):
        patched = dict(result)
        patched["params"] = params
        st.session_state.element_ranking_result = patched
        result = patched

    detail_key = (
        "element_ranking_judge_detail",
        run_params_compute_key(run_params),
        pick_judge,
    )
    cached = st.session_state.get(detail_key)
    if isinstance(cached, tuple) and len(cached) >= 2:
        jd_c = cached[0] if isinstance(cached[0], pd.DataFrame) else pd.DataFrame()
        je_c = cached[1] if isinstance(cached[1], pd.DataFrame) else pd.DataFrame()
        jg_c = (
            cached[2]
            if len(cached) >= 3 and isinstance(cached[2], pd.DataFrame)
            else pd.DataFrame()
        )
        if not jg_c.empty and (not jd_c.empty or not je_c.empty or not jg_c.empty):
            return jd_c, je_c, jg_c
        if not jd_c.empty or not je_c.empty:
            if len(cached) >= 3:
                return jd_c, je_c, jg_c

    def _compute_judge_breakdown(analytics, ranking_result, rp, judge_name):
        control_tbl = _element_ranking_control_table(ranking_result, analytics, rp)
        if control_tbl.empty:
            return empty
        return compute_judge_detail_for_identity(
            analytics,
            judge_name,
            control_tbl,
            params,
            **element_ranking_filter_kwargs(rp),
        )

    with st.spinner(f"Loading breakdown for {pick_judge}…"):
        detail = run_with_isolated_analytics(
            _compute_judge_breakdown, result, run_params, pick_judge
        )
    if len(detail) == 2:
        detail = (*detail, pd.DataFrame())
    if detail[0].empty and detail[1].empty and detail[2].empty:
        return detail
    st.session_state[detail_key] = detail
    return detail


def _element_ranking_report_discipline_names(
    analytics: JudgeAnalytics, run_params: tuple
) -> list[str]:
    """Discipline labels included in the current ranking report scope."""
    rp = unpack_element_ranking_run_params(run_params)
    disc_tuple = rp[2]
    scope_key = rp[3]
    if disc_tuple:
        id_to_name = {
            int(dt_id): name
            for dt_id, name in element_ranking_discipline_types(analytics, scope_key)
        }
        return [
            id_to_name[int(dt_id)]
            for dt_id in disc_tuple
            if int(dt_id) in id_to_name
        ]
    return element_ranking_discipline_names_for_scope(analytics, scope_key)


def _control_goe_chart_series_label(discipline: str, element_type: str) -> str:
    return f"{discipline} — {element_type}"


def _control_goe_chart_series_options(element_detail: pd.DataFrame) -> list[str]:
    """Ordered discipline × element-type labels for the control-GOE chart multiselect."""
    if element_detail.empty or not {"Discipline", "Element type"}.issubset(
        element_detail.columns
    ):
        return []
    sort_col = (
        "Partial marking score"
        if "Partial marking score" in element_detail.columns
        else "Element marks"
    )
    scored = element_detail.sort_values(sort_col, ascending=False)
    seen: set[str] = set()
    out: list[str] = []
    for _, row in scored.iterrows():
        label = _control_goe_chart_series_label(
            str(row["Discipline"]), str(row["Element type"])
        )
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out


def _element_ranking_control_goe_chart(
    control_goe_detail: pd.DataFrame,
    element_detail: pd.DataFrame,
    *,
    min_bin_marks: int = 10,
    series_keys: list[str] | None = None,
    max_types: int = 3,
):
    """Line chart: partial marking score vs control GOE for selected element-type series."""
    if control_goe_detail.empty:
        return None
    if "Control GOE" not in control_goe_detail.columns:
        return None

    if series_keys:
        plot_series = list(series_keys)
    elif not element_detail.empty and {"Discipline", "Element type"}.issubset(
        element_detail.columns
    ):
        plot_series = _control_goe_chart_series_options(element_detail)[:max_types]
    else:
        return None

    if not plot_series:
        return None

    parts: list[pd.DataFrame] = []
    for label in plot_series:
        if " — " not in label:
            continue
        disc, etype = label.split(" — ", 1)
        mask = (control_goe_detail["Discipline"] == disc) & (
            control_goe_detail["Element type"] == etype
        )
        if min_bin_marks > 0:
            mask = mask & (control_goe_detail["Element marks"] >= min_bin_marks)
        subset = control_goe_detail.loc[mask].copy()
        if subset.empty:
            continue
        subset["Series"] = label
        parts.append(subset)

    if not parts:
        return None

    plot_df = pd.concat(parts, ignore_index=True)
    fig = px.line(
        plot_df.sort_values(["Series", "Control GOE"]),
        x="Control GOE",
        y="Partial marking score",
        color="Series",
        markers=True,
        title="Partial marking score by panel-median (control) GOE",
        labels={
            "Control GOE": "Rounded panel median GOE",
            "Partial marking score": "Partial marking score √(mean(m²))",
        },
    )
    fig.update_layout(hovermode="x unified")
    return fig


def _pcs_deviation_chart_series_label(discipline: str, component: str) -> str:
    return f"{discipline} — {component}"


def _pcs_deviation_detail_discipline_col(detail: pd.DataFrame) -> str | None:
    for col in ("Discipline", "discipline"):
        if col in detail.columns:
            return col
    return None


def _pcs_deviation_detail_component_col(detail: pd.DataFrame) -> str | None:
    for col in ("Component", "component"):
        if col in detail.columns:
            return col
    return None


def _pcs_deviation_chart_series_options(detail: pd.DataFrame) -> list[str]:
    """Ordered discipline × component labels for the PCS control-bin chart."""
    disc_col = _pcs_deviation_detail_discipline_col(detail)
    comp_col = _pcs_deviation_detail_component_col(detail)
    if detail.empty or not disc_col or not comp_col:
        return []
    sort_col = (
        "Partial marking score"
        if "Partial marking score" in detail.columns
        else "PCS marks"
    )
    scored = detail.sort_values(sort_col, ascending=False)
    seen: set[str] = set()
    out: list[str] = []
    for _, row in scored.iterrows():
        label = _pcs_deviation_chart_series_label(
            str(row[disc_col]), str(row[comp_col])
        )
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out


_PCS_DEVIATION_CHART_Y_PARTIAL = "partial_marking_score"
_PCS_DEVIATION_CHART_Y_MEAN_ABS_ERROR = "mean_abs_error"
_PCS_DEVIATION_CHART_Y_METRICS: dict[str, dict[str, str]] = {
    _PCS_DEVIATION_CHART_Y_PARTIAL: {
        "column": "Partial marking score",
        "title": "Partial marking score by panel-median PCS range",
        "ylabel": "Partial marking score √(mean(m²))",
    },
    _PCS_DEVIATION_CHART_Y_MEAN_ABS_ERROR: {
        "column": "Mean |error|",
        "title": "Mean |error| by panel-median PCS range",
        "ylabel": "Mean |error| (raw PCS units)",
    },
}


def _pcs_deviation_control_bin_chart(
    control_bin_detail: pd.DataFrame,
    component_detail: pd.DataFrame,
    *,
    min_bin_marks: int = 0,
    series_keys: list[str] | None = None,
    y_metric: str = _PCS_DEVIATION_CHART_Y_PARTIAL,
):
    """Line chart vs panel-median range for PCS components (σ̂-scaled or raw |error|)."""
    if control_bin_detail.empty or "Control bin" not in control_bin_detail.columns:
        return None

    metric = _PCS_DEVIATION_CHART_Y_METRICS.get(
        y_metric, _PCS_DEVIATION_CHART_Y_METRICS[_PCS_DEVIATION_CHART_Y_PARTIAL]
    )
    y_col = metric["column"]
    if y_col not in control_bin_detail.columns:
        return None

    disc_col = _pcs_deviation_detail_discipline_col(control_bin_detail)
    comp_col = _pcs_deviation_detail_component_col(control_bin_detail)
    if not disc_col or not comp_col:
        return None

    if series_keys:
        plot_series = list(series_keys)
    else:
        source = (
            component_detail
            if not component_detail.empty
            and _pcs_deviation_detail_discipline_col(component_detail)
            and _pcs_deviation_detail_component_col(component_detail)
            else control_bin_detail
        )
        plot_series = _pcs_deviation_chart_series_options(source)

    if not plot_series:
        return None

    parts: list[pd.DataFrame] = []
    for label in plot_series:
        if " — " not in label:
            continue
        disc, component = label.split(" — ", 1)
        mask = (control_bin_detail[disc_col] == disc) & (
            control_bin_detail[comp_col] == component
        )
        if min_bin_marks > 0:
            mask = mask & (control_bin_detail["PCS marks"] >= min_bin_marks)
        subset = control_bin_detail.loc[mask].copy()
        if subset.empty:
            continue
        subset["Series"] = label
        parts.append(subset)

    if not parts:
        return None

    plot_df = pd.concat(parts, ignore_index=True)
    fig = px.line(
        plot_df.sort_values(["Series", "Control bin"]),
        x="Control bin",
        y=y_col,
        color="Series",
        markers=True,
        title=metric["title"],
        labels={
            "Control bin": "Panel median PCS bin",
            y_col: metric["ylabel"],
        },
    )
    bins_sorted = sorted(plot_df["Control bin"].unique())
    if "Control median range" in plot_df.columns:
        range_by_bin = (
            plot_df.drop_duplicates("Control bin")
            .set_index("Control bin")["Control median range"]
            .to_dict()
        )
        fig.update_xaxes(
            tickmode="array",
            tickvals=bins_sorted,
            ticktext=[range_by_bin.get(b, str(b)) for b in bins_sorted],
        )
    fig.update_layout(hovermode="x unified")
    return fig


def _element_ranking_judge_detail_column_config() -> dict:
    return {
        "Element marks": st.column_config.NumberColumn(
            "Element marks",
            format="%d",
            help="Element marks for this judge in the row’s discipline (and element type, if shown).",
        ),
        "Partial marking score": st.column_config.NumberColumn(
            "Partial marking score",
            format="%.4f",
            help=(
                "√(mean(m²)) using only marks in this row’s slice (same formula as overall "
                "marking score, scoped to discipline or element type)."
            ),
        ),
        "Mean GOE bias": st.column_config.NumberColumn(
            "Mean GOE bias",
            format="%+.3f",
            help="Signed mean GOE − panel median in this slice; positive = above median.",
        ),
        "Mean |error|": st.column_config.NumberColumn(
            "Mean |error|",
            format="%.3f",
            help="Average |GOE − panel median| in this slice (raw GOE units).",
        ),
        "Mean σ̂": st.column_config.NumberColumn(
            "Mean σ̂",
            format="%.3f",
            help="Average σ̂ applied to marks in this discipline slice.",
        ),
        "Control GOE": st.column_config.NumberColumn(
            "Control GOE",
            format="%+d",
            help="Panel median GOE for the element, rounded (σ̂ model bin).",
        ),
    }


def _pcs_quality_methodology_text(topic: str) -> str:
    """Markdown for the PCS quality methodology dropdown."""
    texts = {
        "Overview": """
**Data** — PCS marks from competitions with event date on or after **2022-07-01**.
Judges linked to the same directory official are one identity (same as other pages).

**Per skater × segment × PCS component** — Panel reference is the **median** of all
judges’ PCS on that line. Metrics are computed separately for each judge identity,
discipline, and PCS component (SS, TR, PE, CO, etc.) present in the **filtered** window.

**Component set per discipline** — For each discipline, only PCS components that
appear somewhere in the filtered marks are used (not a fixed historical list).

**Minimum PCS marks** — Optional filter: exclude judge identities with fewer than
*N* PCS marks in the filtered range (each skater × component line counts as one mark).

**Three alignment measures (per judge × discipline × PCS component)** — Each is scaled
to 0–1 (higher = closer to the panel). They are reported and ranked **separately**;
there is no combined overall score.

| Measure | What it captures |
| --- | --- |
| Ranking | Per segment, does skater rank order match the panel? (mean across segments) |
| Bias | Are your PCS systematically high or low vs the panel? |
| Differentiation | Is your spread of marks similar to the panel’s spread? |

**Judge-level scores** — Equal weight across PCS components in each discipline, then
mark-weighted across disciplines. Each measure has its own ranking table.

""",
        "Ranking correlation score": """
For one judge, one discipline, one PCS component: each **segment** (short program,
free skate, etc.) is one **event**. Within that segment, rank all skaters on that
component and compute Spearman **ρ** between the judge’s ranks and panel medians
(needs at least **3 skaters** in the segment).

Per segment:

``segment_ranking = max(0, (ρ − 0.7) / 0.3)``  

**Ranking sub-score** = equal-weight mean of ``segment_ranking`` over segments the
judge marked (segments with &lt;3 skaters are skipped). **Spearman ρ** in the detail
table is the mean ρ across those segments. Bias and differentiation are separate
measures (not combined with ranking).
""",
        "Bias score": """
Per skater-line: ``bias = judge_PCS − panel_median_PCS``.

``average_bias = mean(bias)`` over skater-lines in that judge × discipline × component.

``bias_score = max(0, 1 − |average_bias| / 0.5)``  

0 average bias → 1; |average bias| ≥ 0.5 → 0.
""",
        "Differentiation score": """
Compare spread of judge PCS vs spread of panel medians across skater-lines (same slice).

``judge_variance = variance(judge_PCS)``  
``panel_variance = variance(panel_median)``  

``diff_score = max(0, min(1, 1 − |log(judge_variance / panel_variance)|))``  

1 when variances match; penalizes much wider or narrower marking than the panel.

``bias_tendency`` and ``spread_tendency`` summarize direction (lenient/harsh; wider/narrower).
""",
        "Judge-level aggregation": """
For each measure, **discipline score** = equal-weight mean over PCS components the
judge marked in that discipline (among components present in the filtered period).

**Judge score** = mark-weighted mean across disciplines:

``judge_score = Σ (discipline_score × PCS_marks_in_discipline) / Σ PCS_marks``  

The three measures are independent; rankings are computed separately for each.
""",
    }
    return texts.get(topic, "")


def _pcs_quality_metric_ranking_column_config(metric_label: str) -> dict:
    cfg = {
        "Rank": st.column_config.NumberColumn(
            "Rank",
            format="%d",
            width="small",
            help=f"Order by {metric_label} (1 = highest).",
        ),
        "Judge": st.column_config.TextColumn(
            "Judge",
            width="medium",
            help="Official identity (directory-linked judges grouped).",
        ),
        metric_label: st.column_config.NumberColumn(
            metric_label,
            format="%.4f",
            width="small",
            help=f"{metric_label} in [0, 1]; higher = closer to the panel on this measure.",
        ),
        "Disciplines": st.column_config.NumberColumn(
            "Disciplines",
            format="%d",
            width="small",
            help="Number of discipline types this judge scored in the filtered range.",
        ),
        "PCS marks": st.column_config.NumberColumn(
            "PCS marks",
            format="%d",
            width="small",
            help="PCS lines (judge × skater × segment × component) in the filtered range.",
        ),
    }
    if metric_label == "Ranking score":
        cfg["Mean Spearman ρ"] = st.column_config.NumberColumn(
            "Mean Spearman ρ",
            format="%.3f",
            width="small",
            help="Mean segment-level Spearman ρ across components and disciplines.",
        )
    if metric_label == "Bias score":
        cfg["Mean bias"] = st.column_config.NumberColumn(
            "Mean bias",
            format="%+.3f",
            width="small",
            help="Average judge PCS − panel median; positive = lenient, negative = harsh.",
        )
        cfg["Tendency"] = st.column_config.TextColumn(
            "Tendency",
            width="medium",
            help="Lenient, harsh, or neutral from mean bias (|mean| ≤ 0.05 → neutral).",
        )
    if metric_label == "Differentiation score":
        cfg["Var ratio"] = st.column_config.NumberColumn(
            "Var ratio",
            format="%.2f",
            width="small",
            help="Judge mark variance ÷ panel median variance (>1 wider, <1 narrower).",
        )
        cfg["Spread vs panel"] = st.column_config.TextColumn(
            "Spread vs panel",
            width="medium",
            help="Wider, narrower, or similar spread vs panel (ratio outside 0.9–1.1).",
        )
    return cfg


_PCS_METRIC_RANKING_UI: tuple[tuple[str, str, str, str], ...] = (
    (
        "ranking",
        "Ranking score",
        "Ranking — rank order vs panel",
        "Per segment, how well skater rank order on each PCS component matches "
        "the panel (segments with fewer than 3 skaters are skipped).",
    ),
    (
        "bias",
        "Bias score",
        "Bias — systematic PCS vs panel",
        "Whether PCS marks are systematically above or below panel medians, "
        "averaged across all lines in scope. A higher score means marks are "
        "generally closer to the panel median (less systematic leniency or harshness).",
    ),
    (
        "differentiation",
        "Differentiation score",
        "Differentiation — mark spread vs panel",
        "Whether the spread of this judge's PCS matches the panel's spread. "
        "See **Spread vs panel** (wider / narrower / similar) and **Var ratio** "
        "(judge variance ÷ panel variance). A higher score means spread is closer to the panel.",
    ),
)


def _pcs_quality_format_discipline_summary(disc_sub: pd.DataFrame) -> pd.DataFrame:
    """Compact columns for the per-judge discipline table."""
    if disc_sub.empty:
        return disc_sub
    out = disc_sub.copy()
    out = out.rename(
        columns={
            "discipline": "Discipline",
            "ranking_score": "Ranking",
            "bias_score": "Bias",
            "mean_bias": "Mean bias",
            "bias_tendency": "Tendency",
            "diff_score": "Differentiation",
            "variance_ratio": "Var ratio",
            "spread_tendency": "Spread",
            "n_components_scored": "Components",
            "PCS marks": "Marks",
        }
    )
    for col in ("Ranking", "Bias", "Differentiation", "Var ratio"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(4)
    if "Mean bias" in out.columns:
        out["Mean bias"] = pd.to_numeric(out["Mean bias"], errors="coerce").round(3)
    display_cols = [
        "Discipline",
        "Ranking",
        "Bias",
        "Mean bias",
        "Tendency",
        "Differentiation",
        "Var ratio",
        "Spread",
        "Components",
        "Marks",
    ]
    return out[[c for c in display_cols if c in out.columns]]


def _pcs_quality_discipline_column_config() -> dict:
    return {
        "Discipline": st.column_config.TextColumn(
            "Discipline",
            width="medium",
            help="Discipline type for this block of PCS marks.",
        ),
        "Ranking": st.column_config.NumberColumn(
            "Ranking",
            format="%.4f",
            width="small",
            help="Equal-weight mean ranking score across PCS components in this discipline.",
        ),
        "Bias": st.column_config.NumberColumn(
            "Bias",
            format="%.4f",
            width="small",
            help="Equal-weight mean bias score in this discipline.",
        ),
        "Mean bias": st.column_config.NumberColumn(
            "Mean bias",
            format="%+.3f",
            width="small",
            help="Mark-weighted mean judge − panel in this discipline.",
        ),
        "Tendency": st.column_config.TextColumn(
            "Tendency",
            width="medium",
            help="Lenient, harsh, or neutral from mean bias.",
        ),
        "Differentiation": st.column_config.NumberColumn(
            "Differentiation",
            format="%.4f",
            width="small",
            help="Equal-weight mean differentiation score in this discipline.",
        ),
        "Var ratio": st.column_config.NumberColumn(
            "Var ratio",
            format="%.2f",
            width="small",
            help="Mark-weighted variance ratio in this discipline.",
        ),
        "Spread": st.column_config.TextColumn(
            "Spread",
            width="medium",
            help="Wider, narrower, or similar vs panel spread.",
        ),
        "Components": st.column_config.NumberColumn(
            "Components",
            format="%d",
            width="small",
            help=(
                "PCS components this judge marked in this discipline "
                "(among components present in the filtered period)."
            ),
        ),
        "Marks": st.column_config.NumberColumn(
            "Marks",
            format="%d",
            width="small",
            help="PCS lines for this judge in this discipline within the filters.",
        ),
    }


def _pcs_quality_component_column_config() -> dict:
    return {
        "Discipline": st.column_config.TextColumn(
            "Discipline",
            width="medium",
            help="Discipline type for this PCS component block.",
        ),
        "Component": st.column_config.TextColumn(
            "Component",
            width="small",
            help="PCS component code (SS, TR, PE, CO, PR, TI, …).",
        ),
        "Spearman ρ": st.column_config.NumberColumn(
            "Spearman ρ",
            format="%.3f",
            width="small",
            help=(
                "Mean Spearman ρ across segments: within each segment, judge vs panel "
                "skater rank order on this component (segments with <3 skaters skipped)."
            ),
        ),
        "Segments ranked": st.column_config.NumberColumn(
            "Segments ranked",
            format="%d",
            width="small",
            help=(
                "Segments with ≥3 skaters where rank correlation was computed "
                "(each segment is one event)."
            ),
        ),
        "Ranking score": st.column_config.NumberColumn(
            "Ranking score",
            format="%.3f",
            width="small",
            help=(
                "Equal-weight mean of per-segment rank scores: each segment maps "
                "ρ to max(0, (ρ−0.7)/0.3)."
            ),
        ),
        "Mean bias": st.column_config.NumberColumn(
            "Mean bias",
            format="%+.3f",
            width="small",
            help="Average (judge PCS − panel median). Positive = tends above the panel.",
        ),
        "Tendency": st.column_config.TextColumn(
            "Tendency",
            width="medium",
            help="Lenient, harsh, or neutral from mean bias.",
        ),
        "Bias score": st.column_config.NumberColumn(
            "Bias score",
            format="%.3f",
            width="small",
            help="Scaled bias score in [0, 1].",
        ),
        "Differentiation score": st.column_config.NumberColumn(
            "Differentiation score",
            format="%.3f",
            width="small",
            help="Scaled match of mark spread vs panel.",
        ),
        "Var ratio": st.column_config.NumberColumn(
            "Var ratio",
            format="%.2f",
            width="small",
            help="Judge variance ÷ panel variance for this component.",
        ),
        "Spread": st.column_config.TextColumn(
            "Spread",
            width="medium",
            help="Wider, narrower, or similar vs panel.",
        ),
        "PCS marks": st.column_config.NumberColumn(
            "PCS marks",
            format="%d",
            width="small",
            help="Skater-segment lines for this judge × discipline × component.",
        ),
    }


def _pcs_quality_format_component_detail(detail: pd.DataFrame) -> pd.DataFrame:
    if detail.empty:
        return detail
    out = detail.copy()
    out = out.sort_values(
        ["discipline", "ranking_score", "component"],
        ascending=[True, False, True],
    )
    out = out.rename(
        columns={
            "discipline": "Discipline",
            "component": "Component",
            "spearman_rho": "Spearman ρ",
            "n_segments_ranked": "Segments ranked",
            "ranking_score": "Ranking score",
            "mean_bias": "Mean bias",
            "bias_score": "Bias score",
            "bias_tendency": "Tendency",
            "diff_score": "Differentiation score",
            "variance_ratio": "Var ratio",
            "spread_tendency": "Spread",
            "n_marks": "PCS marks",
        }
    )
    display_cols = [
        "Discipline",
        "Component",
        "Spearman ρ",
        "Segments ranked",
        "Ranking score",
        "Mean bias",
        "Tendency",
        "Bias score",
        "Differentiation score",
        "Var ratio",
        "Spread",
        "PCS marks",
    ]
    if "Var ratio" in out.columns:
        out["Var ratio"] = pd.to_numeric(out["Var ratio"], errors="coerce").round(2)
    for col in (
        "Spearman ρ",
        "Ranking score",
        "Mean bias",
        "Bias score",
        "Differentiation score",
    ):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(4)
    if "Mean bias" in out.columns:
        out["Mean bias"] = out["Mean bias"].round(3)
    return out[[c for c in display_cols if c in out.columns]]


def pcs_quality_analysis_page():
    """PCS judge quality vs panel medians for post-2022-07-01 competitions."""
    from pcs_quality_analysis import MIN_PCS_ANALYSIS_EVENT_DATE

    st.header("PCS Quality Analysis")
    from pcs_quality_analysis import PCS_QUALITY_MAX_MARKS

    st.caption(
        "Three separate alignment measures vs **panel medians** on PCS (ranking, bias, "
        "differentiation). Each has its own ranking table — they are not "
        "combined into one score. **Discipline** is required (defaults to Singles). "
        "Components per discipline are equal-weighted; disciplines "
        "are mark-weighted. Only competitions on or after "
        f"**{MIN_PCS_ANALYSIS_EVENT_DATE.isoformat()}** are included. "
        f"Large filters load up to **{PCS_QUALITY_MAX_MARKS:,}** PCS marks "
        "(set ``PCS_QUALITY_MAX_MARKS`` on the server to raise the cap). "
        "Warm shards with ``scripts/precompute_pcs_quality_cache.py`` "
        "(add ``--summaries`` for cache-only loads without loading all marks)."
    )

    with st.expander("How scores are calculated", expanded=False):
        methodology_topic = st.selectbox(
            "Topic",
            [
                "Overview",
                "Ranking correlation score",
                "Bias score",
                "Differentiation score",
                "Judge-level aggregation",
            ],
            index=0,
            key="pcs_quality_methodology",
            help="Reference only — there is no alternate scoring model on this page.",
        )
        st.markdown(_pcs_quality_methodology_text(methodology_topic))

    analytics = get_analytics_safe()

    scope_label = st.selectbox(
        "Competition scope",
        list(_COMPETITION_SCOPE_LABELS),
        index=0,
        key="pcs_quality_competition_scope",
        help="Filters competitions via linked officials competition type.",
    )
    scope_key = _competition_scope_key(scope_label)

    years = sorted(
        {str(y) for y in analytics.get_years() if y},
        reverse=True,
    )
    col_y1, col_y2 = st.columns(2)
    with col_y1:
        start_season = st.selectbox(
            "Season year from",
            ["Any"] + years,
            key="pcs_quality_start_season",
        )
        start_season_year = None if start_season == "Any" else start_season
    with col_y2:
        end_season = st.selectbox(
            "Season year to",
            ["Any"] + years,
            key="pcs_quality_end_season",
        )
        end_season_year = None if end_season == "Any" else end_season

    if start_season_year is None and end_season_year is None:
        st.warning(
            "Season year is **Any** for both bounds — this loads every PCS mark since "
            "2022-07-01 and may time out or crash on small servers. Prefer a season range "
            "or enable event dates."
        )

    use_event_dates = st.checkbox(
        "Narrow by competition event dates",
        key="pcs_quality_use_event_dates",
        help=(
            "Further restrict by event date (still at least "
            f"{MIN_PCS_ANALYSIS_EVENT_DATE.isoformat()})."
        ),
    )
    event_start_date = None
    event_end_date = None
    if use_event_dates:
        date_min, date_max = analytics.get_competition_event_date_bounds(
            competition_scope=scope_key,
        )
        date_min = max(date_min, MIN_PCS_ANALYSIS_EVENT_DATE)
        date_max = max(date_max, date_min)
        dc1, dc2 = st.columns(2)
        with dc1:
            event_start_date = st.date_input(
                "Event on or after",
                value=date_min,
                min_value=MIN_PCS_ANALYSIS_EVENT_DATE,
                max_value=date_max,
                key="pcs_quality_start_date",
            )
        with dc2:
            event_end_date = st.date_input(
                "Event on or before",
                value=date_max,
                min_value=MIN_PCS_ANALYSIS_EVENT_DATE,
                max_value=date_max,
                key="pcs_quality_end_date",
            )
        if event_start_date > event_end_date:
            st.warning("Start date is after end date; results may be empty.")

    event_start_iso = (
        event_start_date.isoformat()
        if use_event_dates and event_start_date
        else None
    )
    event_end_iso = (
        event_end_date.isoformat() if use_event_dates and event_end_date else None
    )

    if scope_key != COMPETITION_SCOPE_ALL:
        discipline_types = analytics.qualifying_event_segment_discipline_types()
    else:
        discipline_types = analytics.get_discipline_types()
    discipline_names = [name for _id, name in discipline_types if name]

    def _pcs_quality_default_discipline_selection() -> list[str]:
        if "Singles" in discipline_names:
            return ["Singles"]
        return [discipline_names[0]] if discipline_names else []

    if "pcs_quality_disciplines" not in st.session_state:
        st.session_state["pcs_quality_disciplines"] = (
            _pcs_quality_default_discipline_selection()
        )
    else:
        valid = [
            n
            for n in st.session_state["pcs_quality_disciplines"]
            if n in discipline_names
        ]
        if not valid:
            st.session_state["pcs_quality_disciplines"] = (
                _pcs_quality_default_discipline_selection()
            )
        elif valid != st.session_state["pcs_quality_disciplines"]:
            st.session_state["pcs_quality_disciplines"] = valid

    selected_disciplines = st.multiselect(
        "Discipline types",
        discipline_names,
        key="pcs_quality_disciplines",
        help="Required. Defaults to Singles. Analysis includes only the selected segment disciplines.",
    )
    discipline_ids = [
        dt_id for dt_id, name in discipline_types if name in selected_disciplines
    ]
    disciplines_ok = bool(selected_disciplines)
    if not disciplines_ok:
        st.warning("Select at least one discipline type to run or view results.")

    col_pcs_cache = st.columns([2, 1])[1]
    with col_pcs_cache:
        use_pcs_precomputed_cache = st.checkbox(
            "Use precomputed cache",
            value=True,
            key="pcs_quality_use_cache",
            help=(
                "Load cached season×discipline PCS shards (missing shards are computed)."
            ),
        )

    if "pcs_quality_min_pcs_marks" not in st.session_state:
        st.session_state["pcs_quality_min_pcs_marks"] = 0
    min_pcs_marks = st.number_input(
        "Minimum PCS marks per judge",
        min_value=0,
        step=10,
        key="pcs_quality_min_pcs_marks",
        help=(
            "Exclude judge identities with fewer PCS marks in the filtered range. "
            "Changing only this re-filters the last run (no reload)."
        ),
    )
    us_officials_only = st.checkbox(
        "US directory officials only",
        key="pcs_quality_us_officials_only",
        help=(
            "Show rankings only for judges linked to a USFS official. "
            "PCS quality scores still use the full judge pool."
        ),
    )

    run_clicked = st.button("Run analysis", type="primary", key="pcs_quality_run_btn")

    if run_clicked:
        if not disciplines_ok:
            st.error("Select at least one discipline type before running.")
        else:
            from datetime import date as _date

            from pcs_quality_cache import load_cached_pcs_quality

            event_start = (
                _date.fromisoformat(event_start_iso) if event_start_iso else None
            )
            event_end = _date.fromisoformat(event_end_iso) if event_end_iso else None
            if use_pcs_precomputed_cache:

                def _load_pcs_cache(
                    cache_analytics,
                    *,
                    start_sy,
                    end_sy,
                    ev_start,
                    ev_end,
                    disc_ids,
                    scope,
                ):
                    return load_cached_pcs_quality(
                        cache_analytics.session,
                        cache_analytics,
                        start_season_year=start_sy,
                        end_season_year=end_sy,
                        event_start_date=ev_start,
                        event_end_date=ev_end,
                        discipline_type_ids=disc_ids,
                        competition_scope=scope,
                    )

                cached = run_with_isolated_analytics(
                    _load_pcs_cache,
                    start_sy=start_season_year,
                    end_sy=end_season_year,
                    ev_start=event_start,
                    ev_end=event_end,
                    disc_ids=discipline_ids,
                    scope=scope_key,
                )
                if cached is not None:
                    st.session_state.pcs_quality_result = cached
                    st.success("Loaded precomputed PCS cache for this filter set.")
                    st.rerun()
            with st.spinner("Loading PCS marks and computing judge profiles…"):
                st.session_state.pcs_quality_result = _cached_pcs_quality_analysis(
                    start_season_year,
                    end_season_year,
                    event_start_iso,
                    event_end_iso,
                    tuple(discipline_ids),
                    scope_key,
                )

    base_result = st.session_state.get("pcs_quality_result")
    if not disciplines_ok:
        return
    if base_result is None:
        st.info("Set filters and click **Run analysis**.")
        return

    from pcs_quality_analysis import apply_min_pcs_marks_to_result

    # Drop legacy huge ``marks`` payloads from session (pre-perf runs).
    if isinstance(base_result, dict) and "marks" in base_result:
        base_result = {k: v for k, v in base_result.items() if k != "marks"}
        st.session_state.pcs_quality_result = base_result

    result = apply_min_pcs_marks_to_result(base_result, int(min_pcs_marks))
    if us_officials_only:
        from judge_official_display_filter import (
            apply_us_officials_display_filter_to_pcs_result,
        )

        n_before_us = len(result.get("profiles", pd.DataFrame()))
        result = apply_us_officials_display_filter_to_pcs_result(
            result, us_linked_identity_labels_for_ui()
        )
        n_after_us = len(result.get("profiles", pd.DataFrame()))
        if n_before_us and n_after_us < n_before_us:
            st.caption(
                f"US officials display filter: **{n_after_us}** of {n_before_us} ranked "
                "identities are linked to the USFS directory."
            )

    if result.get("error") and result["profiles"].empty:
        err = result["error"]
        if "exceeds limit" in err.lower():
            st.error(err)
        else:
            st.warning(err)
        return

    profiles = result["profiles"]
    component_detail = result.get("component_detail", pd.DataFrame())
    discipline_summary = result.get("discipline_summary", pd.DataFrame())
    n_marks = int(result.get("n_raw_marks") or 0)

    n_before = result.get("n_judges_before_min_filter")
    excluded = (
        int(n_before) - len(profiles)
        if n_before is not None and int(min_pcs_marks) > 0
        else 0
    )

    if int(min_pcs_marks) > 0:
        m1, m2, m3 = st.columns(3)
        m1.metric("PCS marks in scope", f"{n_marks:,}")
        m2.metric("Judges ranked", len(profiles))
        m3.metric(
            "Excluded (below min marks)",
            excluded,
            help=f"Judges with fewer than {int(min_pcs_marks)} PCS marks in scope",
        )
    else:
        m1, m2 = st.columns(2)
        m1.metric("PCS marks in scope", f"{n_marks:,}")
        m2.metric("Judges ranked", len(profiles))

    if profiles.empty:
        n_before = base_result.get("n_judges") or 0
        if int(min_pcs_marks) > 0 and n_before > 0:
            st.warning(
                f"No judges meet the minimum of **{int(min_pcs_marks)}** PCS marks "
                f"({n_before} judge(s) had marks in scope)."
            )
        else:
            st.info("No judge profiles could be built for the selected filters.")
        return

    with st.expander("What do the three measures mean?", expanded=False):
        st.markdown(
            """
Each **PCS component** (SS, TR, PE, CO, …) is scored separately against **panel medians**
on the same skater-segment lines. **Ranking** is computed per segment (event), then
averaged. Three scores (0–1, higher is better) are reported **independently**:

- **Ranking** — Per segment, rank-order agreement vs panel; mean across segments (≥3 skaters each)  
- **Bias** — Average PCS above/below panel (penalizes systematic leniency/harshness)  
- **Differentiation** — Whether your mark spread matches the panel’s spread  

Within each discipline: equal weight across components you marked.  
Across disciplines: mark-weighted. Open **How scores are calculated** above for formulas.
            """
        )

    metric_rankings = result.get("metric_rankings") or {}
    st.subheader("Rankings by measure")
    st.caption(
        "Higher scores (closer to 1) indicate closer alignment with the panel on that measure."
    )
    for slug, label, title, description in _PCS_METRIC_RANKING_UI:
        rank_df = metric_rankings.get(slug, pd.DataFrame())
        if rank_df is None or rank_df.empty:
            continue
        st.markdown(f"#### {title}")
        st.caption(description)
        st.dataframe(
            rank_df,
            width="stretch",
            hide_index=True,
            column_config=_pcs_quality_metric_ranking_column_config(label),
        )
        _streamlit_download(
            f"Download {slug} rankings CSV",
            data=rank_df.to_csv(index=False).encode("utf-8"),
            file_name=f"pcs_quality_{slug}_rankings.csv",
            mime="text/csv",
            key=f"pcs_quality_{slug}_rankings_download",
        )

    judge_names = sorted(profiles["Judge"].tolist(), key=str.lower)
    pick = st.selectbox("Judge detail", judge_names, key="pcs_quality_detail_judge")
    if pick:
        judge_row = profiles.loc[profiles["Judge"] == pick]
        if not judge_row.empty:
            jr = judge_row.iloc[0]
            st.subheader(f"Summary — {pick}")
            s1, s2, s3 = st.columns(3)
            s1.metric("Ranking score", f"{jr.get('ranking_score', 0):.4f}")
            s2.metric("Bias score", f"{jr.get('bias_score', 0):.4f}")
            s3.metric("Differentiation score", f"{jr.get('diff_score', 0):.4f}")
            if jr.get("bias_tendency"):
                st.caption(
                    f"Bias tendency: **{jr['bias_tendency']}** "
                    f"(mean bias {float(jr.get('mean_bias', 0)):+.3f})"
                )
            if jr.get("spread_tendency"):
                st.caption(
                    f"Spread: **{jr['spread_tendency']}** "
                    f"(variance ratio {float(jr.get('variance_ratio', 0)):.2f})"
                )
            rho = jr.get("Mean Spearman ρ")
            if rho is not None and pd.notna(rho):
                st.caption(f"Mean Spearman ρ (ranking): {float(rho):.3f}")
            st.caption(f"PCS marks in scope: {int(jr.get('PCS marks', 0)):,}")

        if not discipline_summary.empty:
            disc_sub = discipline_summary.loc[
                discipline_summary["identity"] == pick
            ].copy()
            if not disc_sub.empty:
                st.subheader(f"By discipline — {pick}")
                st.caption(
                    "Equal-weight mean of each measure across PCS components in that discipline."
                )
                disc_display = _pcs_quality_format_discipline_summary(disc_sub)
                st.dataframe(
                    disc_display,
                    width="stretch",
                    hide_index=True,
                    column_config=_pcs_quality_discipline_column_config(),
                )

        if not component_detail.empty:
            comp_sub = component_detail.loc[
                component_detail["identity"] == pick
            ].copy()
            if not comp_sub.empty:
                st.subheader(f"By PCS component — {pick}")
                st.caption(
                    "One row per discipline × component the judge marked. Scores are in [0, 1] "
                    "(higher = closer to the panel on that measure). Scroll horizontally for all columns."
                )
                comp_display = _pcs_quality_format_component_detail(comp_sub)
                st.dataframe(
                    comp_display,
                    width="stretch",
                    hide_index=True,
                    column_config=_pcs_quality_component_column_config(),
                )
                from app_query_params import label_to_query_slug

                _pick_slug = label_to_query_slug(pick)
                _streamlit_download(
                    f"Download component detail CSV ({pick})",
                    data=comp_display.to_csv(index=False).encode("utf-8"),
                    file_name=f"pcs_quality_{_pick_slug}_components.csv",
                    mime="text/csv",
                    key=f"pcs_quality_comp_dl_{_pick_slug}",
                )


def _mark_element_ranking_benchmark_customized() -> None:
    st.session_state["element_ranking_benchmark_customized"] = True


def _element_ranking_segment_level_session_value(preset: str | None) -> str:
    return preset or ELEMENT_RANKING_LEVEL_FILTER_ALL


def _sync_element_ranking_benchmark_pool(
    start_season: str,
    end_season: str,
    scope_label: str,
    segment_level_preset: str | None,
) -> None:
    """
    Mirror ranking season years, competition scope, and segment levels into the
    σ̂ benchmark pool until the user edits the benchmark widgets.
    """
    seg_session = _element_ranking_segment_level_session_value(segment_level_preset)
    main_key = (start_season, end_season, scope_label, seg_session)
    prev_main = st.session_state.get("element_ranking_ranking_filters_for_benchmark")
    customized = st.session_state.get("element_ranking_benchmark_customized", False)

    if prev_main != main_key and (prev_main is None or not customized):
        st.session_state["element_ranking_benchmark_start_season"] = start_season
        st.session_state["element_ranking_benchmark_end_season"] = end_season
        st.session_state["element_ranking_benchmark_competition_scope"] = scope_label
        st.session_state["element_ranking_benchmark_segment_levels"] = seg_session

    st.session_state["element_ranking_ranking_filters_for_benchmark"] = main_key


def element_deviation_ranking_page():
    """Element GOE marking scores vs panel-median control (sigma-normalized)."""
    st.header("Element Deviation Ranking Analysis")
    st.caption(
        "Ranks judges by how closely their element GOEs track a **panel control score** "
        "(median GOE per element), after normalizing spread by discipline, element type, "
        "and rounded control GOE. **Lower marking score = closer to the model** "
        "(not necessarily “better” judging in absolute terms)."
    )

    with st.expander("Methodology", expanded=False):
        st.markdown(
            """
**1. Data** — Element GOE marks from competitions on or after **2018-07-01**
(current scale). Judges linked to the same directory official are merged
(same identity groups as Individual Judge / Cross-Judge).

**2. Control score** — For each element, the panel **median** GOE across judges.
**Error** = judge GOE − control score.

**3. Intrinsic spread σ̂** — For each combination of *(discipline, element type,
rounded control GOE)* with at least ``min_bin_count`` marks, estimate σ̂ as the
sample standard deviation of errors in that bin. If a mark’s bin is missing, try
neighboring integer GOE levels (±1, ±2), else a global fallback σ (with a floor).

**4. Normalized mark** — ``m = error / σ̂`` (per mark).

**5. Marking score** — Per judge identity:
``M = √(mean(m²))``. Ranked ascending (rank 1 = lowest M).

PCS scores are not part of this model.
            """
        )

    analytics = get_analytics_safe()

    st.subheader("Filters")
    scope_label = st.selectbox(
        "Competition scope",
        list(_COMPETITION_SCOPE_LABELS),
        index=0,
        key="element_ranking_competition_scope",
        help="Filters competitions via linked officials competition type.",
    )
    scope_key = _competition_scope_key(scope_label)

    years = filter_element_ranking_season_years(analytics.get_years())
    if not years:
        st.error("No competition seasons found in the database.")
        return
    start_season_year = None
    end_season_year = None
    event_start_iso = None
    event_end_iso = None

    col_y1, col_y2 = st.columns(2)
    with col_y1:
        start_season = st.selectbox(
            "Season year from",
            ["Any"] + years,
            key="element_ranking_start_season",
        )
        start_season_year = None if start_season == "Any" else start_season
    with col_y2:
        end_season = st.selectbox(
            "Season year to",
            ["Any"] + years,
            key="element_ranking_end_season",
        )
        end_season_year = None if end_season == "Any" else end_season

    st.caption(
        f"Element marks are limited to competitions on or after "
        f"**{MIN_ELEMENT_MARKING_EVENT_DATE.isoformat()}** (current GOE scale)."
    )
    use_event_dates = st.checkbox(
        "Narrow by competition event dates",
        key="element_ranking_use_event_dates",
        help=(
            "Further restrict rankings by event date (not applied to the σ̂ benchmark pool). "
            "Events with neither date are excluded."
        ),
    )
    if use_event_dates:
        date_min, date_max = analytics.get_competition_event_date_bounds(
            competition_scope=scope_key,
        )
        date_min = max(date_min, MIN_ELEMENT_MARKING_EVENT_DATE)
        date_max = max(date_max, date_min)
        default_start = max(date_min, MIN_ELEMENT_MARKING_EVENT_DATE)
        dc1, dc2 = st.columns(2)
        with dc1:
            event_start = st.date_input(
                "Event on or after",
                value=default_start,
                min_value=MIN_ELEMENT_MARKING_EVENT_DATE,
                max_value=date_max,
                key="element_ranking_start_date",
            )
        with dc2:
            event_end = st.date_input(
                "Event on or before",
                value=date_max,
                min_value=MIN_ELEMENT_MARKING_EVENT_DATE,
                max_value=date_max,
                key="element_ranking_end_date",
            )
        if event_start > event_end:
            st.warning("Start date is after end date; results may be empty.")
        event_start_iso = event_start.isoformat()
        event_end_iso = event_end.isoformat()

    col_d, col_levels, col_cache = st.columns([2, 1, 1])
    with col_d:
        discipline_types = element_ranking_discipline_types(analytics, scope_key)
        discipline_names = [name for _id, name in discipline_types]
        selected_disciplines = st.multiselect(
            "Discipline types",
            discipline_names,
            key="element_ranking_disciplines",
        )
        discipline_ids = [
            dt_id for dt_id, name in discipline_types if name in selected_disciplines
        ] if selected_disciplines else None
    with col_levels:
        segment_level_preset = st.selectbox(
            "Segment levels",
            list(ELEMENT_RANKING_LEVEL_FILTER_LABELS),
            format_func=lambda key: ELEMENT_RANKING_LEVEL_FILTER_LABELS[key],
            key="element_ranking_segment_levels",
            help=(
                "Restrict element marks by ``segment.level``. Novice = Novice and "
                "Advanced Novice only; Junior = Junior only; Senior = Senior only "
                "(no Excel or International). Other levels are excluded unless you "
                "choose All segment levels."
            ),
        )
        if segment_level_preset == ELEMENT_RANKING_LEVEL_FILTER_ALL:
            segment_level_preset = None
    with col_cache:
        use_precomputed_cache = st.checkbox(
            "Use precomputed cache",
            value=True,
            key="element_ranking_use_cache",
            help=(
                "Load cached season×discipline shards and assemble (missing shards are computed)."
            ),
        )

    _sync_element_ranking_benchmark_pool(
        start_season, end_season, scope_label, segment_level_preset
    )

    benchmark_start_season_year = None
    benchmark_end_season_year = None
    benchmark_scope_key = COMPETITION_SCOPE_ALL
    benchmark_segment_level_preset = None
    with st.expander("σ̂ benchmark pool", expanded=False):
        st.caption(
            "Seasons, competition scope, and segment levels used only to fit "
            "intrinsic spread (σ̂). Defaults to the same values as the ranking "
            "filters above; change these to use a custom σ̂ pool. Event dates "
            "apply to rankings only, not to this pool."
        )
        bc1, bc2 = st.columns(2)
        with bc1:
            bench_start_pick = st.selectbox(
                "Season year from",
                ["Any"] + years,
                key="element_ranking_benchmark_start_season",
                on_change=_mark_element_ranking_benchmark_customized,
            )
            benchmark_start_season_year = (
                None if bench_start_pick == "Any" else bench_start_pick
            )
        with bc2:
            bench_end_pick = st.selectbox(
                "Season year to",
                ["Any"] + years,
                key="element_ranking_benchmark_end_season",
                on_change=_mark_element_ranking_benchmark_customized,
            )
            benchmark_end_season_year = (
                None if bench_end_pick == "Any" else bench_end_pick
            )
        bench_scope_label = st.selectbox(
            "Competition scope",
            list(_COMPETITION_SCOPE_LABELS),
            key="element_ranking_benchmark_competition_scope",
            help="Officials competition-type filter for σ̂ fitting only.",
            on_change=_mark_element_ranking_benchmark_customized,
        )
        benchmark_scope_key = _competition_scope_key(bench_scope_label)
        bench_segment_level_pick = st.selectbox(
            "Segment levels",
            list(ELEMENT_RANKING_LEVEL_FILTER_LABELS),
            format_func=lambda key: ELEMENT_RANKING_LEVEL_FILTER_LABELS[key],
            key="element_ranking_benchmark_segment_levels",
            help=(
                "Restrict element marks used for σ̂ fitting by ``segment.level``. "
                "Same presets as the ranking filter above."
            ),
            on_change=_mark_element_ranking_benchmark_customized,
        )
        if bench_segment_level_pick == ELEMENT_RANKING_LEVEL_FILTER_ALL:
            benchmark_segment_level_preset = None
        else:
            benchmark_segment_level_preset = bench_segment_level_pick

    st.subheader("Model parameters")
    p1, p2, p3 = st.columns(3)
    with p1:
        if "element_ranking_min_marks" not in st.session_state:
            st.session_state["element_ranking_min_marks"] = 0
        min_marks = st.number_input(
            "Minimum element marks per judge",
            min_value=0,
            step=50,
            key="element_ranking_min_marks",
            help=(
                "Exclude identities with fewer marks after filters. "
                "Changing only this reuses the last computation (no full re-run)."
            ),
        )
    with p2:
        floor_sigma = st.number_input(
            "Floor σ̂",
            min_value=0.01,
            max_value=1.0,
            value=float(_ELEM_RANK_FLOOR_SIGMA),
            step=0.01,
            format="%.2f",
            key="element_ranking_floor_sigma",
            help=(
                "Minimum intrinsic spread σ̂ applied to every element mark. "
                "Fitted bin standard deviations and the global fallback σ are "
                "raised to this floor so m = (GOE − control) / σ̂ does not "
                "inflate when a bin’s spread is very small."
            ),
        )
    with p3:
        min_bin_count = st.number_input(
            "Min marks per σ̂ bin",
            min_value=5,
            max_value=200,
            value=int(_ELEM_RANK_MIN_BIN_COUNT),
            step=5,
            key="element_ranking_min_bin_count",
            help="(discipline, element type, rounded control GOE) buckets with fewer marks are skipped.",
        )

    us_officials_only = st.checkbox(
        "US directory officials only",
        key="element_ranking_us_officials_only",
        help=(
            "Show rankings only for judges linked to a USFS official "
            "(Admin → Judge ↔ directory matcher). σ̂ and panel medians still use all judges."
        ),
    )

    disc_tuple = tuple(discipline_ids) if discipline_ids else None
    run_params = (
        start_season_year,
        end_season_year,
        disc_tuple,
        scope_key,
        event_start_iso,
        event_end_iso,
        int(min_marks),
        float(floor_sigma),
        int(min_bin_count),
        benchmark_start_season_year,
        benchmark_end_season_year,
        benchmark_scope_key,
        segment_level_preset,
        benchmark_segment_level_preset,
    )
    _finalize_element_ranking_job()
    job_status = st.session_state.get("element_ranking_status", "idle")
    job_running = job_status == "running"

    btn_col_run, btn_col_cancel = st.columns(2)
    with btn_col_run:
        run_clicked = st.button(
            "Run analysis",
            type="primary",
            key="element_ranking_run_btn",
            disabled=job_running,
            help="Loads element marks and fits σ̂ bins. Can take a minute on wide filters.",
        )
    with btn_col_cancel:
        cancel_clicked = st.button(
            "Cancel analysis",
            key="element_ranking_cancel_btn",
            disabled=not job_running,
            help="Stops the background job (may take a few seconds).",
        )

    if cancel_clicked and job_running:
        _cancel_element_ranking_job()
        st.rerun()

    scope_err = validate_element_ranking_scope(
        start_season_year,
        end_season_year,
        available_years=years,
    )
    if scope_err:
        st.error(scope_err)

    if run_clicked and not job_running and not scope_err:
        _stop_element_ranking_process()
        st.session_state.element_ranking_run_params = run_params
        st.session_state.pop("element_ranking_result", None)
        st.session_state.pop("element_ranking_error_msg", None)
        st.session_state.pop("element_ranking_cache_saved", None)
        st.session_state.pop("element_ranking_cache_error", None)
        st.session_state.pop("element_ranking_judge_summary_all", None)
        st.session_state.pop("element_ranking_judge_summary_scope_key", None)
        for _k in list(st.session_state.keys()):
            if isinstance(_k, tuple) and _k[:1] in (
                "element_ranking_judge_detail",
                "element_ranking_control",
            ):
                del st.session_state[_k]
        if use_precomputed_cache:
            cached = run_with_isolated_analytics(
                lambda cache_analytics, rp: load_cached_rankings(
                    cache_analytics.session, cache_analytics, rp
                ),
                run_params,
            )
            if cached is not None:
                cached = run_with_isolated_analytics(
                    _enrich_ranking_result_for_drilldown,
                    run_params,
                    cached,
                )
                st.session_state.element_ranking_result = cached
                _persist_element_ranking_judge_summary_pool(
                    cached, run_params=run_params
                )
                st.session_state.element_ranking_status = "done"
                st.success("Loaded precomputed cache for this filter set.")
                st.rerun()
        if _element_ranking_use_subprocess():
            fd, pickle_path = tempfile.mkstemp(prefix="elem_rank_", suffix=".pkl")
            os.close(fd)
            st.session_state.element_ranking_pickle_path = pickle_path
            proc = start_ranking_subprocess(
                run_params, pickle_path, database_url=get_database_url()
            )
            st.session_state.element_ranking_proc = proc
            st.session_state.element_ranking_status = "running"
            st.rerun()
        else:
            fd, pickle_path = tempfile.mkstemp(prefix="elem_rank_", suffix=".pkl")
            os.close(fd)
            st.session_state.element_ranking_pickle_path = pickle_path
            with st.spinner("Computing rankings…"):
                computed = rehydrate_packaged_ranking_result(
                    _run_element_ranking_compute(run_params)
                )
            enriched = run_with_isolated_analytics(
                _enrich_ranking_result_for_drilldown,
                run_params,
                computed,
            )
            st.session_state.element_ranking_result = enriched
            _persist_element_ranking_judge_summary_pool(
                enriched, run_params=run_params
            )
            st.session_state.element_ranking_status = "done"
            ok, err = run_with_isolated_analytics(
                lambda cache_analytics, rp, ranking_result: try_save_element_ranking_cache(
                    cache_analytics.session,
                    cache_analytics,
                    rp,
                    ranking_result,
                ),
                run_params,
                st.session_state.element_ranking_result,
            )
            if ok:
                st.session_state.element_ranking_cache_saved = True
                st.session_state.pop("element_ranking_cache_error", None)
            elif err:
                st.session_state.element_ranking_cache_error = err
            st.rerun()

    if job_running:
        st.warning(
            "Analysis is running in a separate process. You can switch pages; "
            "return here and click **Cancel analysis** to stop it."
        )
        time.sleep(0.8)
        st.rerun()

    if job_status == "cancelled":
        st.info("Analysis cancelled.")
        return

    if job_status == "error":
        st.error(
            st.session_state.get(
                "element_ranking_error_msg", "Analysis failed."
            )
        )
        return

    stored_params = st.session_state.get("element_ranking_run_params")
    base_result = st.session_state.get("element_ranking_result")
    if base_result is None:
        st.info(
            "Set filters and click **Run analysis**. "
            "Rankings are not computed automatically so navigation stays responsive."
        )
        return
    _persist_element_ranking_judge_summary_pool(
        base_result, run_params=stored_params
    )

    compute_match = (
        stored_params is not None
        and run_params_same_sigma_and_ranking_scope(stored_params, run_params)
    )
    if not compute_match:
        st.warning(
            "Filters or model parameters changed since the last run. "
            "Click **Run analysis** to refresh results."
        )
        return

    if stored_params is not None and load_control_by_element(base_result).empty:
        enriched = run_with_isolated_analytics(
            _enrich_ranking_result_for_drilldown,
            stored_params,
            base_result,
        )
        if enriched is not base_result:
            st.session_state.element_ranking_result = enriched
            base_result = enriched

    result = apply_min_marks_to_ranking_result(
        base_result,
        int(min_marks),
        judge_summary_all=_element_ranking_scoped_judge_summary_pool(
            base_result, run_params
        ),
    )
    min_marks_changed = (
        stored_params is not None
        and run_params_same_sigma_and_ranking_scope(stored_params, run_params)
        and int(stored_params[6] or 0) != int(min_marks)
    )
    if min_marks_changed:
        st.caption(
            "Minimum marks filter updated — reusing the last σ̂ fit and panel medians."
        )
    if int(min_marks) > 0 and not result.get("_min_marks_filter_applied", False):
        st.warning(
            "Could not apply the minimum element marks filter to the saved results. "
            "Click **Run analysis** to refresh rankings."
        )

    if us_officials_only:
        from judge_official_display_filter import (
            apply_us_officials_display_filter_to_ranking_result,
        )

        n_before_us = len(result.get("marking", pd.DataFrame()))
        result = apply_us_officials_display_filter_to_ranking_result(
            result, us_linked_identity_labels_for_ui()
        )
        n_after_us = len(result.get("marking", pd.DataFrame()))
        if n_before_us and n_after_us < n_before_us:
            st.caption(
                f"US officials display filter: **{n_after_us}** of {n_before_us} ranked "
                "identities are linked to the USFS directory."
            )

    if result["error"]:
        st.warning(result["error"])
        return

    cache_err = st.session_state.get("element_ranking_cache_error")
    if cache_err:
        st.warning(
            "Rankings completed but could not be saved for reuse: "
            f"{cache_err}"
        )
    elif st.session_state.get("element_ranking_cache_saved"):
        st.caption("Results saved for this filter set (will load faster next time).")

    st.subheader("Run summary")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "Element marks (filtered)",
        f"{result['n_raw_marks']:,}",
        help="All element GOE marks in the filtered dataset (before per-judge minimum).",
    )
    m2.metric(
        "σ̂ bins fitted",
        f"{result['n_sigma_buckets']:,}",
        help=(
            "Count of (discipline, element type, rounded control GOE) bins with at least "
            "the minimum marks used to estimate σ̂."
        ),
    )
    m3.metric(
        "Judges ranked",
        len(result["marking"]),
        help="Judge identities meeting the minimum element marks threshold.",
    )
    m4.metric(
        "Floor σ̂",
        f"{floor_sigma:.2f}",
        help="Minimum σ̂ used when normalizing marks (see Model parameters).",
    )
    if uses_separate_benchmark_pool(run_params):
        bench_start, bench_end = benchmark_season_bounds(run_params)
        bench_scope = benchmark_competition_scope(run_params)
        bench_levels = benchmark_segment_level_preset(run_params)
        rank_start, rank_end = start_season_year, end_season_year
        scope_labels = {v: k for k, v in _COMPETITION_SCOPE_LABEL_TO_KEY.items()}
        bench_scope_label = scope_labels.get(bench_scope, bench_scope)
        rank_scope_label = scope_labels.get(scope_key, scope_key)
        def _level_label(preset: str | None) -> str:
            return ELEMENT_RANKING_LEVEL_FILTER_LABELS.get(
                preset or ELEMENT_RANKING_LEVEL_FILTER_ALL, "All"
            )

        st.caption(
            f"σ̂ benchmark pool: seasons **{bench_start or 'Any'}**–**{bench_end or 'Any'}**, "
            f"scope **{bench_scope_label}**, levels **{_level_label(bench_levels)}**. "
            f"Rankings: seasons **{rank_start or 'Any'}**–**{rank_end or 'Any'}**, "
            f"scope **{rank_scope_label}**, levels **{_level_label(segment_level_preset)}**"
            + (
                f", events **{event_start_iso}**–**{event_end_iso}**"
                if event_start_iso and event_end_iso
                else ""
            )
            + " (cached shards and σ̂ reused when available)."
        )

    if result["marking"].empty:
        st.info("No judges remain after filters and minimum marks threshold.")
        return

    marking = result["marking"]

    st.subheader("Distribution of marking scores")
    if not result["summary"].empty:
        st.dataframe(
            result["summary"],
            width="stretch",
            hide_index=True,
            column_config={
                "Statistic": st.column_config.TextColumn(
                    "Statistic",
                    help="Summary stats across judges’ marking scores (M = √(mean(m²))).",
                ),
                "Value": st.column_config.NumberColumn(
                    "Value",
                    format="%.6f",
                    help="Value of the statistic for the judge population in this run.",
                ),
            },
        )
    fig = px.histogram(
        marking,
        x="Marking score",
        nbins=min(40, max(10, len(marking) // 5)),
        title="Marking scores across judges (lower = closer to control-score model)",
    )
    fig.update_layout(bargap=0.05)
    st.plotly_chart(fig, width="stretch")

    st.subheader("Rankings")
    st.caption(
        "**Marking score** = √(mean(m²)) over element marks, with m = (GOE − panel median) / σ̂. "
        "**Mean GOE bias** is the signed average raw GOE − panel median (+ above, − below). "
        "**Mean |m|** is the average absolute normalized mark."
    )
    st.dataframe(
        marking,
        width="stretch",
        hide_index=True,
        column_config=_element_ranking_rankings_column_config(),
    )
    _streamlit_download(
        "Download rankings CSV",
        data=marking.to_csv(index=False).encode("utf-8"),
        file_name="element_deviation_rankings.csv",
        mime="text/csv",
        key="element_ranking_rankings_download",
    )

    st.subheader("σ̂ parameters (by discipline, element type, control GOE)")
    st.caption(
        "For each bin with at least **min marks per σ̂ bin** panel marks in the filtered data, "
        "**Error stdev** is the sample SD of (GOE − median). **σ̂ used in model** is that value "
        "(floored at the configured minimum) when the bin is included; otherwise a neighbor or "
        "fallback σ is used per mark."
    )
    sigma_bins = result.get("sigma_bins", pd.DataFrame())
    if sigma_bins.empty:
        st.info("No σ̂ bins to display for the current filters.")
    else:
        st.dataframe(
            sigma_bins,
            width="stretch",
            hide_index=True,
            column_config=_element_ranking_sigma_bins_column_config(),
        )
        _streamlit_download(
            "Download σ̂ parameters CSV",
            data=sigma_bins.to_csv(index=False).encode("utf-8"),
            file_name="element_sigma_parameters.csv",
            mime="text/csv",
            key="element_ranking_sigma_download",
        )

    judge_names = marking["Judge"].tolist() if "Judge" in marking.columns else []
    if judge_names:
        st.subheader("How a judge’s score breaks down")
        pick_judge = st.selectbox(
            "Select judge",
            judge_names,
            key="element_ranking_detail_judge",
        )
        judge_row = marking.loc[marking["Judge"] == pick_judge]
        if not judge_row.empty and "Mean GOE bias" in judge_row.columns:
            bias = float(judge_row["Mean GOE bias"].iloc[0])
            bias_label = (
                "above panel median"
                if bias > 0.005
                else "below panel median"
                if bias < -0.005
                else "near panel median"
            )
            st.metric(
                "Overall mean GOE bias",
                f"{bias:+.3f}",
                help=(
                    "Signed average of (judge GOE − panel median GOE) over all element "
                    "marks for this judge. Positive = systematically higher GOEs than "
                    "the panel median; negative = systematically lower."
                ),
            )
            st.caption(
                f"On average this judge marks **{bias_label}** "
                f"({bias:+.3f} GOE vs panel median per element mark)."
            )
        _detail_col_config = _element_ranking_judge_detail_column_config()
        jd, je, jg = _element_ranking_load_judge_breakdown(
            result, run_params, pick_judge
        )
        if jd.empty and je.empty and jg.empty:
            st.info(
                _element_ranking_breakdown_failure_hint(
                    result, run_params, pick_judge
                )
            )
        else:
            from element_ranking_takeaways import (
                build_element_ranking_judge_takeaways,
                detail_filter_options,
                filter_judge_detail_tables,
            )

            disc_options, et_options = detail_filter_options(jd, je, jg)
            report_disciplines = _element_ranking_report_discipline_names(
                analytics, run_params
            )
            discipline_choices = sorted(set(report_disciplines) | set(disc_options))
            default_disciplines = [
                d for d in report_disciplines if d in discipline_choices
            ]
            judge_filter_key = pick_judge.replace(" ", "_")[:48]
            with st.expander("Drill-down filters", expanded=False):
                filt_c1, filt_c2, filt_c3 = st.columns(3)
                with filt_c1:
                    detail_min_marks = st.number_input(
                        "Minimum marks per row",
                        min_value=0,
                        value=0,
                        step=5,
                        key=f"element_ranking_detail_min_marks_{judge_filter_key}",
                        help="Hide breakdown rows with fewer element marks than this.",
                    )
                with filt_c2:
                    detail_disciplines = st.multiselect(
                        "Discipline",
                        discipline_choices,
                        default=default_disciplines,
                        key=f"element_ranking_detail_disciplines_{judge_filter_key}",
                        help=(
                            "Defaults to all disciplines in the report scope. "
                            "Clear all to include every discipline present in this breakdown."
                        ),
                    )
                with filt_c3:
                    detail_element_types = st.multiselect(
                        "Element type",
                        et_options,
                        default=et_options,
                        key=f"element_ranking_detail_element_types_{judge_filter_key}",
                        help=(
                            "Defaults to all element types for this judge. "
                            "Clear all to include every element type in the breakdown."
                        ),
                    )

            jd_f, je_f, jg_f = filter_judge_detail_tables(
                jd,
                je,
                jg,
                min_marks=int(detail_min_marks) if int(detail_min_marks) > 0 else None,
                disciplines=detail_disciplines or None,
                element_types=detail_element_types or None,
            )
            if jd_f.empty and je_f.empty and jg_f.empty:
                st.warning(
                    "No breakdown rows match the drill-down filters. "
                    "Lower the minimum marks or clear discipline / element-type filters."
                )
            else:
                judge_row_series = (
                    judge_row.iloc[0]
                    if not judge_row.empty
                    else None
                )
                takeaway_min = max(30, int(detail_min_marks or 0))
                takeaways = build_element_ranking_judge_takeaways(
                    pick_judge,
                    judge_row_series,
                    jd_f,
                    je_f,
                    jg_f,
                    min_marks=takeaway_min,
                    min_bin_marks=max(15, min(30, takeaway_min)),
                )
                if takeaways:
                    st.markdown("**Takeaways**")
                    for line in takeaways:
                        st.markdown(f"- {line}")

                if not jd_f.empty:
                    jd_one = jd_f.sort_values("Partial marking score")
                    st.markdown(
                        "**By discipline** "
                        "(partial score = √(mean(m²)) within that discipline; σ̂-scaled)"
                    )
                    st.dataframe(
                        jd_one,
                        width="stretch",
                        hide_index=True,
                        column_config=_detail_col_config,
                    )
                if not je_f.empty:
                    je_one = je_f.sort_values("Partial marking score", ascending=False)
                    st.markdown(
                        "**By discipline and element type** "
                        "(largest normalized partial scores first)"
                    )
                    st.dataframe(
                        je_one,
                        width="stretch",
                        hide_index=True,
                        column_config=_detail_col_config,
                    )
                if not jg_f.empty:
                    jg_one = jg_f.sort_values("Partial marking score", ascending=False)
                    st.markdown(
                        "**By control GOE range** "
                        "(discipline × element type × rounded panel median GOE)"
                    )
                    st.caption(
                        "Each row is one σ̂ bin. Partial score is √(mean(m²)) using "
                        "normalized marks m = (GOE − panel median) / σ̂."
                    )
                    chart_series_options = _control_goe_chart_series_options(
                        je_f if not je_f.empty else jg_one
                    )
                    default_chart_series = chart_series_options[:3]
                    chart_series = st.multiselect(
                        "Chart series (discipline × element type)",
                        chart_series_options,
                        default=default_chart_series,
                        key=f"element_ranking_chart_series_{judge_filter_key}",
                        help=(
                            "Each line is partial marking score vs rounded panel "
                            "median GOE. Points match the drill-down filters above "
                            "(minimum marks, discipline, element type)."
                        ),
                    )
                    goe_fig = _element_ranking_control_goe_chart(
                        jg_one,
                        je_f if not je_f.empty else jg_one,
                        min_bin_marks=0,
                        series_keys=chart_series or None,
                    )
                    if goe_fig is not None:
                        st.plotly_chart(goe_fig, width="stretch")
                    elif chart_series:
                        st.caption(
                            "No chart: selected series have no rows under the "
                            "current drill-down filters."
                        )
                    st.dataframe(
                        jg_one,
                        width="stretch",
                        hide_index=True,
                        column_config=_detail_col_config,
                    )


_PCS_DEVIATION_FLOOR_SIGMA = 0.05
_PCS_DEVIATION_MIN_BIN_COUNT = 30


def _pcs_deviation_rankings_column_config() -> dict:
    return {
        "rank": st.column_config.NumberColumn("Rank", format="%d"),
        "Marking score": st.column_config.NumberColumn(
            "Marking score",
            format="%.4f",
            help="√(mean(m²)) over PCS marks; lower = closer to the panel-median / σ̂ model.",
        ),
        "PCS marks": st.column_config.NumberColumn(
            "PCS marks",
            format="%d",
            help="Number of PCS marks for this judge identity after filters.",
        ),
        "Mean PCS bias": st.column_config.NumberColumn(
            "Mean PCS bias",
            format="%+.3f",
            help="Signed average PCS − panel median (+ above, − below).",
        ),
        "Mean |error|": st.column_config.NumberColumn(
            "Mean |error|",
            format="%.3f",
            help="Average |PCS − panel median| in raw points.",
        ),
        "Mean σ̂": st.column_config.NumberColumn(
            "Mean σ̂",
            format="%.3f",
            help="Average intrinsic spread σ̂ applied to this judge’s marks.",
        ),
        "Mean |m|": st.column_config.NumberColumn(
            "Mean |m|",
            format="%.3f",
            help="Average |m| where m = (PCS − panel median) / σ̂.",
        ),
    }


def _pcs_deviation_sigma_bins_column_config() -> dict:
    return {
        "Marks in bin": st.column_config.NumberColumn("Marks in bin", format="%d"),
        "Error stdev (all marks)": st.column_config.NumberColumn(
            "Error stdev (all marks)", format="%.4f"
        ),
        "σ̂ used in model": st.column_config.NumberColumn(
            "σ̂ used in model", format="%.4f"
        ),
    }


def _pcs_deviation_judge_detail_column_config() -> dict:
    return {
        "PCS marks": st.column_config.NumberColumn("PCS marks", format="%d"),
        "Partial marking score": st.column_config.NumberColumn(
            "Partial marking score",
            format="%.4f",
            help="√(mean(m²)) within the row’s slice.",
        ),
        "Mean PCS bias": st.column_config.NumberColumn(
            "Mean PCS bias", format="%+.3f"
        ),
        "Mean |error|": st.column_config.NumberColumn("Mean |error|", format="%.3f"),
        "Mean σ̂": st.column_config.NumberColumn("Mean σ̂", format="%.3f"),
    }


def _clear_pcs_deviation_judge_detail_cache() -> None:
    for key in list(st.session_state.keys()):
        if isinstance(key, tuple) and key and key[0] == "pcs_deviation_judge_detail":
            del st.session_state[key]


def _pcs_deviation_sigma_params(
    result: dict,
    run_params: tuple | None,
) -> dict:
    """σ̂ bin lookup for drill-down (inline result or DB cache)."""
    params = result.get("params")
    if isinstance(params, dict) and params:
        return params
    if run_params is None:
        return {}
    from pcs_deviation_cache import load_cached_sigma_params_for_run

    cached = run_with_isolated_analytics(
        lambda analytics, rp: load_cached_sigma_params_for_run(
            analytics.session, analytics, rp
        ),
        run_params,
    )
    return cached if isinstance(cached, dict) else {}


def _pcs_deviation_breakdown_failure_hint(
    result: dict,
    run_params: tuple,
    pick_judge: str,
) -> str:
    if not _pcs_deviation_sigma_params(result, run_params):
        return (
            "σ̂ parameters are missing from the saved run. "
            "Click **Run analysis** to refresh results."
        )
    return (
        f"No PCS marks found for **{pick_judge}** under the current filters, "
        "or that identity could not be resolved. "
        "Click **Run analysis** after changing season, scope, or model parameters."
    )


def _pcs_deviation_judge_detail_from_result(
    result: dict,
    pick_judge: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    """Slice precomputed drill-down tables when the run already materialized them."""

    def _slice_judge(df: object) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame) or df.empty or "Judge" not in df.columns:
            return pd.DataFrame()
        return df.loc[df["Judge"] == pick_judge].copy()

    jd_all = result.get("judge_discipline_detail_all")
    if isinstance(jd_all, pd.DataFrame) and not jd_all.empty:
        jd = _slice_judge(jd_all)
        jc = _slice_judge(result.get("judge_component_detail_all"))
        jb = _slice_judge(result.get("judge_control_bin_detail_all"))
        if not jd.empty or not jc.empty or not jb.empty:
            return jd, jc, jb

    jd = _slice_judge(result.get("judge_discipline_detail"))
    jc = _slice_judge(result.get("judge_component_detail"))
    jb = _slice_judge(result.get("judge_control_bin_detail"))
    if not jd.empty or not jc.empty or not jb.empty:
        return jd, jc, jb
    return None


def _pcs_deviation_load_judge_breakdown(
    result: dict,
    run_params: tuple,
    pick_judge: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """On-demand per-judge tables (discipline, component, panel-median bins)."""
    from pcs_deviation_analysis import (
        compute_judge_detail_for_identity_pcs,
        ranking_scope_kwargs_from_run_params,
        run_params_ranking_compute_key,
        unpack_pcs_deviation_run_params,
    )

    embedded = _pcs_deviation_judge_detail_from_result(result, pick_judge)
    if embedded is not None:
        detail_key = (
            "pcs_deviation_judge_detail",
            run_params_ranking_compute_key(run_params),
            pick_judge,
        )
        st.session_state[detail_key] = embedded
        return embedded

    empty = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    params = _pcs_deviation_sigma_params(result, run_params)
    if not params:
        return empty
    if not result.get("params"):
        patched = dict(result)
        patched["params"] = params
        st.session_state.pcs_deviation_result = patched
        result = patched

    detail_key = (
        "pcs_deviation_judge_detail",
        run_params_ranking_compute_key(run_params),
        pick_judge,
    )
    cached = st.session_state.get(detail_key)
    if isinstance(cached, tuple) and len(cached) >= 3:
        jd_c = cached[0] if isinstance(cached[0], pd.DataFrame) else pd.DataFrame()
        jc_c = cached[1] if isinstance(cached[1], pd.DataFrame) else pd.DataFrame()
        jb_c = cached[2] if isinstance(cached[2], pd.DataFrame) else pd.DataFrame()
        if not jb_c.empty or not jd_c.empty or not jc_c.empty:
            return jd_c, jc_c, jb_c

    rp = unpack_pcs_deviation_run_params(run_params)
    scope = ranking_scope_kwargs_from_run_params(run_params)

    def _compute_judge_breakdown(analytics, judge_name, sigma_params, rank_scope):
        return compute_judge_detail_for_identity_pcs(
            analytics,
            judge_name,
            sigma_params,
            floor_sigma=float(rp[7]),
            **rank_scope,
        )

    with st.spinner(f"Loading breakdown for {pick_judge}…"):
        detail = run_with_isolated_analytics(
            _compute_judge_breakdown,
            pick_judge,
            params,
            scope,
        )
    if detail[0].empty and detail[1].empty and detail[2].empty:
        return detail
    st.session_state[detail_key] = detail
    return detail


def _mark_pcs_deviation_benchmark_customized() -> None:
    st.session_state["pcs_deviation_benchmark_customized"] = True


def _pcs_deviation_segment_level_session_value(preset: str | None) -> str:
    from pcs_deviation_analysis import ELEMENT_RANKING_LEVEL_FILTER_ALL

    return preset or ELEMENT_RANKING_LEVEL_FILTER_ALL


def _sync_pcs_deviation_benchmark_pool(
    start_season: str,
    end_season: str,
    scope_label: str,
    segment_level_preset: str | None,
) -> None:
    seg_session = _pcs_deviation_segment_level_session_value(segment_level_preset)
    main_key = (start_season, end_season, scope_label, seg_session)
    prev_main = st.session_state.get("pcs_deviation_ranking_filters_for_benchmark")
    customized = st.session_state.get("pcs_deviation_benchmark_customized", False)

    if prev_main != main_key and (prev_main is None or not customized):
        st.session_state["pcs_deviation_benchmark_start_season"] = start_season
        st.session_state["pcs_deviation_benchmark_end_season"] = end_season
        st.session_state["pcs_deviation_benchmark_competition_scope"] = scope_label
        st.session_state["pcs_deviation_benchmark_segment_levels"] = seg_session

    st.session_state["pcs_deviation_ranking_filters_for_benchmark"] = main_key


def pcs_deviation_analysis_page():
    """PCS marking scores vs panel-median control (sigma-normalized)."""
    from pcs_deviation_analysis import (
        ELEMENT_RANKING_LEVEL_FILTER_ALL,
        ELEMENT_RANKING_LEVEL_FILTER_LABELS,
        MIN_PCS_DEVIATION_EVENT_DATE,
        PCS_DEVIATION_COMPETITION_SCOPE_LABELS,
        apply_min_marks_to_pcs_deviation_result,
        compute_pcs_deviation_rankings_from_run_params,
        filter_pcs_deviation_season_years,
        pcs_deviation_competition_scope_key,
        pcs_deviation_discipline_types,
        run_params_same_sigma_and_ranking_scope,
        unpack_pcs_deviation_run_params,
        uses_separate_benchmark_pool,
        validate_pcs_deviation_scope,
    )

    st.header("PCS Deviation Analysis")
    st.caption(
        "Ranks judges by how closely their PCS marks track a **panel control score** "
        "(median PCS per skater × component), after normalizing spread by discipline, "
        "PCS component, and panel-median score range (0.25–1, 1.25–2, etc.). "
        "**Lower marking score = closer to the model**. "
        f"Only competitions on or after **{MIN_PCS_DEVIATION_EVENT_DATE.isoformat()}**; "
        "disciplines limited to Singles, Pairs, Ice Dance, and Synchronized. "
        "Warm shards with ``scripts/precompute_pcs_deviation_cache.py`` "
        "(add ``--sigma-benchmark --summaries`` for cache-only loads without loading all marks)."
    )

    with st.expander("Methodology", expanded=False):
        st.markdown(
            """
**1. Data** — PCS marks from competitions on or after **2022-07-01**.
Judges linked to the same directory official are merged (same identity groups as
other judge reports).

**2. Control score** — For each skater × PCS component, the panel **median** PCS.
**Error** = judge PCS − control score.

**3. Intrinsic spread σ̂** — For each *(discipline, PCS component, panel-median
range)* with at least ``min_bin_count`` marks, estimate σ̂ as the sample standard
deviation of errors in that bin. Ranges are width 1 (0.25–1, 1.25–2, 2.25–3, …).
If a mark’s bin is missing, try neighboring ranges (±1, ±2), else a global fallback σ.

**4. Normalized mark** — ``m = error / σ̂`` (per mark).

**5. Marking score** — Per judge identity: ``M = √(mean(m²))``. Ranked ascending.
            """
        )

    analytics = get_analytics_safe()

    st.subheader("Filters")
    scope_label = st.selectbox(
        "Competition scope",
        list(PCS_DEVIATION_COMPETITION_SCOPE_LABELS),
        index=0,
        key="pcs_deviation_competition_scope",
        help="Filters competitions via linked officials competition type (NQS-only excluded).",
    )
    scope_key = pcs_deviation_competition_scope_key(scope_label)

    years = filter_pcs_deviation_season_years(analytics.get_years())
    if not years:
        st.error("No competition seasons found in the database.")
        return
    start_season_year = None
    end_season_year = None
    event_start_iso = None
    event_end_iso = None

    col_y1, col_y2 = st.columns(2)
    with col_y1:
        start_season = st.selectbox(
            "Season year from",
            ["Any"] + years,
            key="pcs_deviation_start_season",
        )
        start_season_year = None if start_season == "Any" else start_season
    with col_y2:
        end_season = st.selectbox(
            "Season year to",
            ["Any"] + years,
            key="pcs_deviation_end_season",
        )
        end_season_year = None if end_season == "Any" else end_season

    st.caption(
        f"PCS marks are limited to competitions on or after "
        f"**{MIN_PCS_DEVIATION_EVENT_DATE.isoformat()}**."
    )
    use_event_dates = st.checkbox(
        "Narrow by competition event dates",
        key="pcs_deviation_use_event_dates",
        help=(
            "Further restrict rankings by event date (not applied to the σ̂ benchmark pool). "
            "Events with neither date are excluded."
        ),
    )
    if use_event_dates:
        date_min, date_max = analytics.get_competition_event_date_bounds(
            competition_scope=scope_key,
        )
        date_min = max(date_min, MIN_PCS_DEVIATION_EVENT_DATE)
        date_max = max(date_max, date_min)
        default_start = max(date_min, MIN_PCS_DEVIATION_EVENT_DATE)
        dc1, dc2 = st.columns(2)
        with dc1:
            event_start = st.date_input(
                "Event on or after",
                value=default_start,
                min_value=MIN_PCS_DEVIATION_EVENT_DATE,
                max_value=date_max,
                key="pcs_deviation_start_date",
            )
        with dc2:
            event_end = st.date_input(
                "Event on or before",
                value=date_max,
                min_value=MIN_PCS_DEVIATION_EVENT_DATE,
                max_value=date_max,
                key="pcs_deviation_end_date",
            )
        if event_start > event_end:
            st.warning("Start date is after end date; results may be empty.")
        event_start_iso = event_start.isoformat()
        event_end_iso = event_end.isoformat()

    col_d, col_levels, col_cache = st.columns([2, 1, 1])
    with col_d:
        discipline_types = pcs_deviation_discipline_types(analytics, scope_key)
        discipline_names = [name for _id, name in discipline_types]
        if "pcs_deviation_disciplines" not in st.session_state:
            st.session_state["pcs_deviation_disciplines"] = discipline_names
        selected_disciplines = st.multiselect(
            "Discipline types",
            discipline_names,
            key="pcs_deviation_disciplines",
        )
        discipline_ids = (
            [dt_id for dt_id, name in discipline_types if name in selected_disciplines]
            if selected_disciplines
            else None
        )
    with col_levels:
        segment_level_preset = st.selectbox(
            "Segment levels",
            list(ELEMENT_RANKING_LEVEL_FILTER_LABELS),
            format_func=lambda key: ELEMENT_RANKING_LEVEL_FILTER_LABELS[key],
            key="pcs_deviation_segment_levels",
            help="Restrict PCS marks by ``segment.level`` (Novice / Junior / Senior presets).",
        )
        if segment_level_preset == ELEMENT_RANKING_LEVEL_FILTER_ALL:
            segment_level_preset = None
    with col_cache:
        use_precomputed_cache = st.checkbox(
            "Use precomputed cache",
            value=True,
            key="pcs_deviation_use_cache",
            help=(
                "Load cached season×discipline shards and assemble "
                "(missing shards are computed)."
            ),
        )

    _sync_pcs_deviation_benchmark_pool(
        start_season, end_season, scope_label, segment_level_preset
    )

    benchmark_start_season_year = None
    benchmark_end_season_year = None
    benchmark_scope_key = COMPETITION_SCOPE_ALL
    benchmark_segment_level_preset = None
    with st.expander("σ̂ benchmark pool", expanded=False):
        st.caption(
            "Seasons, competition scope, and segment levels used only to fit "
            "intrinsic spread (σ̂). Defaults to the same values as the ranking "
            "filters above. Event dates apply to rankings only, not to this pool."
        )
        bc1, bc2 = st.columns(2)
        with bc1:
            bench_start_pick = st.selectbox(
                "Season year from",
                ["Any"] + years,
                key="pcs_deviation_benchmark_start_season",
                on_change=_mark_pcs_deviation_benchmark_customized,
            )
            benchmark_start_season_year = (
                None if bench_start_pick == "Any" else bench_start_pick
            )
        with bc2:
            bench_end_pick = st.selectbox(
                "Season year to",
                ["Any"] + years,
                key="pcs_deviation_benchmark_end_season",
                on_change=_mark_pcs_deviation_benchmark_customized,
            )
            benchmark_end_season_year = (
                None if bench_end_pick == "Any" else bench_end_pick
            )
        bench_scope_label = st.selectbox(
            "Competition scope",
            list(PCS_DEVIATION_COMPETITION_SCOPE_LABELS),
            key="pcs_deviation_benchmark_competition_scope",
            on_change=_mark_pcs_deviation_benchmark_customized,
        )
        benchmark_scope_key = pcs_deviation_competition_scope_key(bench_scope_label)
        bench_segment_level_pick = st.selectbox(
            "Segment levels",
            list(ELEMENT_RANKING_LEVEL_FILTER_LABELS),
            format_func=lambda key: ELEMENT_RANKING_LEVEL_FILTER_LABELS[key],
            key="pcs_deviation_benchmark_segment_levels",
            on_change=_mark_pcs_deviation_benchmark_customized,
        )
        if bench_segment_level_pick == ELEMENT_RANKING_LEVEL_FILTER_ALL:
            benchmark_segment_level_preset = None
        else:
            benchmark_segment_level_preset = bench_segment_level_pick

    st.subheader("Model parameters")
    p1, p2, p3 = st.columns(3)
    with p1:
        if "pcs_deviation_min_marks" not in st.session_state:
            st.session_state["pcs_deviation_min_marks"] = 0
        min_marks = st.number_input(
            "Minimum PCS marks per judge",
            min_value=0,
            step=50,
            key="pcs_deviation_min_marks",
            help="Exclude identities with fewer marks after filters.",
        )
    with p2:
        floor_sigma = st.number_input(
            "Floor σ̂",
            min_value=0.01,
            max_value=1.0,
            value=float(_PCS_DEVIATION_FLOOR_SIGMA),
            step=0.01,
            format="%.2f",
            key="pcs_deviation_floor_sigma",
        )
    with p3:
        min_bin_count = st.number_input(
            "Min marks per σ̂ bin",
            min_value=5,
            max_value=200,
            value=int(_PCS_DEVIATION_MIN_BIN_COUNT),
            step=5,
            key="pcs_deviation_min_bin_count",
            help="(discipline, component, panel-median range) buckets with fewer marks are skipped.",
        )

    us_officials_only = st.checkbox(
        "US directory officials only",
        key="pcs_deviation_us_officials_only",
        help="Show rankings only for judges linked to a USFS official.",
    )

    disc_tuple = tuple(discipline_ids) if discipline_ids else None
    run_params = (
        start_season_year,
        end_season_year,
        disc_tuple,
        scope_key,
        event_start_iso,
        event_end_iso,
        int(min_marks),
        float(floor_sigma),
        int(min_bin_count),
        benchmark_start_season_year,
        benchmark_end_season_year,
        benchmark_scope_key,
        segment_level_preset,
        benchmark_segment_level_preset,
    )

    scope_err = validate_pcs_deviation_scope(
        start_season_year,
        end_season_year,
        available_years=years,
    )
    if scope_err:
        st.error(scope_err)

    run_clicked = st.button(
        "Run analysis",
        type="primary",
        key="pcs_deviation_run_btn",
        disabled=bool(scope_err),
    )

    if run_clicked and not scope_err:
        st.session_state.pcs_deviation_run_params = run_params
        st.session_state.pop("pcs_deviation_result", None)
        st.session_state.pop("pcs_deviation_cache_saved", None)
        st.session_state.pop("pcs_deviation_cache_error", None)
        _clear_pcs_deviation_judge_detail_cache()
        if use_precomputed_cache:
            from pcs_deviation_cache import load_cached_pcs_deviation_rankings

            cached = run_with_isolated_analytics(
                lambda cache_analytics, rp: load_cached_pcs_deviation_rankings(
                    cache_analytics.session, cache_analytics, rp
                ),
                run_params,
            )
            if cached is not None:
                st.session_state.pcs_deviation_result = cached
                st.session_state.pcs_deviation_cache_saved = True
                st.success("Loaded precomputed cache for this filter set.")
                st.rerun()
        with st.spinner("Computing PCS deviation rankings…"):
            result = run_with_isolated_analytics(
                compute_pcs_deviation_rankings_from_run_params,
                run_params,
            )
        st.session_state.pcs_deviation_result = result
        from pcs_deviation_cache import try_save_pcs_deviation_cache

        ok, err = run_with_isolated_analytics(
            lambda cache_analytics, rp, ranking_result: try_save_pcs_deviation_cache(
                cache_analytics.session,
                cache_analytics,
                rp,
                ranking_result,
            ),
            run_params,
            result,
        )
        if ok:
            st.session_state.pcs_deviation_cache_saved = True
            st.session_state.pop("pcs_deviation_cache_error", None)
        elif err:
            st.session_state.pcs_deviation_cache_error = err
        st.rerun()

    stored_params = st.session_state.get("pcs_deviation_run_params")
    base_result = st.session_state.get("pcs_deviation_result")
    if base_result is None:
        st.info(
            "Set filters and click **Run analysis**. "
            "Rankings are not computed automatically."
        )
        return

    compute_match = (
        stored_params is not None
        and run_params_same_sigma_and_ranking_scope(stored_params, run_params)
    )
    if not compute_match:
        st.warning(
            "Filters or model parameters changed since the last run. "
            "Click **Run analysis** to refresh results."
        )
        return

    result = apply_min_marks_to_pcs_deviation_result(base_result, int(min_marks))

    if us_officials_only:
        from judge_official_display_filter import (
            apply_us_officials_display_filter_to_ranking_result,
        )

        n_before_us = len(result.get("marking", pd.DataFrame()))
        result = apply_us_officials_display_filter_to_ranking_result(
            result, us_linked_identity_labels_for_ui()
        )
        n_after_us = len(result.get("marking", pd.DataFrame()))
        if n_before_us and n_after_us < n_before_us:
            st.caption(
                f"US officials display filter: **{n_after_us}** of {n_before_us} ranked "
                "identities are linked to the USFS directory."
            )

    if result.get("error"):
        st.warning(result["error"])
        return

    cache_err = st.session_state.get("pcs_deviation_cache_error")
    if cache_err:
        st.warning(
            "Rankings completed but could not be saved for reuse: "
            f"{cache_err}"
        )
    elif st.session_state.get("pcs_deviation_cache_saved"):
        st.caption("Results loaded from or saved to cache for this filter set.")

    st.subheader("Run summary")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("PCS marks (filtered)", f"{result['n_raw_marks']:,}")
    m2.metric("σ̂ bins fitted", f"{result['n_sigma_buckets']:,}")
    m3.metric("Judges ranked", len(result["marking"]))
    m4.metric("Floor σ̂", f"{floor_sigma:.2f}")

    if uses_separate_benchmark_pool(run_params):
        from pcs_deviation_analysis import PCS_DEVIATION_SCOPE_LABEL_TO_KEY

        rp = unpack_pcs_deviation_run_params(run_params)
        bench_start, bench_end = rp[9], rp[10]
        scope_labels = {v: k for k, v in PCS_DEVIATION_SCOPE_LABEL_TO_KEY.items()}
        bench_scope_label = scope_labels.get(benchmark_scope_key, benchmark_scope_key)
        rank_scope_label = scope_labels.get(scope_key, scope_key)

        def _level_label(preset: str | None) -> str:
            return ELEMENT_RANKING_LEVEL_FILTER_LABELS.get(
                preset or ELEMENT_RANKING_LEVEL_FILTER_ALL, "All"
            )

        st.caption(
            f"σ̂ benchmark pool: seasons **{bench_start or 'Any'}**–**{bench_end or 'Any'}**, "
            f"scope **{bench_scope_label}**, levels **{_level_label(benchmark_segment_level_preset)}**. "
            f"Rankings: seasons **{start_season_year or 'Any'}**–**{end_season_year or 'Any'}**, "
            f"scope **{rank_scope_label}**, levels **{_level_label(segment_level_preset)}**"
            + (
                f", events **{event_start_iso}**–**{event_end_iso}**"
                if event_start_iso and event_end_iso
                else ""
            )
            + "."
        )

    if result["marking"].empty:
        st.info("No judges remain after filters and minimum marks threshold.")
        return

    marking = result["marking"]

    st.subheader("Distribution of marking scores")
    if not result["summary"].empty:
        st.dataframe(
            result["summary"],
            width="stretch",
            hide_index=True,
        )
    fig = px.histogram(
        marking,
        x="Marking score",
        nbins=min(40, max(10, len(marking) // 5)),
        title="Marking scores across judges (lower = closer to control-score model)",
    )
    fig.update_layout(bargap=0.05)
    st.plotly_chart(fig, width="stretch")

    st.subheader("Rankings")
    st.caption(
        "**Marking score** = √(mean(m²)) over PCS marks, with m = (PCS − panel median) / σ̂. "
        "**Mean PCS bias** is the signed average raw PCS − panel median."
    )
    st.dataframe(
        marking,
        width="stretch",
        hide_index=True,
        column_config=_pcs_deviation_rankings_column_config(),
    )
    _streamlit_download(
        "Download rankings CSV",
        data=marking.to_csv(index=False).encode("utf-8"),
        file_name="pcs_deviation_rankings.csv",
        mime="text/csv",
        key="pcs_deviation_rankings_download",
    )

    st.subheader("σ̂ parameters (by discipline, component, panel-median range)")
    sigma_bins = result.get("sigma_bins", pd.DataFrame())
    if sigma_bins.empty:
        st.info("No σ̂ bins to display for the current filters.")
    else:
        st.dataframe(
            sigma_bins,
            width="stretch",
            hide_index=True,
            column_config=_pcs_deviation_sigma_bins_column_config(),
        )
        _streamlit_download(
            "Download σ̂ parameters CSV",
            data=sigma_bins.to_csv(index=False).encode("utf-8"),
            file_name="pcs_sigma_parameters.csv",
            mime="text/csv",
            key="pcs_deviation_sigma_download",
        )

    judge_names = marking["Judge"].tolist() if "Judge" in marking.columns else []
    if judge_names:
        st.subheader("How a judge’s score breaks down")
        show_drilldown = st.checkbox(
            "Show per-judge breakdown",
            value=False,
            key="pcs_deviation_show_drilldown",
            help=(
                "Load discipline, component, and panel-median tables for one judge. "
                "Leave unchecked to keep the page responsive after Run analysis."
            ),
        )
        if show_drilldown:
            st.caption(
                "Partial marking score in each table is √(mean(m²)) over PCS marks in that "
                "slice, with m = (PCS − panel median) / σ̂. **Mean PCS bias** is the signed "
                "average raw PCS − panel median (+ above, − below). Use drill-down filters to "
                "hide thin rows."
            )
            pick_judge = st.selectbox(
                "Select judge",
                judge_names,
                key="pcs_deviation_detail_judge",
            )
            judge_row = marking.loc[marking["Judge"] == pick_judge]
            if not judge_row.empty and "Mean PCS bias" in judge_row.columns:
                bias = float(judge_row["Mean PCS bias"].iloc[0])
                bias_label = (
                    "above panel median"
                    if bias > 0.005
                    else "below panel median"
                    if bias < -0.005
                    else "near panel median"
                )
                st.metric(
                    "Overall mean PCS bias",
                    f"{bias:+.3f}",
                    help=(
                        "Signed average of (judge PCS − panel median PCS) over all PCS "
                        "marks for this judge. Positive = systematically higher than the "
                        "panel median; negative = systematically lower."
                    ),
                )
                st.caption(
                    f"On average this judge marks **{bias_label}** "
                    f"({bias:+.3f} PCS vs panel median per mark)."
                )

            detail_cfg = _pcs_deviation_judge_detail_column_config()
            jd = result.get("judge_discipline_detail", pd.DataFrame())
            jc = result.get("judge_component_detail", pd.DataFrame())
            jb = result.get("judge_control_bin_detail", pd.DataFrame())
            if jd.empty or pick_judge not in set(jd.get("Judge", [])):
                jd, jc, jb = _pcs_deviation_load_judge_breakdown(
                    result, run_params, pick_judge
                )
            else:
                jd = jd.loc[jd["Judge"] == pick_judge] if not jd.empty else jd
                jc = jc.loc[jc["Judge"] == pick_judge] if not jc.empty else jc
                jb = jb.loc[jb["Judge"] == pick_judge] if not jb.empty else jb

            if jd.empty and jc.empty and jb.empty:
                st.info(
                    _pcs_deviation_breakdown_failure_hint(
                        result, run_params, pick_judge
                    )
                )
            else:
                from pcs_deviation_takeaways import (
                    filter_pcs_judge_detail_tables,
                    pcs_detail_filter_options,
                )

                disc_options, comp_options = pcs_detail_filter_options(jd, jc, jb)
                judge_filter_key = pick_judge.replace(" ", "_")[:48]
                with st.expander("Drill-down filters", expanded=False):
                    filt_c1, filt_c2, filt_c3 = st.columns(3)
                    with filt_c1:
                        detail_min_marks = st.number_input(
                            "Minimum marks per row",
                            min_value=0,
                            value=0,
                            step=5,
                            key=f"pcs_deviation_detail_min_marks_{judge_filter_key}",
                            help="Hide breakdown rows with fewer PCS marks than this.",
                        )
                    with filt_c2:
                        detail_disciplines = st.multiselect(
                            "Discipline",
                            disc_options,
                            default=disc_options,
                            key=f"pcs_deviation_detail_disciplines_{judge_filter_key}",
                            help=(
                                "Defaults to all disciplines in this breakdown. "
                                "Clear all to include every discipline present."
                            ),
                        )
                    with filt_c3:
                        detail_components = st.multiselect(
                            "PCS component",
                            comp_options,
                            default=comp_options,
                            key=f"pcs_deviation_detail_components_{judge_filter_key}",
                            help=(
                                "Defaults to all components for this judge. "
                                "Clear all to include every component in the breakdown."
                            ),
                        )

                jd_f, jc_f, jb_f = filter_pcs_judge_detail_tables(
                    jd,
                    jc,
                    jb,
                    min_marks=int(detail_min_marks) if int(detail_min_marks) > 0 else None,
                    disciplines=detail_disciplines or None,
                    components=detail_components or None,
                )
                if jd_f.empty and jc_f.empty and jb_f.empty:
                    st.warning(
                        "No breakdown rows match the drill-down filters. "
                        "Lower the minimum marks or clear discipline / component filters."
                    )
                else:
                    if not jd_f.empty:
                        st.markdown(
                            "**By discipline** "
                            "(partial score = √(mean(m²)) within that discipline; σ̂-scaled)"
                        )
                        st.dataframe(
                            jd_f.sort_values("Partial marking score"),
                            width="stretch",
                            hide_index=True,
                            column_config=detail_cfg,
                        )
                    if not jc_f.empty:
                        st.markdown(
                            "**By discipline and component** "
                            "(largest normalized partial scores first)"
                        )
                        st.dataframe(
                            jc_f.sort_values("Partial marking score", ascending=False),
                            width="stretch",
                            hide_index=True,
                            column_config=detail_cfg,
                        )
                    if not jb_f.empty:
                        st.markdown(
                            "**By panel-median range** "
                            "(discipline × component × panel-median PCS bin)"
                        )
                        st.caption(
                            "Each row is one σ̂ bin. Partial score is √(mean(m²)) using "
                            "normalized marks m = (PCS − panel median) / σ̂."
                        )
                        chart_series_options = _pcs_deviation_chart_series_options(
                            jc_f if not jc_f.empty else jb_f
                        )
                        chart_c1, chart_c2 = st.columns([3, 1])
                        with chart_c1:
                            chart_series = st.multiselect(
                                "Chart series (discipline × component)",
                                chart_series_options,
                                default=chart_series_options,
                                key=f"pcs_deviation_chart_series_{judge_filter_key}",
                                help=(
                                    "Each line plots the selected Y metric vs panel-median "
                                    "PCS range. Points match the drill-down filters above."
                                ),
                            )
                        with chart_c2:
                            chart_y_metric = st.selectbox(
                                "Chart Y axis",
                                options=[
                                    _PCS_DEVIATION_CHART_Y_PARTIAL,
                                    _PCS_DEVIATION_CHART_Y_MEAN_ABS_ERROR,
                                ],
                                format_func=lambda k: {
                                    _PCS_DEVIATION_CHART_Y_PARTIAL: "Partial marking score",
                                    _PCS_DEVIATION_CHART_Y_MEAN_ABS_ERROR: "Mean |error|",
                                }[k],
                                key=f"pcs_deviation_chart_y_{judge_filter_key}",
                                help=(
                                    "Partial marking score uses σ̂-normalized marks; "
                                    "Mean |error| is average |PCS − panel median| in raw PCS units."
                                ),
                            )
                        chart_min_marks = (
                            int(detail_min_marks) if int(detail_min_marks) > 0 else 0
                        )
                        pcs_fig = _pcs_deviation_control_bin_chart(
                            jb_f,
                            jc_f if not jc_f.empty else jb_f,
                            min_bin_marks=chart_min_marks,
                            series_keys=chart_series or None,
                            y_metric=chart_y_metric,
                        )
                        if pcs_fig is not None:
                            st.plotly_chart(pcs_fig, width="stretch")
                        elif chart_series:
                            st.caption(
                                "No chart: selected series have no rows under the "
                                "current drill-down filters."
                            )
                        st.dataframe(
                            jb_f.sort_values("Partial marking score", ascending=False),
                            width="stretch",
                            hide_index=True,
                            column_config=detail_cfg,
                        )


def temporal_trend_analysis():
    """Temporal Trend Analysis for Judge Consistency"""
    st.header("Temporal Trend Analysis")

    # Analysis configuration
    st.subheader("Analysis Configuration")
    col1, col2, col3 = st.columns(3)

    with col1:
        analysis_type = st.selectbox(
            "Analysis Type",
            [
                "Individual Judge Trends",
                "Overall System Trends",
                "Judge Consistency Ranking",
            ],
            key="temporal_analysis_type",
        )

    with col2:
        metric = st.selectbox(
            "Performance Metric",
            [
                "throwout_rate",
                "anomaly_rate",
                "rule_error_rate",
                "avg_deviation",
            ],
            key="temporal_metric",
                              format_func=lambda x: {
                                  "throwout_rate": "Throwout Rate (%)",
                                  "anomaly_rate": "Anomaly Rate (%)",
                                  "rule_error_rate": "Rule Error Rate (%)",
                                  "avg_deviation": "Average Deviation"
                              }[x])

    with col3:
        score_type = st.selectbox(
            "Score Type",
            ["both", "pcs", "element"],
            key="temporal_score_type",
            format_func=lambda x: {
                                      "both": "Combined (PCS + Elements)",
                                      "pcs": "PCS Only",
                                      "element": "Elements Only"
                                  }[x])

    temporal_scope_label = st.selectbox(
        "Competition scope",
        list(_COMPETITION_SCOPE_LABELS),
        index=0,
        key="temporal_competition_scope",
        help=(
            "Restricts scores to competitions whose linked officials type matches the scope "
            "(same as Cross-Judge and Individual Judge). Scoped modes use Singles / Pairs / "
            "Ice Dance / Synchronized segments only."
        ),
    )
    temporal_competition_scope = _competition_scope_key(temporal_scope_label)

    metric_names = {
        "throwout_rate": "Throwout Rate (%)",
        "anomaly_rate": "Anomaly Rate (%)",
        "rule_error_rate": "Rule Error Rate (%)",
        "avg_deviation": "Average Deviation"
    }

    if analysis_type == "Individual Judge Trends":
        # Judge selection
        analytics = get_analytics_safe()
        ig_labels, ig_map = _identity_group_options(analytics)
        if not ig_labels:
            st.error("No judges found in database")
            return

        selected_judge_display = st.selectbox(
            "Select Judge",
            ig_labels,
            key="temporal_judge_select",
        )
        selected_judge_ids = ig_map[selected_judge_display]

        # Get temporal trends data
        with st.spinner("Loading temporal trends data..."):
            trends_df = analytics.get_temporal_trends_data(
                judge_id=selected_judge_ids,
                period='year',
                metric=metric,
                score_type=score_type,
                competition_scope=temporal_competition_scope,
            )
            consistency_metrics = JudgeAnalytics.consistency_metrics_from_trends_df(
                trends_df
            )

        if trends_df.empty:
            st.warning("No temporal data found for selected judge")
            return

        # Display consistency metrics
        st.subheader("Consistency Metrics")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            trend_direction = consistency_metrics['trend_direction']
            direction_emoji = {
                "increasing": "📈",
                "decreasing": "📉",
                "stable": "➖",
                "insufficient_data": "❓"
            }
            st.metric(
                "Trend Direction",
                f"{direction_emoji.get(trend_direction, '❓')} {trend_direction.title()}"
            )

        with col2:
            st.metric("Consistency Score",
                      f"{consistency_metrics['consistency_score']:.1f}%")

        with col3:
            st.metric("Trend Strength",
                      f"{consistency_metrics['trend_strength']:.3f}")

        with col4:
            st.metric("Coefficient of Variation",
                      f"{consistency_metrics['coefficient_variation']:.1f}%")

        # Create temporal trend chart
        fig = px.line(
            trends_df,
            x='time_period',
            y='metric_value',
            title=f"{selected_judge_display}: {metric_names[metric]} Over Time",
            labels={
                'time_period': 'Year',
                'metric_value': metric_names[metric]
            },
            markers=True)

        # Add trend line
        if len(trends_df) > 1:
            slope = consistency_metrics['slope']
            intercept = trends_df['metric_value'].iloc[0]
            trend_line = [intercept + slope * i for i in range(len(trends_df))]

            fig.add_scatter(x=trends_df['time_period'],
                            y=trend_line,
                            mode='lines',
                            name='Trend Line',
                            line=dict(dash='dash', color='red'))

        fig.update_layout(height=500)
        st.plotly_chart(fig, width="stretch")

        # Show detailed data
        st.subheader("Detailed Trends Data")
        display_df = trends_df[[
            'time_period', 'metric_value', 'total_scores', 'pcs_scores',
            'element_scores'
        ]].copy()
        display_df.columns = [
            'Year', metric_names[metric], 'Total Scores', 'PCS Scores',
            'Element Scores'
        ]
        st.dataframe(display_df, width="stretch")

    elif analysis_type == "Overall System Trends":
        # Get system-wide temporal trends
        with st.spinner("Loading system-wide trends data..."):
            analytics = get_analytics_safe()
            trends_df = analytics.get_temporal_trends_data(
                judge_id=None,
                period='year',
                metric=metric,
                score_type=score_type,
                competition_scope=temporal_competition_scope,
            )

        if trends_df.empty:
            st.warning("No system-wide temporal data found")
            return

        # Create multi-line chart for system trends
        fig = go.Figure()

        fig.add_trace(
            go.Scatter(x=trends_df['time_period'],
                       y=trends_df['avg_metric_value'],
                       mode='lines+markers',
                       name='Average',
                       line=dict(color='blue', width=3)))

        fig.add_trace(
            go.Scatter(x=trends_df['time_period'],
                       y=trends_df['median_metric_value'],
                       mode='lines+markers',
                       name='Median',
                       line=dict(color='green', width=2)))

        # Add error bars for standard deviation
        fig.add_trace(
            go.Scatter(x=trends_df['time_period'],
                       y=trends_df['avg_metric_value'] +
                       trends_df['std_metric_value'],
                       mode='lines',
                       line=dict(width=0),
                       showlegend=False,
                       hoverinfo='skip'))

        fig.add_trace(
            go.Scatter(x=trends_df['time_period'],
                       y=trends_df['avg_metric_value'] -
                       trends_df['std_metric_value'],
                       mode='lines',
                       line=dict(width=0),
                       fill='tonexty',
                       fillcolor='rgba(0,100,80,0.2)',
                       name='±1 Std Dev',
                       hoverinfo='skip'))

        fig.update_layout(
            title=f"System-Wide {metric_names[metric]} Trends Over Time",
            xaxis_title='Year',
            yaxis_title=metric_names[metric],
            height=500)

        st.plotly_chart(fig, width="stretch")

        # System statistics
        st.subheader("System Statistics")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            overall_avg = trends_df['avg_metric_value'].mean()
            st.metric("Overall Average", f"{overall_avg:.2f}")

        with col2:
            overall_trend = "Improving" if trends_df['avg_metric_value'].iloc[
                -1] < trends_df['avg_metric_value'].iloc[0] else "Worsening"
            st.metric("Overall Trend", overall_trend)

        with col3:
            avg_judges_per_year = trends_df['total_judges'].mean()
            st.metric("Avg Judges/Year", f"{avg_judges_per_year:.0f}")

        with col4:
            total_scores = trends_df['total_scores'].sum()
            st.metric("Total Scores Analyzed", f"{total_scores:,}")

        # Show detailed system data
        st.subheader("System Trends Data")
        display_df = trends_df[[
            'time_period', 'avg_metric_value', 'median_metric_value',
            'std_metric_value', 'total_judges', 'total_scores'
        ]].copy()
        display_df.columns = [
            'Year', f'Avg {metric_names[metric]}',
            f'Median {metric_names[metric]}', 'Std Dev', 'Total Judges',
            'Total Scores'
        ]
        for col in [
                f'Avg {metric_names[metric]}',
                f'Median {metric_names[metric]}', 'Std Dev'
        ]:
            display_df[col] = display_df[col].round(2)
        st.dataframe(display_df, width="stretch")

    else:  # Judge Consistency Ranking
        # One row per identity group (merged judge names sharing one directory official)
        analytics = get_analytics_safe()
        ig_labels, ig_map = _identity_group_options(analytics)
        if not ig_labels:
            st.error("No judges found in database")
            return

        with st.spinner("Calculating consistency metrics for all judges..."):
            consistency_df = analytics.get_identity_group_consistency_ranking(
                ig_map,
                metric=metric,
                score_type=score_type,
                competition_scope=temporal_competition_scope,
            )

        if consistency_df.empty:
            st.warning("No consistency data found")
            return

        consistency_df = consistency_df.sort_values('consistency_score',
                                                    ascending=False)

        # Create consistency ranking chart
        fig = px.bar(
            consistency_df.head(20),  # Top 20 most consistent judges
            x='consistency_score',
            y='judge_name',
            orientation='h',
            title=f"Top 20 Most Consistent Judges: {metric_names[metric]}",
            labels={
                'consistency_score': 'Consistency Score (%)',
                'judge_name': 'Judge'
            },
            color='consistency_score',
            color_continuous_scale='Greens')

        fig.update_layout(height=max(400, len(consistency_df.head(20)) * 25))
        st.plotly_chart(fig, width="stretch")

        # Show consistency ranking table
        st.subheader("Judge Consistency Rankings")
        display_df = consistency_df[[
            'judge_name', 'location', 'consistency_score', 'trend_direction',
            'coefficient_variation', 'years_active', 'total_scores'
        ]].copy()
        display_df.columns = [
            'Judge', 'Location', 'Consistency Score (%)', 'Trend Direction',
            'Coeff. of Variation (%)', 'Years Active', 'Total Scores'
        ]
        display_df['Consistency Score (%)'] = display_df[
            'Consistency Score (%)'].round(1)
        display_df['Coeff. of Variation (%)'] = display_df[
            'Coeff. of Variation (%)'].round(1)
        st.dataframe(display_df, width="stretch")


if page == "Individual Judge Analysis":
    st.header("Individual Judge Analysis")

    # Judge selection
    analytics = get_analytics_safe()
    ig_labels, ig_map = _identity_group_options(analytics)
    if not ig_labels:
        st.error("No judges found in database")
        st.stop()

    selected_judge_display = st.selectbox(
        "Select Judge",
        ig_labels,
        key="individual_judge_select",
    )
    selected_judge_ids = ig_map[selected_judge_display]

    # Filters
    st.subheader("Filters")
    individual_scope_label = st.selectbox(
        "Competition scope",
        list(_COMPETITION_SCOPE_LABELS),
        index=0,
        key="individual_judge_competition_scope",
        help=(
            "Qualifying: linked officials type is set and not id 11 (nonqualifying). "
            "NQS: linked officials type id 10. "
            "Sectionals & championships: types 1–9 (excludes NQS and nonqualifying). "
            "Championships only: types 4 (US Championships) and 8 (US Synchro Championships)."
        ),
    )
    individual_competition_scope = _competition_scope_key(individual_scope_label)

    col1, col2 = st.columns(2)

    with col1:
        years = analytics.get_judge_years(
            selected_judge_ids,
            competition_scope=individual_competition_scope,
        )
        year_filter = st.selectbox(
            "Filter by season year",
            ["All Years"] + years,
            key="individual_judge_year",
            help=(
                "USFS season year on each competition record (``competition.year``), "
                "for events this judge scored or appeared on in protocol data."
            ),
        )
        year_filter = None if year_filter == "All Years" else year_filter

    with col2:
        discipline_types = (
            analytics.qualifying_event_segment_discipline_types()
            if individual_competition_scope != COMPETITION_SCOPE_ALL
            else analytics.get_discipline_types()
        )
        discipline_names = [name for dt_id, name in discipline_types]
        selected_disciplines = st.multiselect(
            "Filter by Discipline Type",
            discipline_names,
            key="individual_judge_disciplines",
            help="Also limits the competition list and protocol roles to these segment disciplines.",
        )
        discipline_ids = [
            dt_id for dt_id, name in discipline_types
            if name in selected_disciplines
        ] if selected_disciplines else None

    use_event_dates = st.checkbox(
        "Filter by competition event dates",
        key="individual_judge_use_event_dates",
        help=(
            "Uses each competition's start date when set, otherwise its end date. "
            "Events with neither date are excluded when this filter is on."
        ),
    )
    event_start_date = None
    event_end_date = None
    if use_event_dates:
        date_min, date_max = analytics.get_competition_event_date_bounds(
            selected_judge_ids,
            competition_scope=individual_competition_scope,
            discipline_type_ids=discipline_ids,
        )
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            event_start_date = st.date_input(
                "Event on or after",
                value=date_min,
                min_value=date_min,
                max_value=date_max,
                key="individual_judge_start_date",
            )
        with date_col2:
            event_end_date = st.date_input(
                "Event on or before",
                value=date_max,
                min_value=date_min,
                max_value=date_max,
                key="individual_judge_end_date",
            )
        if event_start_date > event_end_date:
            st.warning("Start date is after end date; results may be empty.")

    judge_competitions = analytics.get_judge_competitions(
        selected_judge_ids,
        competition_scope=individual_competition_scope,
        discipline_type_ids=discipline_ids,
        event_start_date=event_start_date if use_event_dates else None,
        event_end_date=event_end_date if use_event_dates else None,
    )
    competition_names = [
        f"{name} ({year})" for comp_id, name, year in judge_competitions
    ]
    selected_competitions = st.multiselect(
        "Filter by Competitions",
        competition_names,
        help="Ordered by event date (start date, or end date if missing), newest first. "
        "Includes scored events and protocol appearances (segment_official) in the "
        "selected discipline types. Only competitions matching the competition scope above "
        "are listed.",
    )
    competition_ids = [
        comp_id for comp_id, name, year in judge_competitions
        if f"{name} ({year})" in selected_competitions
    ] if selected_competitions else None

    # Get data
    with st.spinner("Loading judge data..."):
        pcs_df = analytics.get_judge_pcs_stats(
            selected_judge_ids,
            year_filter,
            competition_ids,
            discipline_ids,
            competition_scope=individual_competition_scope,
            event_start_date=event_start_date if use_event_dates else None,
            event_end_date=event_end_date if use_event_dates else None,
        )
        element_df = analytics.get_judge_element_stats(
            selected_judge_ids,
            year_filter,
            competition_ids,
            discipline_ids,
            competition_scope=individual_competition_scope,
            event_start_date=event_start_date if use_event_dates else None,
            event_end_date=event_end_date if use_event_dates else None,
        )
        segment_df = analytics.get_judge_segment_stats(
            selected_judge_ids,
            year_filter,
            competition_ids,
            discipline_ids,
            competition_scope=individual_competition_scope,
            event_start_date=event_start_date if use_event_dates else None,
            event_end_date=event_end_date if use_event_dates else None,
        )

    comp_pairs = _individual_judge_protocol_competition_pairs(
        judge_competitions,
        pcs_df,
        element_df,
        year_filter,
        competition_ids,
    )

    if pcs_df.empty and element_df.empty:
        st.warning(
            "No PCS or element score data for this judge with the selected filters. "
            "The protocol roles table below may still list protocol appearances in the "
            "selected disciplines when segment_official data exists."
        )

    if comp_pairs:
        st.subheader("Competitions & protocol roles")
        roles_df, roles_cap = analytics.get_judge_competition_protocol_roles_rows(
            selected_judge_ids,
            comp_pairs,
            competition_scope=individual_competition_scope,
            discipline_type_ids=discipline_ids,
        )
        if roles_cap:
            st.caption(roles_cap)
        st.dataframe(
            roles_df,
            width="stretch",
            hide_index=True,
            column_config={
                "Distinct protocol roles": st.column_config.TextColumn(
                    "Distinct protocol roles",
                    width="large",
                    help="Distinct appointment/protocol roles from segment_official for this judge's "
                    "linked official (or, if unlinked, rows whose protocol official_name matches the "
                    "judge name), sorted alphabetically within each competition.",
                ),
            },
        )
        st.caption(
            "Rows ordered by competition event date (start, else end), newest first; missing dates "
            "last. Roles within a row are alphabetical and limited to the selected discipline "
            "types (same as PCS/element stats). Includes competitions from score filters plus "
            "protocol-only events in the competition filter."
        )

    # Summary statistics
    stats = analytics.calculate_judge_summary_stats(pcs_df, element_df)

    st.subheader("Summary Statistics")
    col1, col2 = st.columns(2)

    with col1:
        st.write("**PCS Statistics**")
        st.metric("Total PCS Scores", stats['pcs_total_scores'])
        st.metric("PCS Throwout Rate",
                  f"{stats['pcs_throwout_rate']:.1f}%")
        st.metric("PCS Anomaly Rate", f"{stats['pcs_anomaly_rate']:.1f}%")
        st.metric("PCS Rule Error Rate",
                  f"{stats['pcs_rule_error_rate']:.1f}%")

    with col2:
        st.write("**Element Statistics**")
        st.metric("Total Element Scores", stats['element_total_scores'])
        st.metric("Element Throwout Rate",
                  f"{stats['element_throwout_rate']:.1f}%")
        st.metric("Element Anomaly Rate",
                  f"{stats['element_anomaly_rate']:.1f}%")
        st.metric("Element Rule Error Rate",
                  f"{stats['element_rule_error_rate']:.1f}%")

    # Analysis Tables - Elements first, then PCS
    if not element_df.empty:
        st.subheader("Element Analysis")

        # Element analysis by type with high/low breakdown
        def analyze_element_issues(group):
            total_scores = len(group)
            throwouts = group['thrown_out'].sum()
            anomalies = group['anomaly'].sum()
            rule_errors = group['is_rule_error'].sum()

            # High/Low breakdown for throwouts and anomalies
            throwout_high = ((group['thrown_out'] == True) &
                             (group['deviation'] > 0)).sum()
            throwout_low = ((group['thrown_out'] == True) &
                            (group['deviation'] < 0)).sum()
            anomaly_high = ((group['anomaly'] == True) &
                            (group['deviation'] > 0)).sum()
            anomaly_low = ((group['anomaly'] == True) &
                           (group['deviation'] < 0)).sum()

            return pd.Series({
                'total_scores': total_scores,
                'throwouts': throwouts,
                'throwout_high': throwout_high,
                'throwout_low': throwout_low,
                'anomalies': anomalies,
                'anomaly_high': anomaly_high,
                'anomaly_low': anomaly_low,
                'rule_errors': rule_errors
            })

        element_summary = element_df.groupby('element_type_name').apply(
            analyze_element_issues, include_groups=False).reset_index()
        element_summary['throwout_rate'] = (
            element_summary['throwouts'] /
            element_summary['total_scores']) * 100
        element_summary['anomaly_rate'] = (
            element_summary['anomalies'] /
            element_summary['total_scores']) * 100
        element_summary['rule_error_rate'] = (
            element_summary['rule_errors'] /
            element_summary['total_scores']) * 100

        # Display as table with high/low breakdown
        st.subheader("Element Rates by Element Type")
        display_summary = element_summary[[
            'element_type_name', 'total_scores', 'throwouts',
            'throwout_high', 'throwout_low', 'throwout_rate', 'anomalies',
            'anomaly_high', 'anomaly_low', 'anomaly_rate', 'rule_errors',
            'rule_error_rate'
        ]].copy()
        display_summary.columns = [
            'Element Type', 'Total Scores', 'Throwouts', 'High Throwouts',
            'Low Throwouts', 'Throwout Rate (%)', 'Anomalies (>2.0)',
            'High Anomalies', 'Low Anomalies', 'Anomaly Rate (%)',
            'Rule Errors', 'Rule Error Rate (%)'
        ]
        for col in [
                'Throwout Rate (%)', 'Anomaly Rate (%)',
                'Rule Error Rate (%)'
        ]:
            display_summary[col] = display_summary[col].round(1)
        st.dataframe(
            display_summary,
            width="stretch",
            hide_index=True,
            column_config={
                "Element Type": st.column_config.TextColumn(
                    "Element Type",
                    pinned=True,
                ),
            },
        )

        # Detailed element scores with issues
        st.subheader("Element Score Details")

        # Issue type filters
        col1, col2, col3 = st.columns(3)
        with col1:
            show_thrown_out = st.checkbox("Thrown Out",
                                          value=True,
                                          key="elem_thrown_out")
        with col2:
            show_anomalies = st.checkbox("Anomalies",
                                         value=True,
                                         key="elem_anomalies")
        with col3:
            show_rule_errors = st.checkbox("Rule Errors",
                                           value=True,
                                           key="elem_rule_errors")

        # Filter based on selected issue types
        issue_filter = ((element_df['thrown_out'] & show_thrown_out) |
                        (element_df['anomaly'] & show_anomalies) |
                        (element_df['is_rule_error'] & show_rule_errors))
        problem_elements = element_df[issue_filter].copy()

        if not problem_elements.empty:

            def get_issue_type(row):
                issues = []
                if row['thrown_out']:
                    direction = 'High' if row['deviation'] > 0 else 'Low'
                    issues.append(f'Thrown Out ({direction})')
                if row['anomaly']:
                    direction = 'High' if row['deviation'] > 0 else 'Low'
                    issues.append(f'Anomaly ({direction})')
                if row['is_rule_error']:
                    issues.append('Rule Error')
                return ', '.join(issues)

            problem_elements['issue_type'] = problem_elements.apply(
                get_issue_type, axis=1)

            # Create display dataframe with proper competition links
            display_df = problem_elements[[
                'competition_name', 'competition_url', 'year',
                'segment_name', 'skater_name', 'element_name',
                'element_type_name', 'judge_score', 'panel_average',
                'deviation', 'issue_type'
            ]].copy()
            display_df = display_df.drop(columns=['competition_url']).rename(
                columns={
                    'competition_name': 'Competition',
                    'year': 'Year',
                    'segment_name': 'Segment',
                    'skater_name': 'Skater',
                    'element_name': 'Element Name',
                    'element_type_name': 'Element Type',
                    'judge_score': 'Judge Score',
                    'panel_average': 'Panel Average',
                    'deviation': 'Deviation',
                    'issue_type': 'Issue Type',
                }
            )

            st.dataframe(
                display_df,
                width="stretch",
                hide_index=True,
                column_config={
                    "Element Type": st.column_config.TextColumn(
                        "Element Type",
                        pinned=True,
                    ),
                },
            )

            # Show competition links separately
            if 'competition_url' in problem_elements.columns and not problem_elements['competition_url'].isna().all():
                st.subheader("Competition Links")
                unique_competitions = problem_elements[[
                    'competition_name', 'competition_url'
                ]].drop_duplicates()
                for _, row in unique_competitions.iterrows():
                    if pd.notna(row['competition_url']) and row['competition_url']:
                        href = _competition_results_index_url(row['competition_url'])
                        if href:
                            st.markdown(
                                f"[{row['competition_name']}]({href})"
                            )
        else:
            st.info(
                "No element scores with issues found for selected filters")

    if not pcs_df.empty:
        st.subheader("PCS Analysis")

        # PCS throwout and deviation rates by type with high/low breakdown
        def analyze_pcs_issues(group):
            total_scores = len(group)
            throwouts = group['thrown_out'].sum()
            anomalies = group['anomaly'].sum()
            rule_errors = group['is_rule_error'].sum()

            # High/Low breakdown for throwouts and anomalies
            throwout_high = ((group['thrown_out'] == True) &
                             (group['deviation'] > 0)).sum()
            throwout_low = ((group['thrown_out'] == True) &
                            (group['deviation'] < 0)).sum()
            anomaly_high = ((group['anomaly'] == True) &
                            (group['deviation'] > 0)).sum()
            anomaly_low = ((group['anomaly'] == True) &
                           (group['deviation'] < 0)).sum()

            return pd.Series({
                'total_scores': total_scores,
                'throwouts': throwouts,
                'throwout_high': throwout_high,
                'throwout_low': throwout_low,
                'anomalies': anomalies,
                'anomaly_high': anomaly_high,
                'anomaly_low': anomaly_low,
                'rule_errors': rule_errors
            })

        pcs_summary = pcs_df.groupby('pcs_type_name').apply(
            analyze_pcs_issues, include_groups=False).reset_index()
        pcs_summary['throwout_rate'] = (pcs_summary['throwouts'] /
                                        pcs_summary['total_scores']) * 100
        pcs_summary['anomaly_rate'] = (pcs_summary['anomalies'] /
                                       pcs_summary['total_scores']) * 100
        pcs_summary['rule_error_rate'] = (
            pcs_summary['rule_errors'] / pcs_summary['total_scores']) * 100

        # Display as table with high/low breakdown
        st.subheader("PCS Rates by Component Type")
        display_summary = pcs_summary[[
            'pcs_type_name', 'total_scores', 'throwouts', 'throwout_high',
            'throwout_low', 'throwout_rate', 'anomalies', 'anomaly_high',
            'anomaly_low', 'anomaly_rate', 'rule_errors', 'rule_error_rate'
        ]].copy()
        display_summary.columns = [
            'PCS Component', 'Total Scores', 'Throwouts', 'High Throwouts',
            'Low Throwouts', 'Throwout Rate (%)', 'Anomalies (>1.5)',
            'High Anomalies', 'Low Anomalies', 'Anomaly Rate (%)',
            'Rule Errors', 'Rule Error Rate (%)'
        ]
        for col in [
                'Throwout Rate (%)', 'Anomaly Rate (%)',
                'Rule Error Rate (%)'
        ]:
            display_summary[col] = display_summary[col].round(1)
        st.dataframe(
            display_summary,
            width="stretch",
            hide_index=True,
            column_config={
                "PCS Component": st.column_config.TextColumn(
                    "PCS Component",
                    pinned=True,
                ),
            },
        )

        # Detailed PCS scores with issues
        st.subheader("PCS Score Details")

        # Issue type filters
        col1, col2, col3 = st.columns(3)
        with col1:
            show_thrown_out_pcs = st.checkbox("Thrown Out",
                                              value=True,
                                              key="pcs_thrown_out")
        with col2:
            show_anomalies_pcs = st.checkbox("Anomalies",
                                             value=True,
                                             key="pcs_anomalies")
        with col3:
            show_rule_errors_pcs = st.checkbox("Rule Errors",
                                               value=True,
                                               key="pcs_rule_errors")

        # Filter based on selected issue types
        issue_filter_pcs = (
            (pcs_df['thrown_out'] & show_thrown_out_pcs) |
            (pcs_df['anomaly'] & show_anomalies_pcs) |
            (pcs_df['is_rule_error'] & show_rule_errors_pcs))
        problem_pcs = pcs_df[issue_filter_pcs].copy()

        if not problem_pcs.empty:

            def get_issue_type(row):
                issues = []
                if row['thrown_out']:
                    direction = 'High' if row['deviation'] > 0 else 'Low'
                    issues.append(f'Thrown Out ({direction})')
                if row['anomaly']:
                    direction = 'High' if row['deviation'] > 0 else 'Low'
                    issues.append(f'Anomaly ({direction})')
                if row['is_rule_error']:
                    issues.append('Rule Error')
                return ', '.join(issues)

            problem_pcs['issue_type'] = problem_pcs.apply(get_issue_type,
                                                          axis=1)

            # Create display dataframe with proper competition links
            display_df_pcs = problem_pcs[[
                'competition_name', 'competition_url', 'year',
                'segment_name', 'skater_name', 'pcs_type_name',
                'judge_score', 'panel_average', 'deviation', 'issue_type'
            ]].copy()
            display_df_pcs = display_df_pcs.drop(columns=['competition_url']).rename(
                columns={
                    'competition_name': 'Competition',
                    'year': 'Year',
                    'segment_name': 'Segment',
                    'skater_name': 'Skater',
                    'pcs_type_name': 'PCS Component',
                    'judge_score': 'Judge Score',
                    'panel_average': 'Panel Average',
                    'deviation': 'Deviation',
                    'issue_type': 'Issue Type',
                }
            )

            st.dataframe(
                display_df_pcs,
                width="stretch",
                hide_index=True,
                column_config={
                    "PCS Component": st.column_config.TextColumn(
                        "PCS Component",
                        pinned=True,
                    ),
                },
            )

            # Show competition links separately
            if 'competition_url' in problem_pcs.columns and not problem_pcs['competition_url'].isna().all():
                st.subheader("Competition Links")
                unique_competitions_pcs = problem_pcs[[
                    'competition_name', 'competition_url'
                ]].drop_duplicates()
                for _, row in unique_competitions_pcs.iterrows():
                    if pd.notna(row['competition_url']) and row['competition_url']:
                        href = _competition_results_index_url(row['competition_url'])
                        if href:
                            st.markdown(
                                f"[{row['competition_name']}]({href})"
                            )
        else:
            st.info("No PCS scores with issues found for selected filters")

    # Segment Statistics
    if not segment_df.empty:
        st.subheader("Segment Statistics")
        st.write("Performance summary for segments judged by this judge:")

        # Sort by total anomalies + rule errors descending
        segment_display = segment_df.copy()
        segment_display['total_issues'] = segment_display['total_anomalies'] + segment_display['total_rule_errors']
        segment_display = segment_display.sort_values('total_issues', ascending=False)

        # Calculate allowed errors and excess
        def calculate_allowed_errors(skater_count):
            if skater_count <= 10:
                return 1
            elif skater_count <= 20:
                return 2
            elif skater_count <= 30:
                return 3
            elif skater_count <= 40:
                return 4
            return 5

        segment_display['allowed_errors'] = segment_display['skater_count'].apply(calculate_allowed_errors)
        segment_display['excess_anomalies'] = segment_display['total_anomalies'] - segment_display['allowed_errors']
        segment_display['excess_anomalies'] = segment_display['excess_anomalies'].apply(lambda x: max(0, x))

        # Format the display columns
        segment_display_cols = segment_display[[
            'competition_name', 'competition_year', 'discipline', 'segment_name',
            'skater_count', 'allowed_errors', 'total_anomalies', 'excess_anomalies',
            'pcs_anomalies', 'element_anomalies', 'total_rule_errors', 
            'pcs_rule_errors', 'element_rule_errors'
        ]].copy()

        segment_display_cols.columns = [
            'Competition', 'Year', 'Discipline', 'Segment',
            'Skaters', 'Allowed Errors', 'Total Anomalies', 'Excess Anomalies',
            'PCS Anomalies', 'Element Anomalies', 'Total Rule Errors', 
            'PCS Rule Errors', 'Element Rule Errors'
        ]

        # Add totals row
        totals_row = pd.DataFrame([{
            'Competition': 'TOTAL',
            'Year': '',
            'Discipline': '',
            'Segment': '',
            'Skaters': segment_display_cols['Skaters'].sum(),
            'Allowed Errors': segment_display_cols['Allowed Errors'].sum(),
            'Total Anomalies': segment_display_cols['Total Anomalies'].sum(),
            'Excess Anomalies': segment_display_cols['Excess Anomalies'].sum(),
            'PCS Anomalies': segment_display_cols['PCS Anomalies'].sum(),
            'Element Anomalies': segment_display_cols['Element Anomalies'].sum(),
            'Total Rule Errors': segment_display_cols['Total Rule Errors'].sum(),
            'PCS Rule Errors': segment_display_cols['PCS Rule Errors'].sum(),
            'Element Rule Errors': segment_display_cols['Element Rule Errors'].sum()
        }])

        segment_display_with_totals = pd.concat([segment_display_cols, totals_row], ignore_index=True)

        st.dataframe(segment_display_with_totals, width="stretch")
    else:
        st.info("No segment data found for selected filters")

    # Download Report
    st.divider()
    st.subheader("Download Judge Report")
    st.write(
        "Download a report for this judge containing only their data — "
        "safe to share without exposing other judges' information."
    )

    safe_name = selected_judge_display.replace(' ', '_').replace('/', '_')
    single_competition_display_name = None
    report_filter_lines = None
    if competition_ids is not None and len(competition_ids) == 1:
        _cid = competition_ids[0]
        for comp_id, name, year in judge_competitions:
            if comp_id == _cid:
                single_competition_display_name = f"{name} ({year})"
                break
    else:
        scope_summary_line = f"Competition scope: {individual_scope_label}"
        report_filter_lines = [scope_summary_line]
        if year_filter is not None:
            report_filter_lines.append(f"Season year: {year_filter}")
        else:
            report_filter_lines.append("Season year: All years")
        if use_event_dates and event_start_date and event_end_date:
            report_filter_lines.append(
                f"Event dates: {event_start_date.isoformat()} – {event_end_date.isoformat()}"
            )
        if selected_competitions:
            report_filter_lines.append(
                "Competitions: " + ", ".join(selected_competitions)
            )
        else:
            report_filter_lines.append(
                "Competitions: All (this judge's events)"
            )
        if selected_disciplines:
            report_filter_lines.append(
                "Discipline types: " + ", ".join(selected_disciplines)
            )
        else:
            report_filter_lines.append("Discipline types: All")

    report_cache_key = (
        selected_judge_display,
        year_filter,
        individual_competition_scope,
        tuple(selected_disciplines) if selected_disciplines else (),
        tuple(sorted(competition_ids)) if competition_ids else (),
        event_start_date.isoformat()
        if use_event_dates and event_start_date
        else None,
        event_end_date.isoformat()
        if use_event_dates and event_end_date
        else None,
        stats.get("pcs_total_scores"),
        stats.get("element_total_scores"),
        len(pcs_df),
        len(element_df),
        len(segment_df),
    )
    if st.session_state.get("individual_judge_report_cache_key") != report_cache_key:
        st.session_state["individual_judge_report_cache_key"] = report_cache_key
        st.session_state["individual_judge_report_bytes"] = build_judge_report_html(
            selected_judge_display,
            stats,
            pcs_df,
            element_df,
            segment_df,
            single_competition_display_name=single_competition_display_name,
            filter_summary_lines=report_filter_lines,
        )
    html_bytes = st.session_state["individual_judge_report_bytes"]
    _dn_comp = ""
    if single_competition_display_name:
        _dn_comp = (
            "_"
            + single_competition_display_name.replace(" ", "_")
            .replace("/", "_")
            .replace("(", "")
            .replace(")", "")
        )
    _streamlit_download(
        "Download Interactive HTML Report",
        data=html_bytes,
        file_name=f"judge_report_{safe_name}{_dn_comp}.html",
        mime="text/html",
        key="individual_judge_report_download",
    )

elif page == "Rule Errors Analysis":
    st.header("Rule Errors Analysis")

    analytics = get_analytics_safe()

    # Filters
    st.subheader("Filters")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        years = analytics.get_years()
        year_filter = st.selectbox("Filter by Year", ["All Years"] + years, key="rule_errors_year")
        year_filter = None if year_filter == "All Years" else year_filter

    with col2:
        qualifying_scope_label = st.selectbox(
            "Competition scope",
            list(_COMPETITION_SCOPE_LABELS),
            index=0,
            key="rule_errors_qualifying",
            help=(
                "Qualifying: linked officials type is set and not id 11. "
                "NQS: linked officials type id 10. "
                "Sectionals & championships: types 1–9. "
                "Championships only: types 4 and 8."
            ),
        )
        rule_errors_competition_scope = _competition_scope_key(qualifying_scope_label)

    with col3:
        competitions = analytics.get_competitions(
            competition_scope=rule_errors_competition_scope
        )
        competition_names = [f"{name} ({year})" for comp_id, name, year in competitions]
        selected_competitions = st.multiselect("Filter by Competitions", competition_names, key="rule_errors_comps")
        competition_ids = [
            comp_id for comp_id, name, year in competitions
            if f"{name} ({year})" in selected_competitions
        ] if selected_competitions else None

    with col4:
        _re_labels, _re_map = _identity_group_options(analytics)
        selected_judge_identities = st.multiselect(
            "Filter by Judges",
            _re_labels,
            key="rule_errors_judges",
        )
        judge_ids = sorted(
            {jid for lab in selected_judge_identities for jid in _re_map[lab]}
        ) if selected_judge_identities else None

    # Get rule errors data
    with st.spinner("Loading rule errors data..."):
        rule_errors_df = analytics.get_all_rule_errors(
            year_filter,
            competition_ids,
            judge_ids,
            competition_scope=rule_errors_competition_scope,
        )

    if rule_errors_df.empty:
        st.warning("No rule errors found with selected filters")
    else:
        # Summary statistics
        st.subheader("Rule Errors Summary")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Rule Errors", len(rule_errors_df))

        with col2:
            pcs_errors = len(rule_errors_df[rule_errors_df['element_name'] == ''])
            st.metric("PCS Rule Errors", pcs_errors)

        with col3:
            element_errors = len(rule_errors_df[rule_errors_df['element_name'] != ''])
            st.metric("Element Rule Errors", element_errors)

        with col4:
            unique_judges = rule_errors_df['judge_name'].nunique()
            st.metric("Judges with Rule Errors", unique_judges)

        # Rule errors by judge
        st.subheader("Rule Errors by Judge")
        judge_summary = rule_errors_df.groupby('judge_name').agg({
            'judge_id': 'first',
            'element_name': 'count'
        }).rename(columns={'element_name': 'total_rule_errors'})

        # Count PCS vs Element errors (using element_type column to distinguish)
        pcs_errors_count = rule_errors_df[rule_errors_df['element_name'] == ''].groupby('judge_name').size().fillna(0)
        element_errors_count = rule_errors_df[rule_errors_df['element_name'] != ''].groupby('judge_name').size().fillna(0)

        judge_summary['pcs_errors'] = pcs_errors_count
        judge_summary['element_errors'] = element_errors_count
        judge_summary = judge_summary.sort_values('total_rule_errors', ascending=False).reset_index()

        judge_summary_display = judge_summary[['judge_name', 'total_rule_errors', 'pcs_errors', 'element_errors']].copy()
        judge_summary_display.columns = ['Judge', 'Total Rule Errors', 'PCS Errors', 'Element Errors']

        st.dataframe(judge_summary_display, width="stretch")

        # Detailed rule errors table
        st.subheader("Detailed Rule Errors")

        # Create display dataframe with proper competition links
        display_df = rule_errors_df[[
            'judge_name', 'competition_name', 'competition_url', 'competition_year',
            'segment_name', 'discipline_name', 'skater_name', 'element_name', 'element_type',
            'element_notes', 'max_goe_allowed',
            'judge_score', 'panel_average', 'deviation'
        ]].copy()

        display_df.columns = [
            'Judge', 'Competition', 'Competition URL', 'Year',
            'Segment', 'Discipline', 'Skater', 'Element Name', 'Element Type',
            'Element Notes', 'Max GOE Allowed',
            'Judge Score', 'Panel Average', 'Deviation'
        ]

        st.dataframe(display_df.drop('Competition URL', axis=1), width="stretch")

        # Show competition links
        if 'Competition URL' in display_df.columns and not display_df['Competition URL'].isna().all():
            st.subheader("Competition Links")
            unique_competitions = display_df[['Competition', 'Competition URL']].drop_duplicates()
            for _, row in unique_competitions.iterrows():
                if pd.notna(row['Competition URL']) and row['Competition URL']:
                    href = _competition_results_index_url(row["Competition URL"])
                    if href:
                        st.markdown(f"[{row['Competition']}]({href})")

elif page == "Competition Analysis":
    st.header("Competition Analysis")

    # Competition selection
    analytics = get_analytics_safe()
    competitions = analytics.get_competitions()
    if not competitions:
        st.error("No competitions found in database")
        st.stop()

    competition_options = {
        f"{name} ({year})": comp_id
        for comp_id, name, year in competitions
    }
    selected_competition = st.selectbox(
        "Select Competition",
        list(competition_options.keys()),
        key="competition_analysis_select",
    )

    if selected_competition:
        competition_id = competition_options[selected_competition]
        st.session_state["competition_analysis_id"] = competition_id

        comp = analytics.session.get(Competition, competition_id)
        if comp is not None:
            st.subheader(comp.name)
            _render_competition_analysis_header(comp)

        st.subheader("Competition officials")
        st.caption(
            "One row per official per discipline. Roles lists distinct appointments/protocol roles "
            "for that segment category only (directory appointment names when linked, else short IJS-style labels)."
        )
        with st.spinner("Loading officials..."):
            officials_df = _cached_competition_segment_officials(competition_id)
        if officials_df.empty:
            st.info(
                "No segment_official rows for this competition. "
                "Import or backfill protocol panel data to populate this list."
            )
        else:
            display_off = (
                officials_df[
                    ["official", "mbr_number", "discipline", "panel_roles"]
                ]
                .rename(
                    columns={
                        "official": "Official",
                        "mbr_number": "Member #",
                        "discipline": "Discipline",
                        "panel_roles": "Roles",
                    }
                )
            )
            st.dataframe(
                display_off,
                width="stretch",
                hide_index=True,
                column_config={
                    "Official": st.column_config.TextColumn(
                        "Official",
                        width="medium",
                    ),
                    "Member #": st.column_config.TextColumn(
                        "Member #",
                        width="small",
                    ),
                    "Discipline": st.column_config.TextColumn(
                        "Discipline",
                        width="small",
                        help="Segment discipline_type (e.g. Singles, Pairs).",
                    ),
                    "Roles": st.column_config.TextColumn(
                        "Roles",
                        width="large",
                        help="Distinct roles this official held in this discipline at this competition.",
                    ),
                },
            )

        # Get segment statistics for all judges in this competition efficiently
        with st.spinner("Loading competition data..."):
            segment_stats = st.cache_data(
                analytics.get_competition_segment_statistics,
                ttl=300  # Cache for 5 minutes
            )(competition_id)

        if segment_stats.empty:
            st.warning("No segment statistics found for this competition")
        else:
            # Calculate allowed errors function
            def calculate_allowed_errors(skater_count):
                if skater_count <= 10:
                    return 1
                elif skater_count <= 20:
                    return 2
                else:
                    return 3

            # Build the judge-segment grid
            st.subheader(f"Judge Performance Grid - {selected_competition}")
            # Calculate allowed and excess anomalies
            segment_stats['allowed_errors'] = segment_stats['skater_count'].apply(calculate_allowed_errors)
            segment_stats['excess_anomalies'] = (segment_stats['total_anomalies'] - segment_stats['allowed_errors']).apply(lambda x: max(0, x))

            # Create the main grid showing total anomalies
            anomalies_grid = segment_stats.pivot_table(
                index=['discipline', 'segment_name'],
                columns='judge_name',
                values='total_anomalies',
                fill_value=0,
                aggfunc='sum'
            )

            # Create grid for excess anomalies
            excess_grid = segment_stats.pivot_table(
                index=['discipline', 'segment_name'],
                columns='judge_name',
                values='excess_anomalies',
                fill_value=0,
                aggfunc='sum'
            )

            # Blank cells for zeros; all-string cells so PyArrow can serialize judge columns
            anomalies_grid_display = _streamlit_safe_judge_pivot_display(anomalies_grid)
            excess_grid_display = _streamlit_safe_judge_pivot_display(excess_grid)

            # Display total anomalies grid
            st.subheader("Total Anomalies by Judge and Segment")
            if not anomalies_grid.empty:
                st.dataframe(anomalies_grid_display, width="stretch")

                # Add judge totals for total anomalies
                st.subheader("Judge Totals - Total Anomalies")
                judge_totals_anomalies = anomalies_grid.sum().sort_values(ascending=False)
                judge_totals_df_anomalies = pd.DataFrame({
                    'Judge': judge_totals_anomalies.index,
                    'Total Anomalies': judge_totals_anomalies.values
                })
                st.dataframe(judge_totals_df_anomalies, width="stretch")
            else:
                st.info("No anomalies data available for grid display")

            # Display excess anomalies grid
            st.subheader("Excess Anomalies by Judge and Segment")
            if not excess_grid.empty:
                st.dataframe(excess_grid_display, width="stretch")

                # Add judge totals for excess anomalies
                st.subheader("Judge Totals - Excess Anomalies")
                judge_totals_excess = excess_grid.sum().sort_values(ascending=False)
                judge_totals_df_excess = pd.DataFrame({
                    'Judge': judge_totals_excess.index,
                    'Total Excess Anomalies': judge_totals_excess.values
                })
                st.dataframe(judge_totals_df_excess, width="stretch")
            else:
                st.info("No excess anomalies data available for grid display")

            # Rule Errors Summary and List
            st.subheader("Rule Errors Analysis")
            with st.spinner("Loading rule errors data..."):
                rule_errors_df = analytics.get_all_rule_errors(
                    competition_ids=[competition_id]
                )

            if not rule_errors_df.empty:
                # Rule errors summary
                col1, col2, col3 = st.columns(3)

                with col1:
                    total_rule_errors = len(rule_errors_df)
                    st.metric("Total Rule Errors", total_rule_errors)

                with col2:
                    pcs_rule_errors = len(rule_errors_df[rule_errors_df['element_name'] == ''])
                    st.metric("PCS Rule Errors", pcs_rule_errors)

                with col3:
                    element_rule_errors = len(rule_errors_df[rule_errors_df['element_name'] != ''])
                    st.metric("Element Rule Errors", element_rule_errors)

                # Judge breakdown
                rule_error_judge_summary = rule_errors_df.groupby('judge_name').size().sort_values(ascending=False)
                st.subheader("Rule Errors by Judge")
                judge_rule_error_df = pd.DataFrame({
                    'Judge': rule_error_judge_summary.index,
                    'Rule Errors': rule_error_judge_summary.values
                })
                st.dataframe(judge_rule_error_df, width="stretch")

                # Detailed rule errors table
                st.subheader("Detailed Rule Errors")
                display_rule_errors = rule_errors_df[[
                    'judge_name', 'segment_name', 'discipline_name', 'skater_name',
                    'element_name', 'element_type', 'element_notes', 'max_goe_allowed',
                    'judge_score', 'panel_average', 'deviation'
                ]].copy()

                # Add a score type column based on whether element_name is empty
                display_rule_errors['score_type'] = display_rule_errors['element_name'].apply(
                    lambda x: 'PCS' if x == '' else 'Element'
                )

                # Fill NaN values for better display
                display_rule_errors['element_name'] = display_rule_errors['element_name'].fillna('N/A')
                display_rule_errors['element_type'] = display_rule_errors['element_type'].fillna('N/A')

                # Reorder columns to include score_type
                display_rule_errors = display_rule_errors[[
                    'judge_name', 'segment_name', 'discipline_name', 'skater_name',
                    'score_type', 'element_name', 'element_type',
                    'element_notes', 'max_goe_allowed',
                    'judge_score', 'panel_average', 'deviation'
                ]]

                display_rule_errors.columns = [
                    'Judge', 'Segment', 'Discipline', 'Skater', 'Score Type',
                    'Element Name', 'Element Type', 'Element Notes', 'Max GOE Allowed',
                    'Judge Score', 'Panel Average', 'Deviation'
                ]

                st.dataframe(display_rule_errors, width="stretch")
            else:
                st.info("No rule errors found for this competition")

            # Anomalies Analysis
            st.subheader("Anomalies Analysis")

            # Get all judges for this competition from segment stats
            competition_judges = [int(judge_id) for judge_id in segment_stats['judge_id'].unique()]

            # Collect all anomalies data for this competition
            with st.spinner("Loading anomalies data..."):
                all_pcs_anomalies = []
                all_element_anomalies = []

                # Get judge names mapping
                judges = analytics.get_judges()
                judge_names = {judge_id: judge_name for judge_id, judge_name, _ in judges}

                for judge_id in competition_judges:
                    # Get PCS anomalies for this judge and competition
                    judge_pcs = analytics.get_judge_pcs_stats(
                        judge_id, competition_ids=[competition_id]
                    )
                    if not judge_pcs.empty:
                        pcs_anomalies = judge_pcs[judge_pcs['anomaly'] == True]
                        if not pcs_anomalies.empty:
                            pcs_anomalies = pcs_anomalies.copy()
                            pcs_anomalies['judge_id'] = judge_id
                            pcs_anomalies['judge_name'] = judge_names.get(judge_id, 'Unknown')
                            all_pcs_anomalies.append(pcs_anomalies)

                    # Get element anomalies for this judge and competition
                    judge_elements = analytics.get_judge_element_stats(
                        judge_id, competition_ids=[competition_id]
                    )
                    if not judge_elements.empty:
                        element_anomalies = judge_elements[judge_elements['anomaly'] == True]
                        if not element_anomalies.empty:
                            element_anomalies = element_anomalies.copy()
                            element_anomalies['judge_id'] = judge_id
                            element_anomalies['judge_name'] = judge_names.get(judge_id, 'Unknown')
                            all_element_anomalies.append(element_anomalies)

                # Combine all anomalies
                pcs_anomalies_df = pd.concat(all_pcs_anomalies, ignore_index=True) if all_pcs_anomalies else pd.DataFrame()
                element_anomalies_df = pd.concat(all_element_anomalies, ignore_index=True) if all_element_anomalies else pd.DataFrame()

            if not pcs_anomalies_df.empty or not element_anomalies_df.empty:

                # Display PCS anomalies
                if not pcs_anomalies_df.empty:
                    st.subheader("PCS Anomalies")

                    # Filter PCS data based on issue types
                    filtered_pcs = pcs_anomalies_df.copy()

                    if not filtered_pcs.empty:
                        def get_issue_type_pcs(row):
                            issues = []
                            if row['anomaly'] and abs(row['deviation']) >= 1.5:
                                direction = 'High' if row['deviation'] > 0 else 'Low'
                                issues.append(f'Anomaly ({direction})')
                            if row['is_rule_error']:
                                issues.append('Rule Error')
                            return ', '.join(issues)

                        filtered_pcs['issue_type'] = filtered_pcs.apply(get_issue_type_pcs, axis=1)

                        display_pcs_anomalies = filtered_pcs[[
                            'judge_name', 'segment_name', 'discipline_name', 'skater_name',
                            'pcs_type_name', 'judge_score', 'panel_average', 'deviation', 'issue_type'
                        ]].copy()

                        display_pcs_anomalies.columns = [
                            'Judge', 'Segment', 'Discipline', 'Skater', 'PCS Component',
                            'Judge Score', 'Panel Average', 'Deviation', 'Issue Type'
                        ]

                        st.dataframe(display_pcs_anomalies, width="stretch")
                    else:
                        st.info("No PCS anomalies match the selected filters")

                # Display element anomalies
                if not element_anomalies_df.empty:
                    st.subheader("Element Anomalies")

                    # Filter element data based on issue types
                    filtered_elements = element_anomalies_df.copy()

                    if not filtered_elements.empty:
                        def get_issue_type_elem(row):
                            issues = []
                            if row['anomaly'] and abs(row['deviation']) >= 2.0:
                                direction = 'High' if row['deviation'] > 0 else 'Low'
                                issues.append(f'Anomaly ({direction})')
                            if row['is_rule_error']:
                                issues.append('Rule Error')
                            return ', '.join(issues)

                        filtered_elements['issue_type'] = filtered_elements.apply(get_issue_type_elem, axis=1)

                        display_element_anomalies = filtered_elements[[
                            'judge_name', 'segment_name', 'discipline_name', 'skater_name',
                            'element_name', 'element_type_name', 'judge_score', 'panel_average', 'deviation', 'issue_type'
                        ]].copy()

                        display_element_anomalies.columns = [
                            'Judge', 'Segment', 'Discipline', 'Skater', 'Element Name',
                            'Element Type', 'Judge Score', 'Panel Average', 'Deviation', 'Issue Type'
                        ]

                        st.dataframe(display_element_anomalies, width="stretch")
                    else:
                        st.info("No element anomalies match the selected filters")
            else:
                st.info("No anomalies found for this competition")

        # Judge Performance Summary Table
        st.markdown("---")
        st.subheader("Judge Performance Summary")
        st.markdown("Throwout and anomaly rates for all judges at this competition")
        
        with st.spinner("Calculating judge performance metrics..."):
            # Get all segments for this competition
            segments = analytics.session.query(
                Segment.id, Segment.name, DisciplineType.name.label('discipline')
            ).join(
                DisciplineType, Segment.discipline_type_id == DisciplineType.id
            ).filter(
                Segment.competition_id == competition_id
            ).all()
            
            segment_ids = [s.id for s in segments]
            segment_info = {s.id: {'name': s.name, 'discipline': s.discipline} for s in segments}
            
            if segment_ids:
                # Calculate PCS stats per judge with high/low breakdown
                from sqlalchemy import and_
                pcs_stats = analytics.session.query(
                    PcsScorePerJudge.judge_id,
                    Judge.name.label('judge_name'),
                    func.count().label('total_pcs'),
                    func.sum(case((PcsScorePerJudge.thrown_out, 1), else_=0)).label('pcs_throwouts'),
                    func.sum(case((and_(PcsScorePerJudge.thrown_out, PcsScorePerJudge.deviation > 0), 1), else_=0)).label('pcs_throwouts_high'),
                    func.sum(case((and_(PcsScorePerJudge.thrown_out, PcsScorePerJudge.deviation < 0), 1), else_=0)).label('pcs_throwouts_low'),
                    func.sum(case((func.abs(PcsScorePerJudge.deviation) >= 1.5, 1), else_=0)).label('pcs_anomalies'),
                    func.sum(case((PcsScorePerJudge.deviation >= 1.5, 1), else_=0)).label('pcs_anomalies_high'),
                    func.sum(case((PcsScorePerJudge.deviation <= -1.5, 1), else_=0)).label('pcs_anomalies_low'),
                    func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label('pcs_rule_errors')
                ).join(
                    Judge, PcsScorePerJudge.judge_id == Judge.id
                ).join(
                    SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
                ).filter(
                    SkaterSegment.segment_id.in_(segment_ids)
                ).group_by(
                    PcsScorePerJudge.judge_id, Judge.name
                ).all()
                
                # Calculate element stats per judge with high/low breakdown
                element_stats = analytics.session.query(
                    ElementScorePerJudge.judge_id,
                    Judge.name.label('judge_name'),
                    func.count().label('total_elements'),
                    func.sum(case((ElementScorePerJudge.thrown_out, 1), else_=0)).label('element_throwouts'),
                    func.sum(case((and_(ElementScorePerJudge.thrown_out, ElementScorePerJudge.deviation > 0), 1), else_=0)).label('element_throwouts_high'),
                    func.sum(case((and_(ElementScorePerJudge.thrown_out, ElementScorePerJudge.deviation < 0), 1), else_=0)).label('element_throwouts_low'),
                    func.sum(case((func.abs(ElementScorePerJudge.deviation) >= 2.0, 1), else_=0)).label('element_anomalies'),
                    func.sum(case((ElementScorePerJudge.deviation >= 2.0, 1), else_=0)).label('element_anomalies_high'),
                    func.sum(case((ElementScorePerJudge.deviation <= -2.0, 1), else_=0)).label('element_anomalies_low'),
                    func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label('element_rule_errors')
                ).join(
                    Judge, ElementScorePerJudge.judge_id == Judge.id
                ).join(
                    Element, ElementScorePerJudge.element_id == Element.id
                ).join(
                    SkaterSegment, Element.skater_segment_id == SkaterSegment.id
                ).filter(
                    SkaterSegment.segment_id.in_(segment_ids)
                ).group_by(
                    ElementScorePerJudge.judge_id, Judge.name
                ).all()
                
                # Combine into summary
                pcs_dict = {s.judge_id: s for s in pcs_stats}
                elem_dict = {s.judge_id: s for s in element_stats}
                all_judge_ids = set(pcs_dict.keys()) | set(elem_dict.keys())
                
                summary_rows = []
                for judge_id in all_judge_ids:
                    pcs = pcs_dict.get(judge_id)
                    elem = elem_dict.get(judge_id)
                    
                    judge_name = pcs.judge_name if pcs else elem.judge_name
                    
                    total_pcs = pcs.total_pcs if pcs else 0
                    pcs_throwouts = pcs.pcs_throwouts if pcs else 0
                    pcs_throwouts_high = pcs.pcs_throwouts_high if pcs else 0
                    pcs_throwouts_low = pcs.pcs_throwouts_low if pcs else 0
                    pcs_anomalies = pcs.pcs_anomalies if pcs else 0
                    pcs_anomalies_high = pcs.pcs_anomalies_high if pcs else 0
                    pcs_anomalies_low = pcs.pcs_anomalies_low if pcs else 0
                    
                    total_elements = elem.total_elements if elem else 0
                    elem_throwouts = elem.element_throwouts if elem else 0
                    elem_throwouts_high = elem.element_throwouts_high if elem else 0
                    elem_throwouts_low = elem.element_throwouts_low if elem else 0
                    elem_anomalies = elem.element_anomalies if elem else 0
                    elem_anomalies_high = elem.element_anomalies_high if elem else 0
                    elem_anomalies_low = elem.element_anomalies_low if elem else 0
                    
                    summary_rows.append({
                        'Judge': judge_name,
                        'PCS Scores': total_pcs,
                        'PCS Throw %': round(pcs_throwouts / total_pcs * 100, 2) if total_pcs > 0 else 0,
                        'PCS Throw High %': round(pcs_throwouts_high / total_pcs * 100, 2) if total_pcs > 0 else 0,
                        'PCS Throw Low %': round(pcs_throwouts_low / total_pcs * 100, 2) if total_pcs > 0 else 0,
                        'PCS Anom %': round(pcs_anomalies / total_pcs * 100, 2) if total_pcs > 0 else 0,
                        'PCS Anom High %': round(pcs_anomalies_high / total_pcs * 100, 2) if total_pcs > 0 else 0,
                        'PCS Anom Low %': round(pcs_anomalies_low / total_pcs * 100, 2) if total_pcs > 0 else 0,
                        'Elem Scores': total_elements,
                        'Elem Throw %': round(elem_throwouts / total_elements * 100, 2) if total_elements > 0 else 0,
                        'Elem Throw High %': round(elem_throwouts_high / total_elements * 100, 2) if total_elements > 0 else 0,
                        'Elem Throw Low %': round(elem_throwouts_low / total_elements * 100, 2) if total_elements > 0 else 0,
                        'Elem Anom %': round(elem_anomalies / total_elements * 100, 2) if total_elements > 0 else 0,
                        'Elem Anom High %': round(elem_anomalies_high / total_elements * 100, 2) if total_elements > 0 else 0,
                        'Elem Anom Low %': round(elem_anomalies_low / total_elements * 100, 2) if total_elements > 0 else 0
                    })
                
                if summary_rows:
                    summary_df = pd.DataFrame(summary_rows)
                    summary_df = summary_df.sort_values('PCS Throw %', ascending=False)
                    st.dataframe(summary_df, width="stretch")
                    
                    # Per-segment breakdown toggle
                    if st.checkbox("Show per-segment breakdown"):
                        st.subheader("Judge Performance by Segment")
                        
                        for seg_id, seg_info in segment_info.items():
                            with st.expander(f"{seg_info['discipline']} - {seg_info['name']}"):
                                # PCS stats for this segment with high/low
                                from sqlalchemy import and_
                                seg_pcs = analytics.session.query(
                                    Judge.name.label('judge_name'),
                                    func.count().label('total'),
                                    func.sum(case((and_(PcsScorePerJudge.thrown_out, PcsScorePerJudge.deviation > 0), 1), else_=0)).label('throw_high'),
                                    func.sum(case((and_(PcsScorePerJudge.thrown_out, PcsScorePerJudge.deviation < 0), 1), else_=0)).label('throw_low'),
                                    func.sum(case((PcsScorePerJudge.deviation >= 1.5, 1), else_=0)).label('anom_high'),
                                    func.sum(case((PcsScorePerJudge.deviation <= -1.5, 1), else_=0)).label('anom_low')
                                ).join(
                                    Judge, PcsScorePerJudge.judge_id == Judge.id
                                ).join(
                                    SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
                                ).filter(
                                    SkaterSegment.segment_id == seg_id
                                ).group_by(Judge.name).all()
                                
                                # Element stats for this segment with high/low
                                seg_elem = analytics.session.query(
                                    Judge.name.label('judge_name'),
                                    func.count().label('total'),
                                    func.sum(case((and_(ElementScorePerJudge.thrown_out, ElementScorePerJudge.deviation > 0), 1), else_=0)).label('throw_high'),
                                    func.sum(case((and_(ElementScorePerJudge.thrown_out, ElementScorePerJudge.deviation < 0), 1), else_=0)).label('throw_low'),
                                    func.sum(case((ElementScorePerJudge.deviation >= 2.0, 1), else_=0)).label('anom_high'),
                                    func.sum(case((ElementScorePerJudge.deviation <= -2.0, 1), else_=0)).label('anom_low')
                                ).join(
                                    Judge, ElementScorePerJudge.judge_id == Judge.id
                                ).join(
                                    Element, ElementScorePerJudge.element_id == Element.id
                                ).join(
                                    SkaterSegment, Element.skater_segment_id == SkaterSegment.id
                                ).filter(
                                    SkaterSegment.segment_id == seg_id
                                ).group_by(Judge.name).all()
                                
                                pcs_dict_seg = {r.judge_name: r for r in seg_pcs}
                                elem_dict_seg = {r.judge_name: r for r in seg_elem}
                                all_judges_seg = set(pcs_dict_seg.keys()) | set(elem_dict_seg.keys())
                                
                                seg_rows = []
                                for jn in all_judges_seg:
                                    p = pcs_dict_seg.get(jn)
                                    e = elem_dict_seg.get(jn)
                                    
                                    pcs_total = p.total if p else 0
                                    pcs_throw_h = p.throw_high if p else 0
                                    pcs_throw_l = p.throw_low if p else 0
                                    pcs_anom_h = p.anom_high if p else 0
                                    pcs_anom_l = p.anom_low if p else 0
                                    elem_total = e.total if e else 0
                                    elem_throw_h = e.throw_high if e else 0
                                    elem_throw_l = e.throw_low if e else 0
                                    elem_anom_h = e.anom_high if e else 0
                                    elem_anom_l = e.anom_low if e else 0
                                    
                                    pcs_throw_total = pcs_throw_h + pcs_throw_l
                                    pcs_anom_total = pcs_anom_h + pcs_anom_l
                                    elem_throw_total = elem_throw_h + elem_throw_l
                                    elem_anom_total = elem_anom_h + elem_anom_l
                                    
                                    seg_rows.append({
                                        'Judge': jn,
                                        'PCS Scores': pcs_total,
                                        'PCS Throw %': round(pcs_throw_total / pcs_total * 100, 2) if pcs_total > 0 else 0,
                                        'PCS Throw High %': round(pcs_throw_h / pcs_total * 100, 2) if pcs_total > 0 else 0,
                                        'PCS Throw Low %': round(pcs_throw_l / pcs_total * 100, 2) if pcs_total > 0 else 0,
                                        'PCS Anom %': round(pcs_anom_total / pcs_total * 100, 2) if pcs_total > 0 else 0,
                                        'PCS Anom High %': round(pcs_anom_h / pcs_total * 100, 2) if pcs_total > 0 else 0,
                                        'PCS Anom Low %': round(pcs_anom_l / pcs_total * 100, 2) if pcs_total > 0 else 0,
                                        'Elem Scores': elem_total,
                                        'Elem Throw %': round(elem_throw_total / elem_total * 100, 2) if elem_total > 0 else 0,
                                        'Elem Throw High %': round(elem_throw_h / elem_total * 100, 2) if elem_total > 0 else 0,
                                        'Elem Throw Low %': round(elem_throw_l / elem_total * 100, 2) if elem_total > 0 else 0,
                                        'Elem Anom %': round(elem_anom_total / elem_total * 100, 2) if elem_total > 0 else 0,
                                        'Elem Anom High %': round(elem_anom_h / elem_total * 100, 2) if elem_total > 0 else 0,
                                        'Elem Anom Low %': round(elem_anom_l / elem_total * 100, 2) if elem_total > 0 else 0
                                    })
                                
                                if seg_rows:
                                    seg_df = pd.DataFrame(seg_rows)
                                    st.dataframe(seg_df, width="stretch")
                else:
                    st.info("No judge performance data available for this competition")
            else:
                st.info("No segments found for this competition")

elif page == "Cross-Judge Benchmarking":
    cross_judge_benchmarking_page()

elif page == "PCS Quality Analysis":
    pcs_quality_analysis_page()

elif page == "PCS Deviation Analysis":
    pcs_deviation_analysis_page()

elif page == "Element Deviation Ranking Analysis":
    element_deviation_ranking_page()

elif page == "Temporal Trend Analysis":
    temporal_trend_analysis()

elif page == "Panel size benchmarks":
    st.header("Panel size benchmarks")
    st.caption(
        "Rates by **discipline** (segment ``discipline_type``) and **panel size** "
        "(number of judges on that element or PCS line). Competitions are filtered by **linked "
        "officials competition type** (same scopes as Cross-Judge benchmarks). **Anomalies**: PCS uses "
        "|deviation| ≥ 1.5 or rule error; elements use |deviation| ≥ 2.0 or rule error "
        "(same thresholds as judge reports)."
    )
    analytics_bm = get_analytics_safe()
    scope_label = st.selectbox(
        "Competition scope",
        options=list(_COMPETITION_SCOPE_LABELS),
        index=1,
        key="panel_benchmarks_scope",
        help=(
            "Same as Cross-Judge benchmarks: filters "
            "`public.competition` via linked `officials_analysis_competition_type_id`."
        ),
    )
    scope_key = _competition_scope_key(scope_label)

    metric_labels = st.multiselect(
        "Metrics",
        options=["Throwouts", "Anomalies"],
        default=["Throwouts", "Anomalies"],
        help="Throwouts = stored IJS trim; Anomalies = deviation/rule-error thresholds above.",
    )
    label_to_key = {"Throwouts": "throwout", "Anomalies": "anomaly"}
    metric_keys = tuple(label_to_key[k] for k in metric_labels if k in label_to_key)
    if not metric_keys:
        st.info("Select at least one metric.")
        st.stop()

    years_all = analytics_bm.get_years()
    year_pick = st.multiselect(
        "Season years (blank = all)",
        options=years_all,
        default=[],
        help="Competition ``year`` field (e.g. 2526).",
    )
    c_low, c_high = st.columns(2)
    with c_low:
        min_panel = st.number_input("Min panel size", min_value=2, max_value=15, value=3, step=1)
    with c_high:
        max_panel = st.number_input("Max panel size", min_value=2, max_value=15, value=9, step=1)
    if min_panel > max_panel:
        st.error("Min panel size cannot exceed max.")
        st.stop()
    if st.button("Run report", type="primary"):
        with st.spinner("Querying scores…"):
            df_bm = analytics_bm.get_panel_score_benchmarks(
                metrics=metric_keys,
                competition_filter_mode="officials_scope",
                competition_scope=scope_key,
                year_filters=year_pick if year_pick else None,
                min_panel_size=int(min_panel),
                max_panel_size=int(max_panel),
            )
        if df_bm.empty:
            st.info(
                "No rows returned. Check competition scope (linked officials types), "
                "segment disciplines, year filter, and panel size range."
            )
        else:
            st.subheader("Summary table")
            st.dataframe(df_bm, width="stretch", hide_index=True)
            _streamlit_download(
                "Download CSV",
                data=df_bm.to_csv(index=False).encode("utf-8"),
                file_name="panel_size_benchmarks.csv",
                mime="text/csv",
                key="panel_benchmarks_download",
            )
            st.subheader("Charts")
            st.caption(
                "Smaller panels often raise **throwout** rates mechanically (more min/max exposure). "
                "**Anomaly** rates use fixed deviation thresholds and are less driven by panel size."
            )
            bench_titles = {
                "throwout": "Throwout rate (%)",
                "anomaly": "Anomaly rate (%)",
            }
            for bmk in df_bm["benchmark"].unique():
                st.markdown(f"#### {bench_titles.get(str(bmk), str(bmk))}")
                for stype in ("Element", "PCS"):
                    sub = df_bm[
                        (df_bm["benchmark"] == bmk) & (df_bm["score_type"] == stype)
                    ]
                    if sub.empty:
                        continue
                    fig = px.bar(
                        sub,
                        x="panel_size",
                        y="rate_pct",
                        color="discipline",
                        barmode="group",
                        title=f"{stype} · {bench_titles.get(str(bmk), bmk)} by panel size",
                        labels={
                            "panel_size": "Panel size (judges)",
                            "rate_pct": bench_titles.get(str(bmk), "Rate (%)"),
                            "discipline": "Discipline",
                        },
                    )
                    st.plotly_chart(fig, width="stretch")

elif page == "Load Competition":
    st.header("Load Competition")
    st.write(
        "Enter the competition details below and click **Run** to scrape and import "
        "the results directly into the database. This calls `scrape()` from "
        "`downloadResults.py` with `write_to_database=True`."
    )

    # Check that downloadResults is importable
    import importlib.util as _ilu, sys as _sys
    _spec = _ilu.find_spec("downloadResults")
    if _spec is None:
        st.error(
            "`downloadResults` module was not found on ``sys.path`` (expected next to this app: "
            f"``{_DOWNLOAD_RESULTS_PY!s}``)."
        )
    else:
        # ── Basic fields ────────────────────────────────────────────────────────
        base_url = st.text_input(
            "Competition URL",
            placeholder="https://ijs.usfigureskating.org/leaderboard/results/2026/34238/index.asp",
        )
        report_name = st.text_input(
            "Competition Name",
            placeholder="2026 US Synchronized Skating Championships",
        )
        year = st.text_input(
            "Season year code",
            value="2526",
            help="e.g. 2526 for the 2025-26 season, 2425 for 2024-25",
        )
        _load_analytics = get_analytics_safe()
        _oa_types = _load_analytics.get_officials_analysis_competition_types()
        if not _oa_types:
            st.error(
                "No **officials_analysis.competition_type** rows found. Import activity or directory "
                "data first, then load competitions."
            )
        else:
            _oa_ids_sorted = sorted(tid for tid, _ in _oa_types)
            _default_oa_index = (
                _oa_ids_sorted.index(OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING)
                if OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING in _oa_ids_sorted
                else 0
            )

            def _fmt_oa_competition_type(opt_id: int) -> str:
                tname = next((n for tid, n in _oa_types if tid == opt_id), "")
                return format_officials_competition_type_select_label(opt_id, tname or None)

            load_oa_competition_type_id = st.selectbox(
                "Officials competition type",
                options=_oa_ids_sorted,
                index=_default_oa_index,
                format_func=_fmt_oa_competition_type,
                help=(
                    "Required. Links to ``officials_analysis.competition_type`` (SPD vs SYS sectionals "
                    "are grouped in the label). Qualifying/NQS flags are set from this id."
                ),
                key="load_competition_officials_analysis_ct",
            )
            load_oa_competition_type_id = int(load_oa_competition_type_id)
            load_qualifying, load_nqs, load_international = (
                competition_load_flags_from_officials_type_id(
                    load_oa_competition_type_id
                )
            )
            st.caption(
                f"Stored flags: **qualifying**={load_qualifying}, **nqs**={load_nqs}, "
                f"**international**={load_international}. "
                "International types (**15–17**) → international true, qualifying and nqs false. "
                "Nonqualifying (id **11**) → qualifying false. NQS (id **10**) → nqs true."
            )
            # pdf_folder = st.text_input(
            #     "PDF output folder (leave blank if not saving PDFs)",
            #     value="",
            #     placeholder="/path/to/pdfs",
            # )

            # ── Advanced options ─────────────────────────────────────────────────────
            with st.expander("Advanced options"):
                load_ev_levels = st.multiselect(
                    "Quick filter: event level (in segment/event name)",
                    list(LEVEL_CHOICES),
                    default=[],
                    help=(
                        "Case-insensitive on the segment label. **Senior** also matches *Championship* "
                        "(e.g. ``CHAMPIONSHIP_ICE_DANCE_…``). Leave empty and leave custom regex blank for all events."
                    ),
                )
                load_ev_disciplines = st.multiselect(
                    "Quick filter: discipline (in segment/event name)",
                    list(DISCIPLINE_CHOICES),
                    default=[],
                    help=(
                        "Singles uses Women/Men/Ladies-style tokens; Pairs; Dance (Ice Dance, etc.). "
                        "Case-insensitive. Combined with level presets, an event must match both groups."
                    ),
                )
                event_regex_custom = st.text_input(
                    "Custom event regex (optional)",
                    value="",
                    help=(
                        "If set, **replaces** the quick filters above. Only events matching this regex "
                        "are processed (``re.match`` on the parsed label). Prefix with ``(?i)`` for "
                        "case-insensitivity. Leave blank to use quick filters only."
                    ),
                )
                judge_filter = st.text_area(
                    "Judge filter (optional)",
                    value="",
                    height=80,
                    placeholder="Exact panel names — comma, semicolon, or newline separated.",
                    help=(
                        "Limits deviation / error reporting to these judges only (exact match to names "
                        "on the protocol). Separate multiple names with commas, semicolons, or new lines. "
                        "Leave blank for all judges."
                    ),
                )
                specific_exclude = st.text_input(
                    "Specific exclude",
                    value="",
                    help="Exclude specific events matching this string.",
                )
                only_rule_errors = st.checkbox("Only rule errors", value=False)
                # add_additional_analysis = st.checkbox("Add additional analysis", value=False)
                # use_html = st.checkbox("Use HTML mode", value=True)
                # isFSM = st.checkbox("Is FSM competition", value=False)

            st.caption(
                f"Scrape scratch files (FSM judge-detail PDFs): **{scrape_storage_summary()}**. "
                "Set ``USE_GCP=1`` on Heroku to use GCS. Prefer ``GCS_SERVICE_ACCOUNT_JSON`` "
                "(full service-account JSON); or ``GCS_CONNECTION`` + ``GCS_PRIVATE_KEY`` "
                "(PEM may use ``\\n`` for newlines)."
            )

            st.markdown("---")

            if st.button("Run", type="primary"):
                if not base_url.strip():
                    st.error("Please enter a Competition URL.")
                elif not report_name.strip():
                    st.error("Please enter a Report name.")
                else:
                    import importlib as _il
                    _mod = _il.import_module("downloadResults")
                    scrape_fn = getattr(_mod, "scrape")
                    from ijs_results_urls import (
                        is_fsm_results_url,
                        results_url_for_storage,
                    )

                    url = results_url_for_storage(base_url)
                    isFSM = is_fsm_results_url(url)

                    event_regex = effective_event_regex(
                        event_regex_custom, load_ev_levels, load_ev_disciplines
                    )

                    kwargs = dict(
                        base_url=url,
                        report_name=report_name.strip(),
                        event_regex=event_regex,
                        only_rule_errors=only_rule_errors,
                        write_excel=False,
                        write_to_database=True,
                        year=year.strip(),
                        judge_filter=judge_filter.strip(),
                        specific_exclude=specific_exclude.strip(),
                        use_html=True,
                        isFSM=isFSM,
                        qualifying=load_qualifying,
                        nqs=load_nqs,
                        international=load_international,
                        officials_analysis_competition_type_id=load_oa_competition_type_id,
                        update_officials_competition_type=True,
                        rebuild_analytics_caches=False,
                        **scrape_storage_kwargs_for_load(report_name.strip()),
                    )

                    status_area = st.empty()
                    status_area.info("Running scrape — this may take a minute or two…")
                    # Drop the UI session so scrape uses its own connection and cannot
                    # leave the cached analytics session in a bad transaction state.
                    release_analytics_db_connection()
                    try:
                        scrape_fn(**kwargs)
                        status_area.success(
                            f"Done! **{report_name.strip()}** has been imported into the database. "
                            "Analytics caches were not rebuilt during import; run "
                            "`scripts/precompute_cross_judge_cache.py`, "
                            "`scripts/precompute_element_ranking_cache.py`, and "
                            "`scripts/precompute_pcs_quality_cache.py` as needed."
                        )
                        st.cache_data.clear()
                    except Exception as _exc:
                        status_area.error(f"Scrape failed: {_exc}")
                        with st.expander("Error details (traceback)", expanded=False):
                            st.code(traceback.format_exc(), language="python")
                    finally:
                        release_analytics_db_connection()

else:
    st.error(f"Unknown analysis page: {page!r}. Choose an option from the sidebar.")

render_query_help([
    "**Navigation:** `?page=` — `individual`, `cross-judge`, `pcs-quality`, "
    "`element-deviation-ranking`, `temporal`, `panel-size-benchmarks`, "
    "`rule-errors`, `competition`, `load-competition`",
    "",
    "**Scope (most pages):** `?competition_scope=` — `all`, `qualifying`, `nqs`, "
    "`sectionals`, `championships`",
    "",
    "**Individual judge:** `?judge=` (identity label), `?year=` (season; omit for all years), "
    "`?disciplines=` (comma-separated names), `?start_date=` / `?end_date=` "
    "(ISO `YYYY-MM-DD`, with event-date filter enabled)",
    "",
    "**Cross-judge:** `?view=`, `?metric=`, `?score_type=`, `?year=`, `?disciplines=`, "
    "`?start_date=` / `?end_date=` (ISO dates, with event-date filter enabled), "
    "`?us_officials_only=1`",
    "",
    "**Element deviation ranking:** `?competition_scope=`, `?start_season=`, "
    "`?end_season=`, `?disciplines=`, `?start_date=` / `?end_date=`, `?min_marks=`, "
    "`?segment_levels=` (`all`, `novice-junior-senior`, `junior-senior`), "
    "`?floor_sigma=`, `?min_bin_count=`, "
    "`?bench_start_season=` / `?bench_end_season=` (`any` or season code), "
    "`?bench_competition_scope=`, `?bench_segment_levels=` (same slugs as "
    "`segment_levels`), `?us_officials_only=1`",
    "",
    "**PCS quality:** `?competition_scope=`, `?start_season=`, `?end_season=`, "
    "`?disciplines=` (required; comma-separated names, e.g. `Singles`), "
    "`?start_date=` / `?end_date=`, `?min_pcs_marks=`, `?us_officials_only=1`",
    "",
    "**Temporal:** `?analysis_type=`, `?metric=`, `?score_type=`, `?judge=`",
    "",
    "**Rule errors:** `?year=`, `?competitions=` (comma-separated labels), "
    "`?judges=` (comma-separated identity labels)",
    "",
    "**Competition:** `?competition_id=` (numeric id)",
])

sync_analysis_app_query_params(page)
mark_query_params_applied()

# Sidebar information
st.sidebar.markdown("---")
st.sidebar.subheader("About")
st.sidebar.markdown("""
This dashboard analyzes figure skating judge performance by examining:
- **Throwout rates**: How often judges' scores are excluded from final calculations
- **Deviation rates**: How often judges score significantly differently from the panel average
- **PCS thresholds**: Deviations >=1.5 points
- **Element thresholds**: Deviations >=2.0 points

Use the filters to focus your analysis on specific years, competitions, or discipline types.
""")

_admin_py = _os.path.join(_REPO_ROOT, "pages", "admin.py")
if _os.path.isfile(_admin_py):
    st.sidebar.markdown("---")
    if st.sidebar.button(
        "Admin",
        help="Open password-protected admin tools (directory import, merge judges, …)",
        type="secondary",
        width="stretch",
        key="sidebar_open_admin",
    ):
        st.switch_page("pages/admin.py")
