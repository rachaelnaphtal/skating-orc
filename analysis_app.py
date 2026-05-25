import streamlit as st
import pandas as pd
import traceback
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from scipy import stats

from analytics_connection import get_analytics, get_analytics_safe
from analytics import JudgeAnalytics
from models import Segment, DisciplineType, Judge, PcsScorePerJudge, ElementScorePerJudge, Element, SkaterSegment
from sqlalchemy import text, func, case
from report_html import build_judge_report_html
from officials_competition_types import (
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
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
from app_query_params import (
    apply_analysis_filters_for_page,
    init_analysis_app_from_query,
    mark_query_params_applied,
    query_params_changed,
    render_query_help,
    sync_analysis_app_query_params,
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
)

_COMPETITION_SCOPE_LABEL_TO_KEY = {
    "All competitions": COMPETITION_SCOPE_ALL,
    "Qualifying only": COMPETITION_SCOPE_QUALIFYING,
    "NQS only": COMPETITION_SCOPE_NQS,
    "Sectionals & championships": COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    "Championships only": COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
}


def _competition_scope_key(scope_label: str) -> str:
    """Map sidebar label to analytics ``competition_scope`` string."""
    return _COMPETITION_SCOPE_LABEL_TO_KEY.get(scope_label, COMPETITION_SCOPE_ALL)


@st.cache_data(ttl=300)
def _cached_pooled_cross_judge_metrics(
    score_type: str,
    year_filter,
    competition_ids_tuple,
    discipline_ids_tuple,
    competition_scope: str,
    event_start_iso: str | None,
    event_end_iso: str | None,
):
    from datetime import date as _date

    analytics = get_analytics_safe()
    event_start = _date.fromisoformat(event_start_iso) if event_start_iso else None
    event_end = _date.fromisoformat(event_end_iso) if event_end_iso else None
    return analytics.get_pooled_cross_judge_metrics(
        score_type=score_type,
        year_filter=year_filter,
        competition_ids=list(competition_ids_tuple) if competition_ids_tuple else None,
        discipline_type_ids=list(discipline_ids_tuple) if discipline_ids_tuple else None,
        competition_scope=competition_scope,
        event_start_date=event_start,
        event_end_date=event_end,
    )


@st.cache_data(ttl=300)
def _cached_competition_segment_officials(competition_id: int):
    return get_analytics_safe().get_competition_segment_officials_display(competition_id)


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


# Main title
st.title("⛸️ Figure Skating Judge Performance Analytics")

# Navigation (paths relative to this file so cwd does not hide "Load Competition")
import os as _os

_REPO_ROOT = _os.path.dirname(_os.path.abspath(__file__))
_DOWNLOAD_RESULTS_PY = _os.path.join(_REPO_ROOT, "downloadResults.py")
_nav_pages = [
    "Individual Judge Analysis",
    "Cross-Judge Benchmarking",     "Temporal Trend Analysis",
    "Panel size benchmarks",
    "Rule Errors Analysis", "Competition Analysis",
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

apply_analysis_filters_for_page(
    page, get_analytics_safe(), from_url=_url_params_changed
)


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
            (
                "Total excess anomalies (summed across judges)",
                pm["total_excess_anomalies"],
            ),
        ]
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

        # Get heatmap data with caching
        @st.cache_data(ttl=300)  # 5-minute cache
        def get_cached_heatmap_data(
            metric,
            score_type,
            year_filter,
            competition_ids_tuple,
            discipline_ids_tuple,
            competition_scope_key,
            event_start_iso_key,
            event_end_iso_key,
        ):
            from datetime import date as _date

            analytics = get_analytics_safe()
            event_start = (
                _date.fromisoformat(event_start_iso_key)
                if event_start_iso_key
                else None
            )
            event_end = (
                _date.fromisoformat(event_end_iso_key) if event_end_iso_key else None
            )
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

        with st.spinner("Loading data..."):
            # Convert lists to tuples for caching
            comp_ids_tuple = tuple(competition_ids) if competition_ids else None
            disc_ids_tuple = tuple(discipline_ids) if discipline_ids else None

            pm = _cached_pooled_cross_judge_metrics(
                score_type,
                year_filter,
                comp_ids_tuple,
                disc_ids_tuple,
                competition_scope,
                event_start_iso,
                event_end_iso,
            )
            _render_pooled_benchmark_block(pm, score_type)

            heatmap_df = get_cached_heatmap_data(
                metric,
                score_type,
                year_filter,
                comp_ids_tuple,
                disc_ids_tuple,
                competition_scope,
                event_start_iso,
                event_end_iso,
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
        pm_jc = _cached_pooled_cross_judge_metrics(
            score_type,
            None,
            None,
            None,
            competition_scope,
            event_start_iso,
            event_end_iso,
        )
        _render_pooled_benchmark_block(pm_jc, score_type)

        @st.cache_data(ttl=300)
        def get_cached_judge_comp_heatmap(
            metric,
            score_type,
            competition_scope_key,
            event_start_iso_key,
            event_end_iso_key,
        ):
            from datetime import date as _date

            analytics = get_analytics_safe()
            event_start = (
                _date.fromisoformat(event_start_iso_key)
                if event_start_iso_key
                else None
            )
            event_end = (
                _date.fromisoformat(event_end_iso_key) if event_end_iso_key else None
            )
            return analytics.get_judge_competition_heatmap_data(
                metric=metric,
                score_type=score_type,
                competition_scope=competition_scope_key,
                event_start_date=event_start,
                event_end_date=event_end,
            )

        with st.spinner("Loading judge vs competition data..."):
            heatmap_df = get_cached_judge_comp_heatmap(
                metric,
                score_type,
                competition_scope,
                event_start_iso,
                event_end_iso,
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
                        st.markdown(
                            f"[{row['competition_name']}]({row['competition_url']}/index.asp)"
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
                        st.markdown(
                            f"[{row['competition_name']}]({row['competition_url']}/index.asp)"
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

    html_bytes = build_judge_report_html(
        selected_judge_display,
        stats,
        pcs_df,
        element_df,
        segment_df,
        single_competition_display_name=single_competition_display_name,
        filter_summary_lines=report_filter_lines,
    )
    _dn_comp = ""
    if single_competition_display_name:
        _dn_comp = (
            "_"
            + single_competition_display_name.replace(" ", "_")
            .replace("/", "_")
            .replace("(", "")
            .replace(")", "")
        )
    st.download_button(
        label="Download Interactive HTML Report",
        data=html_bytes,
        file_name=f"judge_report_{safe_name}{_dn_comp}.html",
        mime="text/html",
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
            'judge_score', 'panel_average', 'deviation'
        ]].copy()

        display_df.columns = [
            'Judge', 'Competition', 'Competition URL', 'Year',
            'Segment', 'Discipline', 'Skater', 'Element Name', 'Element Type',
            'Judge Score', 'Panel Average', 'Deviation'
        ]

        st.dataframe(display_df.drop('Competition URL', axis=1), width="stretch")

        # Show competition links
        if 'Competition URL' in display_df.columns and not display_df['Competition URL'].isna().all():
            st.subheader("Competition Links")
            unique_competitions = display_df[['Competition', 'Competition URL']].drop_duplicates()
            for _, row in unique_competitions.iterrows():
                if pd.notna(row['Competition URL']) and row['Competition URL']:
                    st.markdown(f"[{row['Competition']}]({row['Competition URL']}/index.asp)")

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
                    'element_name', 'element_type', 'judge_score', 
                    'panel_average', 'deviation'
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
                    'score_type', 'element_name', 'element_type', 'judge_score', 
                    'panel_average', 'deviation'
                ]]

                display_rule_errors.columns = [
                    'Judge', 'Segment', 'Discipline', 'Skater', 'Score Type',
                    'Element Name', 'Element Type', 'Judge Score', 'Panel Average', 'Deviation'
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
            st.download_button(
                "Download CSV",
                data=df_bm.to_csv(index=False).encode("utf-8"),
                file_name="panel_size_benchmarks.csv",
                mime="text/csv",
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
            load_qualifying, load_nqs = competition_load_flags_from_officials_type_id(
                load_oa_competition_type_id
            )
            st.caption(
                f"Stored flags: **qualifying**={load_qualifying}, **nqs**={load_nqs}. "
                "Nonqualifying (id **11**) → qualifying false. NQS (id **10**) → nqs true. "
                "Adult / Collegiate types (**12–14**) → qualifying true, nqs false."
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
                    isFSM = not base_url.strip().endswith("index.asp")
                    url=base_url.strip().replace("/index.asp", "").replace("/index.htm", "")
                    if url.endswith('/'):
                        url = url[:-1]

                    event_regex = effective_event_regex(
                        event_regex_custom, load_ev_levels, load_ev_disciplines
                    )

                    kwargs = dict(
                        base_url=url,
                        report_name=report_name.strip(),
                        event_regex=event_regex,
                        only_rule_errors=only_rule_errors,
                        use_gcp=False,
                        write_excel=False,
                        write_to_database=True,
                        year=year.strip(),
                        judge_filter=judge_filter.strip(),
                        specific_exclude=specific_exclude.strip(),
                        use_html=True,
                        isFSM=isFSM,
                        qualifying=load_qualifying,
                        nqs=load_nqs,
                        officials_analysis_competition_type_id=load_oa_competition_type_id,
                        update_officials_competition_type=True,
                    )

                    status_area = st.empty()
                    status_area.info("Running scrape — this may take a minute or two…")
                    try:
                        scrape_fn(**kwargs)
                        status_area.success(
                            f"Done! **{report_name.strip()}** has been imported into the database."
                        )
                        st.cache_resource.clear()
                        st.cache_data.clear()
                        st.session_state.pop("analytics", None)
                    except Exception as _exc:
                        status_area.error(f"Scrape failed: {_exc}")
                        with st.expander("Error details (traceback)", expanded=False):
                            st.code(traceback.format_exc(), language="python")

else:
    st.error(f"Unknown analysis page: {page!r}. Choose an option from the sidebar.")

render_query_help([
    "**Navigation:** `?page=` — `individual`, `cross-judge`, `temporal`, "
    "`panel-size-benchmarks`, `rule-errors`, `competition`, `load-competition`",
    "",
    "**Scope (most pages):** `?competition_scope=` — `all`, `qualifying`, `nqs`, "
    "`sectionals`, `championships`",
    "",
    "**Individual judge:** `?judge=` (identity label), `?year=` (season; omit for all years), "
    "`?disciplines=` (comma-separated names), `?start_date=` / `?end_date=` "
    "(ISO `YYYY-MM-DD`, with event-date filter enabled)",
    "",
    "**Cross-judge:** `?view=`, `?metric=`, `?score_type=`, `?year=`, `?disciplines=`, "
    "`?start_date=` / `?end_date=` (ISO dates, with event-date filter enabled)",
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
