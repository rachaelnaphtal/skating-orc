import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from scipy import stats

from database import get_db_session, test_connection
from analytics import JudgeAnalytics
from models import Segment, DisciplineType, Judge, PcsScorePerJudge, ElementScorePerJudge, Element, SkaterSegment
from sqlalchemy import text, func, case
from report_html import build_judge_report_html

# Page configuration
st.set_page_config(page_title="Figure Skating Judge Analytics",
                   page_icon="⛸️",
                   layout="wide")

# Initialize session state
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Individual Judge Analysis"


# Test database connection with timeout and better error handling
@st.cache_resource
def get_analytics():
    try:
        with st.spinner("Connecting to database..."):
            connection_test = test_connection()
            if connection_test is not True:
                st.error(f"Database connection failed: {connection_test[1]}")
                st.info(
                    "This usually means the database is starting up. Please refresh the page in a few seconds."
                )
                st.stop()

            session = get_db_session()
            analytics_obj = JudgeAnalytics(session)

            # Test that we can actually query data
            try:
                judges = analytics_obj.get_judges()
                if not judges:
                    st.warning(
                        "Database connected but no judge data found. Please import your data first."
                    )
                    st.info(
                        "Use one of the import scripts to populate your database with figure skating data."
                    )
                else:
                    st.success(
                        f"Database connected successfully! Found {len(judges)} judges."
                    )
            except Exception as e:
                st.error(
                    f"Database connection successful but data access failed: {e}"
                )
                st.stop()

            return analytics_obj

    except Exception as e:
        st.error(f"Failed to initialize analytics: {e}")
        st.info("This might be a temporary issue. Please refresh the page.")
        st.stop()


# Lazy load analytics only when needed with connection retry
def get_analytics_safe():
    """Safely get analytics object with error handling and retry logic"""
    if 'analytics' not in st.session_state:
        st.session_state.analytics = get_analytics()

    # Check if we need to refresh the connection
    try:
        # Simple test query to check if connection is still alive
        analytics = st.session_state.analytics
        analytics.session.execute(text("SELECT 1"))
        return analytics
    except Exception as e:
        st.warning("Database connection lost, reconnecting...")
        # Clear cached analytics and reconnect
        if 'analytics' in st.session_state:
            del st.session_state.analytics
        st.cache_resource.clear()
        st.session_state.analytics = get_analytics()
        return st.session_state.analytics


# Main title
st.title("⛸️ Figure Skating Judge Performance Analytics")

# Navigation
import os as _os
_nav_pages = [
    "Individual Judge Analysis", "Multi-Judge Comparison",
    "Judge Performance Heatmap", "Temporal Trend Analysis",
    "Rule Errors Analysis", "Competition Analysis",
]
# if _os.path.exists("downloadResults.py"):
#     _nav_pages.append("Load Competition")
_nav_pages.append("Admin Tools")

page = st.sidebar.selectbox("Select Analysis Type", _nav_pages)


def judge_performance_heatmap():
    """Judge Performance Heatmap Analysis"""
    st.header("Judge Performance Heatmap")

    # Heatmap configuration
    st.subheader("Heatmap Configuration")
    col1, col2, col3 = st.columns(3)

    with col1:
        heatmap_type = st.selectbox("Heatmap Type",
                                    ["Judge Overview", "Judge vs Competition"])

    with col2:
        metric = st.selectbox("Performance Metric", [
            "throwout_rate", "anomaly_rate", "rule_error_rate", "avg_deviation", "excess_anomalies", "rule_errors"
        ],
                              format_func=lambda x: {
                                  "throwout_rate": "Throwout Rate (%)",
                                  "anomaly_rate": "Anomaly Rate (%)",
                                  "rule_error_rate": "Rule Error Rate (%)",
                                  "avg_deviation": "Average Deviation",
                                  "excess_anomalies": "Total Excess Anomalies",
                                  "rule_errors": "Total Rule Errors"
                              }[x])

    with col3:
        score_type = st.selectbox("Score Type", ["both", "pcs", "element"],
                                  format_func=lambda x: {
                                      "both": "Combined (PCS + Elements)",
                                      "pcs": "PCS Only",
                                      "element": "Elements Only"
                                  }[x])

    if heatmap_type == "Judge Overview":
        # Filters for judge overview
        st.subheader("Filters")
        col1, col2, col3 = st.columns(3)

        with col1:
            analytics = get_analytics_safe()
            years = analytics.get_years()
            year_filter = st.selectbox("Filter by Year", ["All Years"] + years)
            year_filter = None if year_filter == "All Years" else year_filter

        with col2:
            competitions = analytics.get_competitions()
            competition_names = [
                f"{name} ({year})" for comp_id, name, year in competitions
            ]
            selected_competitions = st.multiselect("Filter by Competitions",
                                                   competition_names)
            competition_ids = [
                comp_id for comp_id, name, year in competitions
                if f"{name} ({year})" in selected_competitions
            ] if selected_competitions else None

        with col3:
            discipline_types = analytics.get_discipline_types()
            discipline_names = [name for dt_id, name in discipline_types]
            selected_disciplines = st.multiselect("Filter by Discipline Type",
                                                  discipline_names)
            discipline_ids = [
                dt_id for dt_id, name in discipline_types
                if name in selected_disciplines
            ] if selected_disciplines else None

        # Get heatmap data with caching
        @st.cache_data(ttl=300)  # 5-minute cache
        def get_cached_heatmap_data(metric, score_type, year_filter, competition_ids_tuple, discipline_ids_tuple):
            analytics = get_analytics_safe()
            return analytics.get_judge_performance_heatmap_data(
                metric=metric,
                score_type=score_type,
                year_filter=year_filter,
                competition_ids=list(competition_ids_tuple) if competition_ids_tuple else None,
                discipline_type_ids=list(discipline_ids_tuple) if discipline_ids_tuple else None)

        with st.spinner("Loading heatmap data..."):
            # Convert lists to tuples for caching
            comp_ids_tuple = tuple(competition_ids) if competition_ids else None
            disc_ids_tuple = tuple(discipline_ids) if discipline_ids else None

            heatmap_df = get_cached_heatmap_data(
                metric, score_type, year_filter, comp_ids_tuple, disc_ids_tuple)

        if heatmap_df.empty:
            st.warning("No data found for selected filters")
            return

        # Create bar chart for judge overview
        metric_names = {
            "throwout_rate": "Throwout Rate (%)",
            "anomaly_rate": "Anomaly Rate (%)",
            "rule_error_rate": "Rule Error Rate (%)",
            "avg_deviation": "Average Deviation",
            "excess_anomalies": "Total Excess Anomalies",
            "rule_errors": "Total Rule Errors"
        }

        # Sort by metric value for better visualization
        heatmap_df_sorted = heatmap_df.sort_values('metric_value',
                                                   ascending=True)

        fig = px.bar(
            heatmap_df_sorted,
            x='metric_value',
            y='judge_name',
            orientation='h',
            title=
            f"Judge Performance: {metric_names[metric]} ({score_type.upper()})",
            labels={
                'metric_value': metric_names[metric],
                'judge_name': 'Judge'
            },
            color='metric_value',
            color_continuous_scale='Reds')

        fig.update_layout(height=max(400, len(heatmap_df_sorted) * 25))
        st.plotly_chart(fig, use_container_width=True)

        # Show data table
        st.subheader("Judge Performance Data")
        display_df = heatmap_df_sorted[[
            'judge_name', 'metric_value', 'total_scores'
        ]].copy()
        display_df.columns = [
            'Judge', metric_names[metric], 'Total Scores'
        ]
        st.dataframe(display_df, use_container_width=True)

    else:  # Judge vs Competition
        # Get heatmap data for judge vs competition
        with st.spinner("Loading judge vs competition heatmap data..."):
            analytics = get_analytics_safe()
            heatmap_df = analytics.get_judge_competition_heatmap_data(
                metric=metric, score_type=score_type)

        if heatmap_df.empty:
            st.warning("No data found for judge vs competition analysis")
            return

        # Create pivot table for heatmap
        pivot_df = heatmap_df.pivot(index='judge_name',
                                    columns='competition',
                                    values='metric_value')

        # Create heatmap
        metric_names = {
            "throwout_rate": "Throwout Rate (%)",
            "anomaly_rate": "Anomaly Rate (%)",
            "rule_error_rate": "Rule Error Rate (%)",
            "avg_deviation": "Average Deviation",
            "excess_anomalies": "Total Excess Anomalies",
            "rule_errors": "Total Rule Errors"
        }

        fig = px.imshow(
            pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            aspect='auto',
            color_continuous_scale='Reds',
            title=
            f"Judge vs Competition: {metric_names[metric]} ({score_type.upper()})"
        )

        fig.update_xaxes(side="bottom")
        fig.update_layout(xaxis={'categoryorder': 'category ascending'},
                          yaxis={'categoryorder': 'category ascending'},
                          height=max(400,
                                     len(pivot_df.index) * 25))

        st.plotly_chart(fig, use_container_width=True)

        # Show raw data
        st.subheader("Raw Data")
        display_df = heatmap_df[[
            'judge_name', 'competition', 'metric_value', 'total_scores'
        ]].copy()
        display_df.columns = [
            'Judge', 'Competition', metric_names[metric], 'Total Scores'
        ]
        st.dataframe(display_df, use_container_width=True)


def temporal_trend_analysis():
    """Temporal Trend Analysis for Judge Consistency"""
    st.header("Temporal Trend Analysis")

    # Analysis configuration
    st.subheader("Analysis Configuration")
    col1, col2, col3 = st.columns(3)

    with col1:
        analysis_type = st.selectbox("Analysis Type", [
            "Individual Judge Trends", "Overall System Trends",
            "Judge Consistency Ranking"
        ])

    with col2:
        metric = st.selectbox("Performance Metric", [
            "throwout_rate", "anomaly_rate", "rule_error_rate", "avg_deviation"
        ],
                              format_func=lambda x: {
                                  "throwout_rate": "Throwout Rate (%)",
                                  "anomaly_rate": "Anomaly Rate (%)",
                                  "rule_error_rate": "Rule Error Rate (%)",
                                  "avg_deviation": "Average Deviation"
                              }[x])

    with col3:
        score_type = st.selectbox("Score Type", ["both", "pcs", "element"],
                                  format_func=lambda x: {
                                      "both": "Combined (PCS + Elements)",
                                      "pcs": "PCS Only",
                                      "element": "Elements Only"
                                  }[x])

    metric_names = {
        "throwout_rate": "Throwout Rate (%)",
        "anomaly_rate": "Anomaly Rate (%)",
        "rule_error_rate": "Rule Error Rate (%)",
        "avg_deviation": "Average Deviation"
    }

    if analysis_type == "Individual Judge Trends":
        # Judge selection
        analytics = get_analytics_safe()
        judges = analytics.get_judges()
        if not judges:
            st.error("No judges found in database")
            return

        judge_options = {f"{name} ({location or 'Unknown location'})": judge_id for judge_id, name, location in judges}
        selected_judge_display = st.selectbox("Select Judge",
                                              list(judge_options.keys()))
        selected_judge_id = judge_options[selected_judge_display]

        # Get temporal trends data
        with st.spinner("Loading temporal trends data..."):
            trends_df = analytics.get_temporal_trends_data(
                judge_id=selected_judge_id,
                period='year',
                metric=metric,
                score_type=score_type)

            consistency_metrics = analytics.get_judge_consistency_metrics(
                selected_judge_id, metric, score_type)

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
        st.plotly_chart(fig, use_container_width=True)

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
        st.dataframe(display_df, use_container_width=True)

    elif analysis_type == "Overall System Trends":
        # Get system-wide temporal trends
        with st.spinner("Loading system-wide trends data..."):
            analytics = get_analytics_safe()
            trends_df = analytics.get_temporal_trends_data(
                judge_id=None,
                period='year',
                metric=metric,
                score_type=score_type)

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

        st.plotly_chart(fig, use_container_width=True)

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
        st.dataframe(display_df, use_container_width=True)

    else:  # Judge Consistency Ranking
        # Get all judges and their consistency metrics
        analytics = get_analytics_safe()
        judges = analytics.get_judges()
        if not judges:
            st.error("No judges found in database")
            return

        consistency_data = []

        with st.spinner("Calculating consistency metrics for all judges..."):
            for judge_id, judge_name, location in judges:
                consistency_metrics = analytics.get_judge_consistency_metrics(
                    judge_id, metric, score_type)

                # Get total scores for this judge
                trends_df = analytics.get_temporal_trends_data(
                    judge_id=judge_id,
                    period='year',
                    metric=metric,
                    score_type=score_type)

                if not trends_df.empty:
                    total_scores = trends_df['total_scores'].sum()
                    years_active = len(trends_df)

                    consistency_data.append({
                        'judge_name':
                        judge_name,
                        'location':
                        location or 'Unknown',
                        'consistency_score':
                        consistency_metrics['consistency_score'],
                        'trend_direction':
                        consistency_metrics['trend_direction'],
                        'trend_strength':
                        consistency_metrics['trend_strength'],
                        'coefficient_variation':
                        consistency_metrics['coefficient_variation'],
                        'total_scores':
                        total_scores,
                        'years_active':
                        years_active
                    })

        if not consistency_data:
            st.warning("No consistency data found")
            return

        consistency_df = pd.DataFrame(consistency_data)
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
        st.plotly_chart(fig, use_container_width=True)

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
        st.dataframe(display_df, use_container_width=True)


def statistical_bias_detection():
    """Statistical Bias Detection Analysis"""
    st.header("Statistical Bias Detection")

    # Analysis configuration
    st.subheader("Analysis Configuration")
    col1, col2 = st.columns(2)

    with col1:
        analysis_mode = st.selectbox("Analysis Mode", [
            "Individual Judge Analysis", "System-Wide Bias Summary",
            "Judge Comparison"
        ])

    with col2:
        significance_level = st.selectbox(
            "Significance Level", [0.05, 0.01, 0.001],
            format_func=lambda x:
            f"α = {x} ({'95%' if x == 0.05 else '99%' if x == 0.01 else '99.9%'} confidence)"
        )

    # Filters
    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)

    with col1:
        analytics = get_analytics_safe()
        years = analytics.get_years()
        year_filter = st.selectbox("Filter by Year", ["All Years"] + years)
        year_filter = None if year_filter == "All Years" else year_filter

    with col2:
        competitions = analytics.get_competitions()
        competition_names = [
            f"{name} ({year})" for comp_id, name, year in competitions
        ]
        selected_competitions = st.multiselect("Filter by Competitions",
                                               competition_names)
        competition_ids = [
            comp_id for comp_id, name, year in competitions
            if f"{name} ({year})" in selected_competitions
        ] if selected_competitions else None

    with col3:
        discipline_types = analytics.get_discipline_types()
        discipline_names = [name for dt_id, name in discipline_types]
        selected_disciplines = st.multiselect("Filter by Discipline Type",
                                              discipline_names)
        discipline_ids = [
            dt_id for dt_id, name in discipline_types
            if name in selected_disciplines
        ] if selected_disciplines else None

    if analysis_mode == "Individual Judge Analysis":
        # Judge selection
        judges = analytics.get_judges()
        if not judges:
            st.error("No judges found in database")
            return

        judge_options = {
            f"{name}": judge_id
            for judge_id, name, location in judges
        }
        selected_judge_display = st.selectbox("Select Judge",
                                              list(judge_options.keys()))
        selected_judge_id = judge_options[selected_judge_display]

        # Get statistical significance results
        with st.spinner("Running statistical bias tests..."):
            significance_results = analytics.calculate_statistical_significance(
                selected_judge_id, competition_ids, discipline_ids,
                year_filter)

        if not significance_results['pcs_tests'] and not significance_results[
                'element_tests']:
            st.warning("No data found for selected judge with current filters")
            return

        # Overall bias assessment
        st.subheader("Bias Detection Summary")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            bias_status = "🔴 BIAS DETECTED" if significance_results[
                'bias_detected'] else "🟢 NO BIAS DETECTED"
            st.metric("Bias Status", bias_status)

        with col2:
            overall_sig = "Yes" if significance_results[
                'overall_significance'] else "No"
            st.metric("Statistical Significance", overall_sig)

        with col3:
            sig_ratio = significance_results.get('significance_ratio', 0)
            st.metric("Significance Ratio", f"{sig_ratio:.0%}")

        with col4:
            confidence_level = f"{(1-significance_level)*100:.0f}%"
            st.metric("Confidence Level", confidence_level)

        # PCS Tests
        if significance_results['pcs_tests']:
            st.subheader("PCS Statistical Tests")

            for test_name, test_result in significance_results[
                    'pcs_tests'].items():
                with st.expander(f"PCS {test_name.replace('_', ' ').title()}",
                                 expanded=False):
                    test_cols = st.columns(3)

                    with test_cols[0]:
                        if 'statistic' in test_result:
                            st.metric("Test Statistic",
                                      f"{test_result['statistic']:.4f}")

                    with test_cols[1]:
                        if 'p_value' in test_result:
                            p_val = test_result['p_value']
                            significance_indicator = "🔴 Significant" if p_val < significance_level else "🟢 Not Significant"
                            st.metric("P-Value", f"{p_val:.4f}")
                            st.write(significance_indicator)

                    with test_cols[2]:
                        st.write("**Interpretation:**")
                        st.write(test_result['interpretation'])

                    # Additional metrics for specific tests
                    if test_name == 'throwout_chi2':
                        st.write(
                            f"Actual throwout rate: {test_result['actual_rate']:.2f}%"
                        )
                        st.write(
                            f"Expected throwout rate: {test_result['expected_rate']:.2f}%"
                        )
                    elif test_name == 'outlier_analysis':
                        st.write(
                            f"Outlier count: {test_result['outlier_count']}")
                        st.write(
                            f"Outlier rate: {test_result['outlier_rate']:.2f}%"
                        )

        # Element Tests
        if significance_results['element_tests']:
            st.subheader("Element Statistical Tests")

            for test_name, test_result in significance_results[
                    'element_tests'].items():
                with st.expander(
                        f"Element {test_name.replace('_', ' ').title()}",
                        expanded=False):
                    test_cols = st.columns(3)

                    with test_cols[0]:
                        if 'statistic' in test_result:
                            st.metric("Test Statistic",
                                      f"{test_result['statistic']:.4f}")

                    with test_cols[1]:
                        if 'p_value' in test_result:
                            p_val = test_result['p_value']
                            significance_indicator = "🔴 Significant" if p_val < significance_level else "🟢 Not Significant"
                            st.metric("P-Value", f"{p_val:.4f}")
                            st.write(significance_indicator)

                    with test_cols[2]:
                        st.write("**Interpretation:**")
                        st.write(test_result['interpretation'])

                    # Additional metrics for specific tests
                    if test_name == 'throwout_chi2':
                        st.write(
                            f"Actual throwout rate: {test_result['actual_rate']:.2f}%"
                        )
                        st.write(
                            f"Expected throwout rate: {test_result['expected_rate']:.2f}%"
                        )
                    elif test_name == 'outlier_analysis':
                        st.write(
                            f"Outlier count: {test_result['outlier_count']}")
                        st.write(
                            f"Outlier rate: {test_result['outlier_rate']:.2f}%"
                        )

    elif analysis_mode == "System-Wide Bias Summary":
        # Get bias detection summary for all judges
        with st.spinner("Analyzing bias across all judges..."):
            analytics = get_analytics_safe()
            bias_summary_df = analytics.get_bias_detection_summary(
                competition_ids, discipline_ids, year_filter)

        if bias_summary_df.empty:
            st.warning("No data found for bias analysis")
            return

        # System statistics
        st.subheader("System-Wide Bias Statistics")
        col1, col2, col3, col4 = st.columns(4)

        total_judges = len(bias_summary_df)
        bias_detected_count = bias_summary_df['bias_detected'].sum()
        significance_count = bias_summary_df['overall_significance'].sum()
        avg_significance_ratio = bias_summary_df['significance_ratio'].mean()

        with col1:
            st.metric("Total Judges Analyzed", total_judges)

        with col2:
            bias_rate = (bias_detected_count / total_judges *
                         100) if total_judges > 0 else 0
            st.metric("Judges with Bias Detected",
                      f"{bias_detected_count} ({bias_rate:.1f}%)")

        with col3:
            sig_rate = (significance_count / total_judges *
                        100) if total_judges > 0 else 0
            st.metric("Judges with Significance",
                      f"{significance_count} ({sig_rate:.1f}%)")

        with col4:
            st.metric("Avg Significance Ratio",
                      f"{avg_significance_ratio:.1%}")

        # Bias detection results
        st.subheader("Bias Detection Results")

        # Sort by bias detected and significance ratio
        bias_summary_sorted = bias_summary_df.sort_values(
            ['bias_detected', 'significance_ratio'], ascending=[False, False])

        # Create visualization
        fig = px.scatter(
            bias_summary_sorted,
            x='significance_ratio',
            y='total_scores',
            color='bias_detected',
            size='total_scores',
            hover_data=['judge_name', 'location'],
            title="Judge Bias Detection: Significance Ratio vs Total Scores",
            labels={
                'significance_ratio': 'Significance Ratio',
                'total_scores': 'Total Scores',
                'bias_detected': 'Bias Detected'
            },
            color_discrete_map={
                True: 'red',
                False: 'green'
            })

        st.plotly_chart(fig, use_container_width=True)

        # Detailed results table
        st.subheader("Detailed Bias Analysis")
        display_df = bias_summary_sorted[[
            'judge_name', 'location', 'bias_detected', 'overall_significance',
            'significance_ratio', 'total_scores', 'pcs_throwout_rate',
            'element_throwout_rate'
        ]].copy()
        display_df.columns = [
            'Judge', 'Location', 'Bias Detected', 'Statistical Significance',
            'Significance Ratio', 'Total Scores', 'PCS Throwout Rate (%)',
            'Element Throwout Rate (%)'
        ]

        # Format percentages
        for col in [
                'Significance Ratio', 'PCS Throwout Rate (%)',
                'Element Throwout Rate (%)'
        ]:
            display_df[col] = display_df[col].round(1)

        st.dataframe(display_df, use_container_width=True)

    else:  # Judge Comparison
        # Select two judges for comparison
        analytics = get_analytics_safe()
        judges = analytics.get_judges()
        if len(judges) < 2:
            st.error("Need at least 2 judges for comparison")
            return

        judge_options = {
            f"{name}": judge_id
            for judge_id, name, location in judges
        }

        col1, col2 = st.columns(2)
        with col1:
            judge_1_display = st.selectbox("Select First Judge",
                                           list(judge_options.keys()))
            judge_1_id = judge_options[judge_1_display]

        with col2:
            remaining_judges = [
                j for j in judge_options.keys() if j != judge_1_display
            ]
            judge_2_display = st.selectbox("Select Second Judge",
                                           remaining_judges)
            judge_2_id = judge_options[judge_2_display]

        score_type = st.selectbox("Score Type", ["both", "pcs", "element"],
                                  format_func=lambda x: {
                                      "both": "Both PCS & Elements",
                                      "pcs": "PCS Only",
                                      "element": "Elements Only"
                                  }[x])

        # Run comparison
        with st.spinner("Comparing judge distributions..."):
            comparison_results = analytics.compare_judge_distributions(
                judge_1_id, judge_2_id, score_type)

        if not comparison_results:
            st.warning("No comparable data found between selected judges")
            return

        st.subheader(
            f"Statistical Comparison: {judge_1_display} vs {judge_2_display}")

        for score_category, tests in comparison_results.items():
            st.write(f"**{score_category.upper()} Comparison:**")

            for test_name, test_result in tests.items():
                with st.expander(f"{test_name.replace('_', ' ').title()}",
                                 expanded=False):
                    test_cols = st.columns(3)

                    with test_cols[0]:
                        st.metric("Test Statistic",
                                  f"{test_result['statistic']:.4f}")

                    with test_cols[1]:
                        p_val = test_result['p_value']
                        significance_indicator = "🔴 Significant" if p_val < significance_level else "🟢 Not Significant"
                        st.metric("P-Value", f"{p_val:.4f}")
                        st.write(significance_indicator)

                    with test_cols[2]:
                        st.write("**Interpretation:**")
                        st.write(test_result['interpretation'])


if page == "Individual Judge Analysis":
    st.header("Individual Judge Analysis")

    # Judge selection
    analytics = get_analytics_safe()
    judges = analytics.get_judges()
    if not judges:
        st.error("No judges found in database")
        st.stop()

    judge_options = {
        f"{name}": judge_id
        for judge_id, name, location in judges
    }
    selected_judge_display = st.selectbox("Select Judge",
                                          list(judge_options.keys()))
    selected_judge_id = judge_options[selected_judge_display]

    # Filters
    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)

    with col1:
        years = analytics.get_years()
        year_filter = st.selectbox("Filter by Year", ["All Years"] + years)
        year_filter = None if year_filter == "All Years" else year_filter

    with col2:
        # Get competitions where this judge participated
        judge_competitions = analytics.get_judge_competitions(selected_judge_id)
        competition_names = [
            f"{name} ({year})" for comp_id, name, year in judge_competitions
        ]
        selected_competitions = st.multiselect("Filter by Competitions",
                                               competition_names)
        competition_ids = [
            comp_id for comp_id, name, year in judge_competitions
            if f"{name} ({year})" in selected_competitions
        ] if selected_competitions else None

    with col3:
        discipline_types = analytics.get_discipline_types()
        discipline_names = [name for dt_id, name in discipline_types]
        selected_disciplines = st.multiselect("Filter by Discipline Type",
                                              discipline_names)
        discipline_ids = [
            dt_id for dt_id, name in discipline_types
            if name in selected_disciplines
        ] if selected_disciplines else None

    # Get data
    with st.spinner("Loading judge data..."):
        pcs_df = analytics.get_judge_pcs_stats(selected_judge_id, year_filter,
                                               competition_ids, discipline_ids)
        element_df = analytics.get_judge_element_stats(selected_judge_id,
                                                       year_filter,
                                                       competition_ids,
                                                       discipline_ids)
        segment_df = analytics.get_judge_segment_stats(selected_judge_id, year_filter,
                                                      competition_ids, discipline_ids)

    if pcs_df.empty and element_df.empty:
        st.warning("No data found for selected judge with current filters")
    else:
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
            st.dataframe(display_summary, use_container_width=True)

            # Detailed element scores with issues
            st.subheader("Element Scores with Issues")

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

                st.dataframe(display_df.drop('competition_url', axis=1),
                             use_container_width=True)

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
            st.dataframe(display_summary, use_container_width=True)

            # Detailed PCS scores with issues
            st.subheader("PCS Scores with Issues")

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

                st.dataframe(display_df_pcs.drop('competition_url', axis=1),
                             use_container_width=True)

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

            st.dataframe(segment_display_with_totals, use_container_width=True)
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
        html_bytes = build_judge_report_html(selected_judge_display, stats,
                                             pcs_df, element_df, segment_df)
        st.download_button(
            label="Download Interactive HTML Report",
            data=html_bytes,
            file_name=f"judge_report_{safe_name}.html",
            mime="text/html",
        )

elif page == "Rule Errors Analysis":
    st.header("Rule Errors Analysis")

    analytics = get_analytics_safe()

    # Filters
    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)

    with col1:
        years = analytics.get_years()
        year_filter = st.selectbox("Filter by Year", ["All Years"] + years, key="rule_errors_year")
        year_filter = None if year_filter == "All Years" else year_filter

    with col2:
        competitions = analytics.get_competitions()
        competition_names = [f"{name} ({year})" for comp_id, name, year in competitions]
        selected_competitions = st.multiselect("Filter by Competitions", competition_names, key="rule_errors_comps")
        competition_ids = [
            comp_id for comp_id, name, year in competitions
            if f"{name} ({year})" in selected_competitions
        ] if selected_competitions else None

    with col3:
        judges = analytics.get_judges()
        judge_names = [name for judge_id, name, location in judges]
        selected_judges = st.multiselect("Filter by Judges", judge_names, key="rule_errors_judges")
        judge_ids = [
            judge_id for judge_id, name, location in judges
            if name in selected_judges
        ] if selected_judges else None

    # Get rule errors data
    with st.spinner("Loading rule errors data..."):
        rule_errors_df = analytics.get_all_rule_errors(year_filter, competition_ids, judge_ids)

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

        st.dataframe(judge_summary_display, use_container_width=True)

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

        st.dataframe(display_df.drop('Competition URL', axis=1), use_container_width=True)

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
        list(competition_options.keys())
    )

    if selected_competition:
        competition_id = competition_options[selected_competition]

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

            # Replace 0s with empty strings for display but keep numeric for calculations
            anomalies_grid_display = anomalies_grid.replace(0, '')
            excess_grid_display = excess_grid.replace(0, '')

            # Display total anomalies grid
            st.subheader("Total Anomalies by Judge and Segment")
            if not anomalies_grid.empty:
                st.dataframe(anomalies_grid_display, use_container_width=True)

                # Add judge totals for total anomalies
                st.subheader("Judge Totals - Total Anomalies")
                judge_totals_anomalies = anomalies_grid.sum().sort_values(ascending=False)
                judge_totals_df_anomalies = pd.DataFrame({
                    'Judge': judge_totals_anomalies.index,
                    'Total Anomalies': judge_totals_anomalies.values
                })
                st.dataframe(judge_totals_df_anomalies, use_container_width=True)
            else:
                st.info("No anomalies data available for grid display")

            # Display excess anomalies grid
            st.subheader("Excess Anomalies by Judge and Segment")
            if not excess_grid.empty:
                st.dataframe(excess_grid_display, use_container_width=True)

                # Add judge totals for excess anomalies
                st.subheader("Judge Totals - Excess Anomalies")
                judge_totals_excess = excess_grid.sum().sort_values(ascending=False)
                judge_totals_df_excess = pd.DataFrame({
                    'Judge': judge_totals_excess.index,
                    'Total Excess Anomalies': judge_totals_excess.values
                })
                st.dataframe(judge_totals_df_excess, use_container_width=True)
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
                st.dataframe(judge_rule_error_df, use_container_width=True)

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

                st.dataframe(display_rule_errors, use_container_width=True)
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

                        st.dataframe(display_pcs_anomalies, use_container_width=True)
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

                        st.dataframe(display_element_anomalies, use_container_width=True)
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
                    st.dataframe(summary_df, use_container_width=True)
                    
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
                                    st.dataframe(seg_df, use_container_width=True)
                else:
                    st.info("No judge performance data available for this competition")
            else:
                st.info("No segments found for this competition")

elif page == "Multi-Judge Comparison":
    st.header("Multi-Judge Comparison")

    # Judge selection
    analytics = get_analytics_safe()
    judges = analytics.get_judges()
    if not judges:
        st.error("No judges found in database")
        st.stop()

    judge_options = {
        f"{name}": judge_id
        for judge_id, name, location in judges
    }
    selected_judges_display = st.multiselect("Select Judges to Compare",
                                             list(judge_options.keys()))

    if not selected_judges_display:
        st.warning("Please select at least one judge to compare")
    else:
        selected_judge_ids = [
            judge_options[judge_display]
            for judge_display in selected_judges_display
        ]

        # Filters
        st.subheader("Filters")
        col1, col2, col3 = st.columns(3)

        with col1:
            years = analytics.get_years()
            year_filter = st.selectbox("Filter by Year", ["All Years"] + years)
            year_filter = None if year_filter == "All Years" else year_filter

        with col2:
            competitions = analytics.get_competitions()
            competition_names = [
                f"{name} ({year})" for comp_id, name, year in competitions
            ]
            selected_competitions = st.multiselect("Filter by Competitions",
                                                   competition_names)
            competition_ids = [
                comp_id for comp_id, name, year in competitions
                if f"{name} ({year})" in selected_competitions
            ] if selected_competitions else None

        with col3:
            discipline_types = analytics.get_discipline_types()
            discipline_names = [name for dt_id, name in discipline_types]
            selected_disciplines = st.multiselect("Filter by Discipline Type",
                                                  discipline_names)
            discipline_ids = [
                dt_id for dt_id, name in discipline_types
                if name in selected_disciplines
            ] if selected_disciplines else None

        # Get comparison data
        with st.spinner("Loading comparison data..."):
            pcs_comparison_df = analytics.get_multi_judge_pcs_comparison(
                selected_judge_ids, year_filter, competition_ids,
                discipline_ids)
            element_comparison_df = analytics.get_multi_judge_element_comparison(
                selected_judge_ids, year_filter, competition_ids,
                discipline_ids)

        if pcs_comparison_df.empty and element_comparison_df.empty:
            st.warning(
                "No data found for selected judges with current filters")
        else:
            # Summary comparison table
            st.subheader("Judge Comparison Summary")

            summary_data = []
            for judge_id in selected_judge_ids:
                judge_name = next(name for jid, name, _ in judges
                                  if jid == judge_id)

                # PCS stats
                judge_pcs = pcs_comparison_df[
                    pcs_comparison_df['judge_id'] ==
                    judge_id] if not pcs_comparison_df.empty else pd.DataFrame(
                    )
                # Element stats
                judge_elements = element_comparison_df[
                    element_comparison_df['judge_id'] ==
                    judge_id] if not element_comparison_df.empty else pd.DataFrame(
                    )

                pcs_throwout_rate = (judge_pcs['thrown_out'].sum() /
                                     len(judge_pcs) *
                                     100) if not judge_pcs.empty else 0
                pcs_anomaly_rate = (judge_pcs['anomaly'].sum() /
                                    len(judge_pcs) *
                                    100) if not judge_pcs.empty else 0
                pcs_rule_error_rate = (judge_pcs['is_rule_error'].sum() /
                                       len(judge_pcs) *
                                       100) if not judge_pcs.empty else 0
                element_throwout_rate = (
                    judge_elements['thrown_out'].sum() / len(judge_elements) *
                    100) if not judge_elements.empty else 0
                element_anomaly_rate = (judge_elements['anomaly'].sum() /
                                        len(judge_elements) *
                                        100) if not judge_elements.empty else 0
                element_rule_error_rate = (
                    judge_elements['is_rule_error'].sum() /
                    len(judge_elements) *
                    100) if not judge_elements.empty else 0

                summary_data.append({
                    'Judge':
                    judge_name,
                    'PCS Scores':
                    len(judge_pcs),
                    'PCS Throwout Rate (%)':
                    round(pcs_throwout_rate, 1),
                    'PCS Anomaly Rate (%)':
                    round(pcs_anomaly_rate, 1),
                    'PCS Rule Error Rate (%)':
                    round(pcs_rule_error_rate, 1),
                    'Element Scores':
                    len(judge_elements),
                    'Element Throwout Rate (%)':
                    round(element_throwout_rate, 1),
                    'Element Anomaly Rate (%)':
                    round(element_anomaly_rate, 1),
                    'Element Rule Error Rate (%)':
                    round(element_rule_error_rate, 1)
                })

            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True)

            # Comparison tables
            if not pcs_comparison_df.empty:
                st.subheader("PCS Comparison Analysis")

                # PCS throwout rates by judge
                pcs_judge_summary = pcs_comparison_df.groupby(
                    'judge_name').agg({
                        'thrown_out': ['sum', 'count'],
                        'anomaly': 'sum',
                        'is_rule_error': 'sum'
                    }).round(2)

                pcs_judge_summary.columns = [
                    'throwouts', 'total_scores', 'anomalies', 'rule_errors'
                ]
                pcs_judge_summary['throwout_rate'] = (
                    pcs_judge_summary['throwouts'] /
                    pcs_judge_summary['total_scores']) * 100
                pcs_judge_summary['anomaly_rate'] = (
                    pcs_judge_summary['anomalies'] /
                    pcs_judge_summary['total_scores']) * 100
                pcs_judge_summary['rule_error_rate'] = (
                    pcs_judge_summary['rule_errors'] /
                    pcs_judge_summary['total_scores']) * 100
                pcs_judge_summary = pcs_judge_summary.reset_index()

                # Display PCS comparison as table
                st.subheader("PCS Judge Comparison Table")
                display_pcs = pcs_judge_summary[[
                    'judge_name', 'total_scores', 'throwouts', 'throwout_rate',
                    'anomalies', 'anomaly_rate', 'rule_errors',
                    'rule_error_rate'
                ]].copy()
                display_pcs.columns = [
                    'Judge', 'Total PCS Scores', 'Throwouts',
                    'Throwout Rate (%)', 'Anomalies (>1.5)',
                    'Anomaly Rate (%)', 'Rule Errors', 'Rule Error Rate (%)'
                ]
                for col in [
                        'Throwout Rate (%)', 'Anomaly Rate (%)',
                        'Rule Error Rate (%)'
                ]:
                    display_pcs[col] = display_pcs[col].round(1)
                st.dataframe(display_pcs, use_container_width=True)

            if not element_comparison_df.empty:
                st.subheader("Element Comparison Analysis")

                # Element throwout rates by judge
                element_judge_summary = element_comparison_df.groupby(
                    'judge_name').agg({
                        'thrown_out': ['sum', 'count'],
                        'anomaly': 'sum',
                        'is_rule_error': 'sum'
                    }).round(2)

                element_judge_summary.columns = [
                    'throwouts', 'total_scores', 'anomalies', 'rule_errors'
                ]
                element_judge_summary['throwout_rate'] = (
                    element_judge_summary['throwouts'] /
                    element_judge_summary['total_scores']) * 100
                element_judge_summary['anomaly_rate'] = (
                    element_judge_summary['anomalies'] /
                    element_judge_summary['total_scores']) * 100
                element_judge_summary['rule_error_rate'] = (
                    element_judge_summary['rule_errors'] /
                    element_judge_summary['total_scores']) * 100
                element_judge_summary = element_judge_summary.reset_index()

                # Display Element comparison as table
                st.subheader("Element Judge Comparison Table")
                display_elements = element_judge_summary[[
                    'judge_name', 'total_scores', 'throwouts', 'throwout_rate',
                    'anomalies', 'anomaly_rate', 'rule_errors',
                    'rule_error_rate'
                ]].copy()
                display_elements.columns = [
                    'Judge', 'Total Element Scores', 'Throwouts',
                    'Throwout Rate (%)', 'Anomalies (>2.0)',
                    'Anomaly Rate (%)', 'Rule Errors', 'Rule Error Rate (%)'
                ]
                for col in [
                        'Throwout Rate (%)', 'Anomaly Rate (%)',
                        'Rule Error Rate (%)'
                ]:
                    display_elements[col] = display_elements[col].round(1)
                st.dataframe(display_elements, use_container_width=True)

            # Export functionality
            st.subheader("Export Data")
            if st.button("Export Summary Data to CSV"):
                csv_data = summary_df.to_csv(index=False)
                st.download_button(label="Download CSV",
                                   data=csv_data,
                                   file_name="judge_comparison_summary.csv",
                                   mime="text/csv")

elif page == "Judge Performance Heatmap":
    judge_performance_heatmap()

elif page == "Temporal Trend Analysis":
    temporal_trend_analysis()

elif page == "Admin Tools":
    st.header("Admin Tools")

    st.subheader("Merge Judges")
    st.write(
        "Use this when the same judge appears under two different names. "
        "All scores from the **duplicate** will be reassigned to the **judge to keep**, "
        "then the duplicate record will be deleted. This cannot be undone."
    )

    analytics = get_analytics_safe()
    judges = analytics.get_judges()
    if not judges:
        st.error("No judges found in database.")
        st.stop()

    judge_options = {name: judge_id for judge_id, name, location in judges}
    judge_names = list(judge_options.keys())

    col1, col2 = st.columns(2)
    with col1:
        keep_name = st.selectbox("Judge to keep (primary)", judge_names,
                                 key="merge_keep")
    with col2:
        dupe_options = [n for n in judge_names if n != keep_name]
        dupe_name = st.selectbox("Judge to merge & remove (duplicate)",
                                 dupe_options, key="merge_dupe")

    keep_id = judge_options[keep_name]
    dupe_id = judge_options[dupe_name]

    # Preview counts
    session = analytics.session
    from sqlalchemy import text as sqlt

    pcs_count = session.execute(
        sqlt("SELECT COUNT(*) FROM pcs_score_per_judge WHERE judge_id = :id"),
        {"id": dupe_id}).scalar()
    elem_count = session.execute(
        sqlt("SELECT COUNT(*) FROM element_score_per_judge WHERE judge_id = :id"),
        {"id": dupe_id}).scalar()

    st.markdown("---")
    st.write("**Preview — scores that will be reassigned:**")
    preview_col1, preview_col2 = st.columns(2)
    with preview_col1:
        st.metric("PCS scores", pcs_count)
    with preview_col2:
        st.metric("Element scores", elem_count)

    if pcs_count == 0 and elem_count == 0:
        st.warning(
            f"**{dupe_name}** has no scores in the database. "
            "Only the judge record itself will be deleted."
        )

    st.markdown("---")
    confirmed = st.checkbox(
        f'I understand this will permanently merge "{dupe_name}" into "{keep_name}" '
        f'and delete the "{dupe_name}" record.')

    if st.button("Execute Merge", disabled=not confirmed, type="primary"):
        try:
            session.execute(
                sqlt("UPDATE pcs_score_per_judge SET judge_id = :keep WHERE judge_id = :dupe"),
                {"keep": keep_id, "dupe": dupe_id})
            session.execute(
                sqlt("UPDATE element_score_per_judge SET judge_id = :keep WHERE judge_id = :dupe"),
                {"keep": keep_id, "dupe": dupe_id})
            # Clear precomputed cache rows for both judges if the tables exist
            for tbl in ("judge_excess_anomalies_cache", "judge_summary_cache"):
                try:
                    session.execute(sqlt("SAVEPOINT merge_cache"))
                    session.execute(
                        sqlt(f"DELETE FROM {tbl} WHERE judge_id IN (:keep, :dupe)"),
                        {"keep": keep_id, "dupe": dupe_id})
                    session.execute(sqlt("RELEASE SAVEPOINT merge_cache"))
                except Exception:
                    session.execute(sqlt("ROLLBACK TO SAVEPOINT merge_cache"))
            session.execute(
                sqlt("DELETE FROM judge WHERE id = :dupe"),
                {"dupe": dupe_id})
            session.commit()
            st.cache_resource.clear()
            st.success(
                f"Done! **{pcs_count}** PCS scores and **{elem_count}** element scores "
                f"have been moved to **{keep_name}**. "
                f'The record for "{dupe_name}" has been deleted.'
            )
        except Exception as e:
            session.rollback()
            st.error(f"Merge failed and was rolled back: {e}")

    # ── Manage Judge Emails ────────────────────────────────────────────────────
    st.divider()
    st.subheader("Manage Judge Emails")
    st.write(
        "Upload a spreadsheet (CSV or Excel) with two columns: **Name** and **Email**. "
        "Existing entries are updated by name; new entries are added. "
        "Column headers must be exactly `Name` and `Email`."
    )

    from email_reports import (ensure_email_table, get_email_list,
                                upsert_email_list, delete_email_entry)

    _em_session = get_analytics_safe().session
    ensure_email_table(_em_session)

    uploaded_email_file = st.file_uploader(
        "Upload Name/Email spreadsheet", type=["csv", "xlsx", "xls"],
        key="email_list_upload"
    )
    if uploaded_email_file:
        try:
            if uploaded_email_file.name.endswith(".csv"):
                email_upload_df = pd.read_csv(uploaded_email_file)
            else:
                email_upload_df = pd.read_excel(uploaded_email_file)

            # Flexible column detection (map existing col name -> target name)
            col_map = {}
            for col in email_upload_df.columns:
                if col.strip().lower() == "name":
                    col_map[col] = "judge_name"
                elif col.strip().lower() == "email":
                    col_map[col] = "email"

            if "judge_name" not in col_map.values() or "email" not in col_map.values():
                st.error(
                    f"Could not find 'Name' and 'Email' columns. "
                    f"Found: {list(email_upload_df.columns)}"
                )
            else:
                email_upload_df = email_upload_df.rename(columns=col_map)[
                    ["judge_name", "email"]
                ]
                ins, upd = upsert_email_list(_em_session, email_upload_df)
                st.success(f"Done — {ins} new entries added, {upd} updated.")
        except Exception as _e:
            st.error(f"Failed to read file: {_e}")

    email_list_df = get_email_list(_em_session)
    if email_list_df.empty:
        st.info("No judge emails stored yet. Upload a spreadsheet above to get started.")
    else:
        st.write(f"**{len(email_list_df)} judges** in email list:")
        st.dataframe(email_list_df.rename(columns={"judge_name": "Name", "email": "Email"}),
                     use_container_width=True, hide_index=True)

        with st.expander("Remove an entry"):
            del_name = st.selectbox("Select judge to remove",
                                    email_list_df["judge_name"].tolist(),
                                    key="del_email_name")
            if st.button("Remove", key="del_email_btn"):
                delete_email_entry(_em_session, del_name)
                st.success(f'Removed "{del_name}" from the email list.')
                st.rerun()

    # ── Email Competition Reports ──────────────────────────────────────────────
    st.divider()
    st.subheader("Email Competition Reports")
    st.write(
        "Select a competition and send each judge their individual HTML report by email. "
        "Judges not in the email list will be listed so you can decide what to do — "
        "their reports are still generated but won't be sent."
    )

    from email_reports import (match_judge_to_email, build_report_for_judge,
                                send_report_email, DEFAULT_EMAIL_SUBJECT,
                                DEFAULT_EMAIL_BODY)

    _all_comps = get_analytics_safe().get_competitions()
    if not _all_comps:
        st.info("No competitions found in the database.")
    else:
        comp_options = {f"{name} ({year})": cid for cid, name, year in _all_comps}
        selected_comp_label = st.selectbox("Competition", list(comp_options.keys()),
                                           key="email_comp_select")
        selected_comp_id = comp_options[selected_comp_label]
        selected_comp_name = selected_comp_label

        _email_list_df = get_email_list(_em_session)

        # Find judges who scored in this competition
        _comp_df = get_analytics_safe().get_competition_segment_statistics(
            int(selected_comp_id)
        )
        _judge_map = {}  # judge_id -> judge_name
        if not _comp_df.empty and "judge_id" in _comp_df.columns:
            for _, _jrow in _comp_df[["judge_id", "judge_name"]].drop_duplicates().iterrows():
                _judge_map[_jrow["judge_id"]] = _jrow["judge_name"]

        if not _judge_map:
            st.info("No judges found for this competition.")
        else:
            # Build match preview
            matched, unmatched = [], []
            for jid, jname in _judge_map.items():
                em = match_judge_to_email(jname, _email_list_df)
                if em:
                    matched.append({"Judge": jname, "Email": em, "judge_id": jid})
                else:
                    unmatched.append({"Judge": jname})

            col_m, col_u = st.columns(2)
            with col_m:
                st.write(f"**Will send ({len(matched)})**")
                if matched:
                    st.dataframe(pd.DataFrame(matched)[["Judge", "Email"]],
                                 use_container_width=True, hide_index=True)
                else:
                    st.info("None matched — upload an email list above first.")
            with col_u:
                st.write(f"**No email — skip ({len(unmatched)})**")
                if unmatched:
                    st.dataframe(pd.DataFrame(unmatched),
                                 use_container_width=True, hide_index=True)
                else:
                    st.info("All judges matched!")

            if matched:
                st.markdown("---")
                with st.expander("Email server settings", expanded=True):
                    st.write(
                        "These credentials are used only for this session and are "
                        "never stored. For Gmail, use an "
                        "[App Password](https://myaccount.google.com/apppasswords) "
                        "rather than your regular password."
                    )
                    _sc1, _sc2 = st.columns([3, 1])
                    with _sc1:
                        _smtp_host = st.text_input(
                            "SMTP host",
                            value=st.session_state.get("smtp_host", ""),
                            placeholder="smtp.gmail.com",
                            key="smtp_host_input"
                        )
                    with _sc2:
                        _smtp_port = st.number_input(
                            "Port", min_value=1, max_value=65535,
                            value=st.session_state.get("smtp_port", 587),
                            key="smtp_port_input"
                        )
                    _smtp_user = st.text_input(
                        "From email address",
                        value=st.session_state.get("smtp_user", ""),
                        placeholder="yourname@gmail.com",
                        key="smtp_user_input"
                    )
                    _smtp_pass = st.text_input(
                        "Password / App password",
                        type="password",
                        key="smtp_pass_input"
                    )
                    _smtp_from_name = st.text_input(
                        "Sender display name",
                        value=st.session_state.get("smtp_from_name",
                                                    "Figure Skating Officials"),
                        key="smtp_from_name_input"
                    )
                    # Persist non-sensitive fields across reruns
                    st.session_state["smtp_host"] = _smtp_host
                    st.session_state["smtp_port"] = _smtp_port
                    st.session_state["smtp_user"] = _smtp_user
                    st.session_state["smtp_from_name"] = _smtp_from_name

                _smtp_ready = all([_smtp_host.strip(), _smtp_user.strip(),
                                   _smtp_pass.strip()])

                st.markdown("**Email content**")
                st.caption(
                    "Use `{judge_name}`, `{competition_name}`, and `{from_name}` "
                    "anywhere — they'll be filled in individually for each judge."
                )
                _email_subject = st.text_input(
                    "Subject line",
                    value=st.session_state.get("email_subject", DEFAULT_EMAIL_SUBJECT),
                    key="email_subject_input"
                )
                _email_body = st.text_area(
                    "Email body",
                    value=st.session_state.get("email_body", DEFAULT_EMAIL_BODY),
                    height=220,
                    key="email_body_input"
                )
                st.session_state["email_subject"] = _email_subject
                st.session_state["email_body"] = _email_body

                if st.button("Send reports", type="primary",
                             key="send_reports_btn",
                             disabled=not _smtp_ready):
                    _smtp_cfg = {
                        "host": _smtp_host.strip(),
                        "port": int(_smtp_port),
                        "user": _smtp_user.strip(),
                        "password": _smtp_pass.strip(),
                        "from_name": _smtp_from_name.strip() or "Figure Skating Officials",
                    }
                    _analytics = get_analytics_safe()
                    results = []
                    prog = st.progress(0)
                    for i, row in enumerate(matched):
                        try:
                            html_bytes, _ = build_report_for_judge(
                                _analytics, int(row["judge_id"]),
                                int(selected_comp_id)
                            )
                            send_report_email(
                                _smtp_cfg, row["Email"], row["Judge"],
                                selected_comp_name, html_bytes,
                                subject_template=_email_subject,
                                body_template=_email_body,
                            )
                            results.append((row["Judge"], row["Email"], True, ""))
                        except Exception as _exc:
                            results.append((row["Judge"], row["Email"], False, str(_exc)))
                        prog.progress((i + 1) / len(matched))

                    sent = [r for r in results if r[2]]
                    failed = [r for r in results if not r[2]]
                    if sent:
                        st.success(f"Sent {len(sent)} report(s) successfully.")
                    if failed:
                        st.error(f"{len(failed)} failed to send:")
                        for name, addr, _, err in failed:
                            st.write(f"- **{name}** ({addr}): {err}")
                elif not _smtp_ready:
                    st.caption("Fill in the email server settings above to enable sending.")

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
            "`downloadResults.py` was not found in the project folder. "
            "Please upload or place it here before using this page."
        )
        st.stop()

    # ── Basic fields ────────────────────────────────────────────────────────
    base_url = st.text_input(
        "Competition URL",
        placeholder="https://ijs.usfigureskating.org/leaderboard/results/2026/34238",
    )
    report_name = st.text_input(
        "Report name (used as file prefix)",
        placeholder="2026_US_Synchronized_Skating_Championships",
    )
    year = st.text_input(
        "Season year code",
        value="2526",
        help="e.g. 2526 for the 2025-26 season, 2425 for 2024-25",
    )
    pdf_folder = st.text_input(
        "PDF output folder (leave blank if not saving PDFs)",
        value="",
        placeholder="/path/to/pdfs",
    )

    # ── Advanced options ─────────────────────────────────────────────────────
    with st.expander("Advanced options"):
        excel_folder = st.text_input("Excel output folder (leave blank to skip)", value="")
        event_regex = st.text_input(
            "Event regex filter",
            value="",
            help="Only process events whose names match this regex. Leave blank for all.",
        )
        judge_filter = st.text_input(
            "Judge filter",
            value="",
            help="Restrict processing to judges matching this string.",
        )
        specific_exclude = st.text_input(
            "Specific exclude",
            value="",
            help="Exclude specific events matching this string.",
        )
        only_rule_errors = st.checkbox("Only rule errors", value=False)
        add_additional_analysis = st.checkbox("Add additional analysis", value=False)
        use_html = st.checkbox("Use HTML mode", value=True)
        isFSM = st.checkbox("Is FSM competition", value=False)

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

            kwargs = dict(
                base_url=base_url.strip(),
                report_name=report_name.strip(),
                excel_folder=excel_folder.strip(),
                pdf_folder=pdf_folder.strip(),
                event_regex=event_regex.strip(),
                only_rule_errors=only_rule_errors,
                use_gcp=False,
                add_additional_analysis=add_additional_analysis,
                write_to_database=True,
                year=year.strip(),
                judge_filter=judge_filter.strip(),
                specific_exclude=specific_exclude.strip(),
                use_html=use_html,
                isFSM=isFSM,
            )

            status_area = st.empty()
            status_area.info("Running scrape — this may take a minute or two…")
            try:
                scrape_fn(**kwargs)
                status_area.success(
                    f"Done! **{report_name.strip()}** has been imported into the database."
                )
                st.cache_resource.clear()
            except Exception as _exc:
                status_area.error(f"Scrape failed: {_exc}")

else:  # Statistical Bias Detection
    statistical_bias_detection()

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
