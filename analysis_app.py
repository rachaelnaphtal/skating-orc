import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from scipy import stats

from database import get_db_session, test_connection
from analytics import JudgeAnalytics

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


# Lazy load analytics only when needed
def get_analytics_safe():
    """Safely get analytics object with error handling"""
    if 'analytics' not in st.session_state:
        st.session_state.analytics = get_analytics()
    return st.session_state.analytics


# Main title
st.title("⛸️ Figure Skating Judge Performance Analytics")

# Navigation
page = st.sidebar.selectbox("Select Analysis Type", [
    "Individual Judge Analysis", "Multi-Judge Comparison", 
    "Judge Performance Heatmap", "Temporal Trend Analysis",
    "Rule Errors Analysis", "Competition Analysis"
])


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

        # Get heatmap data
        with st.spinner("Loading heatmap data..."):
            heatmap_df = analytics.get_judge_performance_heatmap_data(
                metric=metric,
                score_type=score_type,
                year_filter=year_filter,
                competition_ids=competition_ids,
                discipline_type_ids=discipline_ids)

        if heatmap_df.empty:
            st.warning("No data found for selected filters")
            return

        # Create bar chart for judge overview
        metric_names = {
            "throwout_rate": "Throwout Rate (%)",
            "anomaly_rate": "Anomaly Rate (%)",
            "rule_error_rate": "Rule Error Rate (%)",
            "avg_deviation": "Average Deviation"
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
            'judge_name', 'metric_value', 'total_scores', 'pcs_scores',
            'element_scores'
        ]].copy()
        display_df.columns = [
            'Judge', metric_names[metric], 'Total Scores', 'PCS Scores',
            'Element Scores'
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
            "avg_deviation": "Average Deviation"
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
                else:
                    return 3
            
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

else:  # Statistical Bias Detection
    statistical_bias_detection()

# Sidebar information
st.sidebar.markdown("---")
st.sidebar.subheader("About")
st.sidebar.markdown("""
This dashboard analyzes figure skating judge performance by examining:
- **Throwout rates**: How often judges' scores are excluded from final calculations
- **Deviation rates**: How often judges score significantly differently from the panel average
- **PCS thresholds**: Deviations >= 1.5 points
- **Element thresholds**: Deviations >= 2.0 points

Use the filters to focus your analysis on specific years, competitions, or discipline types.
""")
