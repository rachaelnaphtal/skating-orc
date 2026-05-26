"""
Shared Streamlit → JudgeAnalytics session helpers for Home (analysis_app) and Admin page.
"""

import streamlit as st
from sqlalchemy import text

from database import (
    ensure_database_for_streamlit,
    get_db_session,
    resolve_database_url,
    test_connection,
)
from analytics import JudgeAnalytics


@st.cache_resource
def get_analytics(_database_url: str, _database_source: str):
    try:
        ensure_database_for_streamlit()
        with st.spinner("Connecting to database…"):
            connection_test = test_connection()
            if connection_test is not True:
                st.error(f"Database connection failed: {connection_test[1]}")
                st.info(
                    "This usually means the database is starting up. Please refresh the page in a few seconds."
                )
                st.stop()

            session = get_db_session()
            analytics_obj = JudgeAnalytics(session)

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


def get_analytics_safe():
    """Safely get analytics object with error handling and retry logic."""
    ensure_database_for_streamlit()
    db_url, db_source = resolve_database_url()
    cached_url = st.session_state.get("_analytics_db_url")
    if cached_url != db_url and "analytics" in st.session_state:
        del st.session_state["analytics"]
        st.cache_resource.clear()

    if "analytics" not in st.session_state:
        st.session_state._analytics_db_url = db_url
        st.session_state.analytics = get_analytics(db_url, db_source)

    try:
        analytics = st.session_state.analytics
        analytics.session.execute(text("SELECT 1"))
        return analytics
    except Exception:
        st.warning("Database connection lost, reconnecting...")
        if "analytics" in st.session_state:
            del st.session_state["analytics"]
        st.cache_resource.clear()
        ensure_database_for_streamlit()
        db_url, db_source = resolve_database_url()
        st.session_state._analytics_db_url = db_url
        st.session_state.analytics = get_analytics(db_url, db_source)
        return st.session_state.analytics
