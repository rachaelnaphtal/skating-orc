"""
Admin hub — opened at **/admin** when running ``streamlit run analysis_app.py``.

Uses the same database resolution as ``analysis_app.py`` (``DATABASE_URL``, Streamlit
secrets, ``USE_CLOUD_DATABASE``, etc.).
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlparse

import streamlit as st

from database import ensure_database_for_streamlit, resolve_database_url

_MAIN_ANALYTICS_SCRIPT = "analysis_app.py"


def _sync_streamlit_database() -> tuple[str, str]:
    """Match ``analysis_app.py``: resolve URL from env/secrets, then share with activity code."""
    ensure_database_for_streamlit()
    db_url, db_source = resolve_database_url()
    os.environ["DATABASE_URL"] = db_url

    _activity = os.path.join(os.path.dirname(os.path.dirname(__file__)), "activityAnalysis")
    if _activity not in sys.path:
        sys.path.insert(0, _activity)
    try:
        from load_activity_data import refresh_activity_engine

        refresh_activity_engine()
    except Exception:
        pass

    return db_url, db_source


def _database_caption_label(db_url: str, db_source: str) -> str:
    try:
        parsed = urlparse(db_url)
        host = parsed.hostname or "?"
        name = (parsed.path or "").lstrip("/") or "?"
        scheme = (parsed.scheme or "db").split("+", 1)[0]
        return f"{scheme}://{host}/{name} ({db_source})"
    except Exception:
        return db_source


# Resolve DB before admin sections import activity/officials modules.
_db_url, _db_source = _sync_streamlit_database()

import admin_sections as adm  # noqa: E402

try:
    st.set_page_config(page_title="Admin", page_icon="🔧", layout="wide")
except Exception:
    pass


def _sidebar_nav_home() -> None:
    if st.sidebar.button(
        "← Analytics home",
        help="Return to the main analytics dashboard",
        type="secondary",
        width="stretch",
        key="admin_sidebar_home",
    ):
        st.switch_page(_MAIN_ANALYTICS_SCRIPT)


_sidebar_nav_home()

adm.render_admin_password_gate()

_title_col, _nav_col = st.columns([5, 2])
with _title_col:
    st.title("Admin")
with _nav_col:
    if st.button(
        "← Analytics home",
        help="Open the main judging analytics dashboard",
        type="primary",
        width="stretch",
        key="admin_main_home",
    ):
        st.switch_page(_MAIN_ANALYTICS_SCRIPT)

st.caption(
    "Password gate matches ``ADMIN_PASSWORD`` when set. "
    f"Database: {_database_caption_label(_db_url, _db_source)}. "
    "Use **← Analytics home** above or in the sidebar to return to the dashboard."
)

section = st.radio(
    "Section",
    [
        "Judge ↔ directory matcher",
        "Import officials directory",
        "Manage judge emails",
        "Email competition reports",
        "Public ↔ officials competition types",
        "International requirement rules",
        "ISU seminar attendance",
        "Merge judges",
    ],
    horizontal=True,
    key="admin_main_section",
)

if section == "Judge ↔ directory matcher":
    adm.render_judge_directory_matcher_embedded()
elif section == "Import officials directory":
    adm.render_directory_import()
elif section == "Manage judge emails":
    adm.render_manage_judge_emails()
elif section == "Email competition reports":
    adm.render_email_competition_reports()
elif section == "Public ↔ officials competition types":
    adm.render_public_competition_officials_types_breakdown()
elif section == "International requirement rules":
    adm.render_international_requirement_rules()
elif section == "ISU seminar attendance":
    adm.render_isu_official_seminars()
else:
    adm.render_merge_judges()
