"""
Admin hub — opened at **/admin** when running ``streamlit run analysis_app.py``.

Uses the same ``DATABASE_URL`` / ``ADMIN_PASSWORD`` as the main judging analytics app.
Layout follows the Home app's ``st.set_page_config`` (avoid calling ``set_page_config`` here).
"""

import streamlit as st

import admin_sections as adm

_MAIN_ANALYTICS_SCRIPT = "analysis_app.py"

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
else:
    adm.render_merge_judges()
