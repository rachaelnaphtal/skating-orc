import pandas as pd
import pyarrow as pa

from activityAnalysis.international_officials_detail import (
    INTL_VIEW_DETAIL,
    INTL_VIEW_MAJOR_EVENTS,
    INTL_VIEW_SUMMARY,
    _appointment_nav_key,
    _appointment_nav_label,
    _streamlit_safe_dataframe,
    intl_view_mode_from_query_param,
    intl_view_query_slug_for_mode,
)


def test_intl_view_query_param_round_trip():
    assert intl_view_mode_from_query_param("major_events") == INTL_VIEW_MAJOR_EVENTS
    assert intl_view_mode_from_query_param("summary") == INTL_VIEW_SUMMARY
    assert intl_view_mode_from_query_param("appointment") == INTL_VIEW_DETAIL
    assert intl_view_query_slug_for_mode(INTL_VIEW_MAJOR_EVENTS) == "major_events"
    assert intl_view_query_slug_for_mode(INTL_VIEW_SUMMARY) == "summary"


def test_appointment_nav_label_includes_discipline():
    row = pd.Series(
        {
            "official_id": 1,
            "official_name": "Jane Doe",
            "appointment_type": "International Judge",
            "discipline_id": 2,
            "discipline": "Singles",
        }
    )
    assert _appointment_nav_label(row) == "Jane Doe — International Judge — Singles"
    assert _appointment_nav_key(1, 12, 2) == (1, 12, 2)
    assert _appointment_nav_key(1, 16, pd.NA) == (1, 16, None)


def test_streamlit_safe_dataframe_converts_int64_season():
    df = pd.DataFrame(
        {
            "Season": pd.Series([2526], dtype="int64"),
            "Competition": ["Worlds"],
            "Comp scope": ["International"],
        }
    )
    out = _streamlit_safe_dataframe(df)
    assert out["Season"].dtype == object
    assert out.loc[0, "Season"] == "25-26"
    pa.Table.from_pandas(out)


def test_streamlit_safe_dataframe_preserves_preformatted_season():
    df = pd.DataFrame(
        {
            "Date": ["2024-03-01"],
            "Season": ["23-24 (2324)"],
            "In person": ["Yes"],
            "Notes": ["Online"],
        }
    )
    out = _streamlit_safe_dataframe(df)
    assert out.loc[0, "Season"] == "23-24 (2324)"
    pa.Table.from_pandas(out)


def test_streamlit_safe_dataframe_from_qualifying_competition_columns():
    df = pd.DataFrame(
        {
            "competition_id": pd.Series([1], dtype="int64"),
            "competition_year": pd.Series([2526], dtype="int64"),
            "competition_name": ["Worlds"],
            "competition_scope": ["ISU Championship"],
            "competition_type": ["ISU Championship"],
            "panel_roles": ["Judge, Technical Controller"],
        }
    )
    display = df.rename(
        columns={
            "competition_year": "Season",
            "competition_name": "Competition",
            "competition_scope": "Comp scope",
            "competition_type": "Comp type",
            "panel_roles": "Panel role(s)",
        }
    )
    show_cols = [
        "Season",
        "Competition",
        "Comp scope",
        "Comp type",
        "Panel role(s)",
    ]
    out = _streamlit_safe_dataframe(display[show_cols])
    assert out.loc[0, "Season"] == "25-26"
    pa.Table.from_pandas(out)
