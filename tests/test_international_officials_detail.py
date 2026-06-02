import pandas as pd
import pyarrow as pa

from activityAnalysis.international_officials_detail import _streamlit_safe_dataframe


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
