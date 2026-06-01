import pandas as pd

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
