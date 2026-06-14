import pandas as pd

from pcs_deviation_takeaways import filter_pcs_judge_detail_tables


def test_filter_pcs_judge_detail_tables_min_marks_and_component():
    jd = pd.DataFrame(
        {
            "discipline": ["Ice Dance"],
            "PCS marks": [40],
            "Partial marking score": [1.0],
        }
    )
    jc = pd.DataFrame(
        {
            "discipline": ["Ice Dance", "Ice Dance"],
            "component": ["SS", "CO"],
            "PCS marks": [40, 10],
            "Partial marking score": [1.0, 0.5],
        }
    )
    jb = pd.DataFrame(
        {
            "discipline": ["Ice Dance", "Ice Dance"],
            "component": ["SS", "CO"],
            "Control bin": [7, 7],
            "PCS marks": [25, 8],
            "Partial marking score": [1.1, 0.4],
        }
    )
    jd_f, jc_f, jb_f = filter_pcs_judge_detail_tables(
        jd, jc, jb, min_marks=15, components=["SS"]
    )
    assert len(jd_f) == 1
    assert list(jc_f["Component"]) == ["SS"]
    assert len(jb_f) == 1
    assert jb_f.iloc[0]["Component"] == "SS"
