import pandas as pd

from element_ranking_takeaways import (
    build_element_ranking_judge_takeaways,
    detail_filter_options,
    filter_judge_detail_tables,
)


def test_takeaways_overall_and_top_slices():
    marking = pd.Series(
        {
            "Judge": "Alex Example",
            "Marking score": 0.42,
            "Element marks": 500,
            "Mean GOE bias": 0.06,
        }
    )
    jd = pd.DataFrame(
        [
            {
                "Judge": "Alex Example",
                "Discipline": "Women",
                "Element marks": 200,
                "Partial marking score": 0.55,
                "Mean GOE bias": 0.08,
                "Mean |error|": 0.12,
                "Mean σ̂": 0.3,
            },
            {
                "Judge": "Alex Example",
                "Discipline": "Pairs",
                "Element marks": 300,
                "Partial marking score": 0.35,
                "Mean GOE bias": -0.04,
                "Mean |error|": 0.09,
                "Mean σ̂": 0.28,
            },
        ]
    )
    je = pd.DataFrame(
        [
            {
                "Judge": "Alex Example",
                "Discipline": "Women",
                "Element type": "Triple Lutz",
                "Element marks": 80,
                "Partial marking score": 0.62,
                "Mean GOE bias": 0.10,
                "Mean |error|": 0.14,
            },
            {
                "Judge": "Alex Example",
                "Discipline": "Women",
                "Element type": "Step Sequence",
                "Element marks": 40,
                "Partial marking score": 0.40,
                "Mean GOE bias": 0.01,
                "Mean |error|": 0.05,
            },
        ]
    )
    lines = build_element_ranking_judge_takeaways(
        "Alex Example", marking, jd, je, min_marks=30, top_n=2
    )
    text = "\n".join(lines)
    assert "Overall GOE bias" in text
    assert "Women" in text
    assert "Triple Lutz" in text
    assert "normalized deviations by element type" in text


def test_takeaways_empty_tables():
    assert (
        build_element_ranking_judge_takeaways(
            "Nobody", None, pd.DataFrame(), pd.DataFrame()
        )
        == []
    )


def test_takeaways_control_goe_bins():
    jg = pd.DataFrame(
        [
            {
                "Judge": "Alex Example",
                "Discipline": "Women",
                "Element type": "Triple Lutz",
                "Control GOE": 2,
                "Element marks": 40,
                "Partial marking score": 0.70,
                "Mean GOE bias": 0.12,
            },
            {
                "Judge": "Alex Example",
                "Discipline": "Women",
                "Element type": "Triple Lutz",
                "Control GOE": 0,
                "Element marks": 35,
                "Partial marking score": 0.35,
                "Mean GOE bias": -0.05,
            },
        ]
    )
    je = pd.DataFrame(
        [
            {
                "Judge": "Alex Example",
                "Discipline": "Women",
                "Element type": "Triple Lutz",
                "Element marks": 75,
                "Partial marking score": 0.55,
                "Mean GOE bias": 0.04,
            }
        ]
    )
    lines = build_element_ranking_judge_takeaways(
        "Alex Example", None, pd.DataFrame(), je, jg, min_bin_marks=15
    )
    text = "\n".join(lines)
    assert "normalized deviations by control goe range" in text.lower()
    assert "≥15 marks" in text
    assert "Triple Lutz" in text


def test_filter_judge_detail_tables():
    jd = pd.DataFrame(
        [
            {
                "Discipline": "Singles",
                "Element marks": 100,
                "Partial marking score": 0.5,
            },
            {
                "Discipline": "Pairs",
                "Element marks": 20,
                "Partial marking score": 0.8,
            },
        ]
    )
    je = pd.DataFrame(
        [
            {
                "Discipline": "Singles",
                "Element type": "Jump",
                "Element marks": 80,
                "Partial marking score": 0.6,
            },
            {
                "Discipline": "Singles",
                "Element type": "Spin",
                "Element marks": 10,
                "Partial marking score": 0.4,
            },
        ]
    )
    jd_f, je_f, _ = filter_judge_detail_tables(
        jd,
        je,
        pd.DataFrame(),
        min_marks=30,
        disciplines=["Singles"],
        element_types=["Jump"],
    )
    assert len(jd_f) == 1
    assert jd_f.iloc[0]["Discipline"] == "Singles"
    assert len(je_f) == 1
    assert je_f.iloc[0]["Element type"] == "Jump"
    discs, ets = detail_filter_options(jd, je, pd.DataFrame())
    assert "Singles" in discs
    assert "Jump" in ets


def test_takeaways_internal_column_names():
    je = pd.DataFrame(
        [
            {
                "judge_name": "Alex Example",
                "discipline": "Women",
                "element_type": "Triple Lutz",
                "element_marks": 80,
                "partial_marking_score": 0.62,
                "mean_goe_bias": 0.10,
            }
        ]
    )
    lines = build_element_ranking_judge_takeaways(
        "Alex Example", None, pd.DataFrame(), je, min_marks=30
    )
    assert any("Triple Lutz" in line for line in lines)
