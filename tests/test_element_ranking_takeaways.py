import pandas as pd

from element_ranking_takeaways import build_element_ranking_judge_takeaways


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
    assert "Largest deviations by element type" in text


def test_takeaways_empty_tables():
    assert (
        build_element_ranking_judge_takeaways(
            "Nobody", None, pd.DataFrame(), pd.DataFrame()
        )
        == []
    )


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
