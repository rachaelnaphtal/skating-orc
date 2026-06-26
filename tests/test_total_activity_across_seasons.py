import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["DATABASE_URL"] = "sqlite:////tmp/activity_tracker_tests.db"

from activityAnalysis.international_listing_seasons import season_codes_ending_at
from activityAnalysis.load_activity_data import (
    TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS,
    TOTAL_ACTIVITY_BUCKET_INTERNATIONALS,
    TOTAL_ACTIVITY_BUCKET_NQS,
    TOTAL_ACTIVITY_BUCKET_NONQUALIFYING_DOMESTIC,
    TOTAL_ACTIVITY_BUCKET_SECTIONALS,
    _aggregate_total_activity_counts,
    _segment_counts_junior_senior,
    TOTAL_ACTIVITY_COL_OAC_COMPS,
    _oac_competition_count,
    _total_activity_metrics_index_from_aggregates,
    _normalize_competition_season_code,
    _total_activity_metric_columns,
    activity_competition_bucket,
)


def test_season_codes_ending_at_four_seasons_through_2526():
    assert season_codes_ending_at(2526, 4) == [2223, 2324, 2425, 2526]


def test_season_codes_ending_at_single_season():
    assert season_codes_ending_at(2526, 1) == [2526]


def test_activity_competition_bucket_priorities():
    assert (
        activity_competition_bucket(
            international=True,
            qualifying=True,
            nqs=False,
            competition_type_id=4,
        )
        == TOTAL_ACTIVITY_BUCKET_INTERNATIONALS
    )
    assert (
        activity_competition_bucket(
            international=False,
            qualifying=True,
            nqs=False,
            competition_type_id=4,
        )
        == TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS
    )
    assert (
        activity_competition_bucket(
            international=False,
            qualifying=True,
            nqs=False,
            competition_type_id=2,
        )
        == TOTAL_ACTIVITY_BUCKET_SECTIONALS
    )
    assert (
        activity_competition_bucket(
            international=False,
            qualifying=True,
            nqs=True,
            competition_type_id=10,
        )
        == TOTAL_ACTIVITY_BUCKET_NQS
    )
    assert (
        activity_competition_bucket(
            international=False,
            qualifying=False,
            nqs=False,
            competition_type_id=11,
        )
        == TOTAL_ACTIVITY_BUCKET_NONQUALIFYING_DOMESTIC
    )


def test_normalize_competition_season_code_calendar_year():
    codes = (2425, 2526)
    assert _normalize_competition_season_code("2526", codes) == 2526
    assert _normalize_competition_season_code(2026, codes) == 2526
    assert _normalize_competition_season_code(2024, codes) == 2425


def test_total_activity_metric_columns_omit_nqs_for_synchro():
    cols = _total_activity_metric_columns(
        [2425, 2526], include_nqs_bucket=False, include_per_season_columns=False
    )
    assert "NQS comps" not in cols
    assert "NQ comps" in cols
    assert "2526 all comps" not in cols
    assert cols.index("All comps") < cols.index("NQ comps")


def test_total_activity_metric_columns_per_season_optional():
    cols = _total_activity_metric_columns(
        [2425, 2526], include_per_season_columns=True
    )
    assert "2526 all comps" in cols
    assert "NQ Jr/Sr" in cols
    assert "jr/sr" not in " ".join(cols)


def test_segment_counts_junior_senior_min_starts_threshold():
    assert _segment_counts_junior_senior("Senior", 4, junior_senior_min_team_count=3)
    assert not _segment_counts_junior_senior("Senior", 3, junior_senior_min_team_count=3)
    assert not _segment_counts_junior_senior("Novice", 10, junior_senior_min_team_count=3)
    assert _segment_counts_junior_senior("Junior", 1, junior_senior_min_team_count=None)


def test_aggregate_total_activity_counts_qualifying_and_overall():
    import pandas as pd

    rows = pd.DataFrame(
        [
            {
                "official_id": 1,
                "season_code": 2526,
                "competition_id": 100,
                "segment_id": 10,
                "segment_level": "Senior",
                "team_count": 8,
                "international": False,
                "qualifying": True,
                "nqs": False,
                "competition_type_id": 4,
            },
            {
                "official_id": 1,
                "season_code": 2526,
                "competition_id": 200,
                "segment_id": 20,
                "segment_level": "Novice",
                "team_count": 0,
                "international": False,
                "qualifying": False,
                "nqs": False,
                "competition_type_id": 11,
            },
        ]
    )
    agg = _aggregate_total_activity_counts(rows, [1], [2526])
    rec = agg[1]
    assert len(rec["buckets"][TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS]["competitions"]) == 1
    assert len(rec["buckets"][TOTAL_ACTIVITY_BUCKET_NONQUALIFYING_DOMESTIC]["competitions"]) == 1
    assert len(rec["qualifying_total"]["competitions"]) == 1
    assert len(rec["overall_total"]["competitions"]) == 2
    assert len(rec["buckets"][TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS]["junior_senior_segments"]) == 1
    assert len(rec["buckets"][TOTAL_ACTIVITY_BUCKET_NONQUALIFYING_DOMESTIC]["junior_senior_segments"]) == 0


def test_total_activity_metrics_index_from_aggregates():
    import pandas as pd

    agg = pd.DataFrame(
        [
            {
                "official_id": 1,
                "season_code": 2526,
                "scope": "period",
                "rollup": "bucket",
                "bucket": TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS,
                "competitions": 2,
                "segments": 4,
                "junior_senior_segments": 3,
            },
            {
                "official_id": 1,
                "season_code": None,
                "scope": "period",
                "rollup": "qualifying",
                "bucket": None,
                "competitions": 5,
                "segments": 8,
                "junior_senior_segments": 6,
            },
            {
                "official_id": 1,
                "season_code": 2526,
                "scope": "season",
                "rollup": "overall",
                "bucket": None,
                "competitions": 1,
                "segments": 2,
                "junior_senior_segments": 1,
            },
        ]
    )
    idx = _total_activity_metrics_index_from_aggregates(agg, [1], [2526])
    rec = idx[1]
    assert rec["buckets"][TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS] == (2, 4, 3)
    assert rec["qualifying_total"] == (5, 8, 6)
    assert rec["overall"][2526] == (1, 2, 1)


def test_oac_competition_count_sums_sectionals_champs_intl():
    buckets = {
        TOTAL_ACTIVITY_BUCKET_SECTIONALS: (2, 0, 0),
        TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS: (1, 0, 0),
        TOTAL_ACTIVITY_BUCKET_INTERNATIONALS: (3, 0, 0),
        TOTAL_ACTIVITY_BUCKET_NQS: (5, 0, 0),
    }
    assert _oac_competition_count(buckets) == 6
