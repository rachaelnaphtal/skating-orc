"""Shard cache helpers: identity refresh, skip-unchanged, summaries-only."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from element_deviation_ranking import ElementRankingShard
from element_ranking_cache import (
    _normalize_shard_marks,
    _shard_cache_is_fresh,
    build_precompute_element_ranking_run_params,
    precompute_element_ranking_shards,
)


class _FakeAnalytics:
    def get_judge_analysis_identity_groups(self):
        return [
            {"label": "Merged Judge", "judge_ids": [126, 2711]},
        ]


def test_normalize_shard_marks_refreshes_stale_judge_name():
    analytics = _FakeAnalytics()
    session = MagicMock()
    df = pd.DataFrame(
        {
            "element_id": [1],
            "judge_id": [2711],
            "judge_name": ["Whitney LUKE"],
            "judge_score": [2.0],
            "discipline_type_id": [1],
            "element_type_id": [10],
        }
    )
    out = _normalize_shard_marks(df, session, analytics)
    assert out.loc[0, "judge_name"] == "Merged Judge"


def test_shard_cache_is_fresh_false_when_missing():
    session = MagicMock()
    session.get.return_value = None
    shard = ElementRankingShard(
        season_year="2425",
        discipline_type_id=1,
        competition_scope="international",
        segment_level_preset="junior_senior",
    )
    assert _shard_cache_is_fresh(session, MagicMock(), shard) is False


def test_shard_cache_is_fresh_true_when_fingerprint_matches():
    session = MagicMock()
    row = MagicMock()
    row.data_fingerprint = "abc123"
    session.get.return_value = row
    shard = ElementRankingShard(
        season_year="2425",
        discipline_type_id=1,
        competition_scope="international",
    )
    with patch(
        "element_ranking_cache._shard_fingerprint",
        return_value="abc123",
    ):
        assert _shard_cache_is_fresh(session, MagicMock(), shard) is True


def test_precompute_shards_skip_unchanged():
    session = MagicMock()
    analytics = MagicMock()
    analytics.get_years.return_value = ["2425"]
    with (
        patch(
            "element_ranking_cache.discipline_ids_for_element_ranking",
            return_value=[1],
        ),
        patch(
            "element_ranking_cache._shard_cache_is_fresh",
            return_value=True,
        ),
        patch("element_ranking_cache._load_marks_from_db") as load_db,
        patch("element_ranking_cache._save_shard_row") as save_row,
    ):
        written, skipped = precompute_element_ranking_shards(
            session,
            analytics,
            competition_scope="international",
            season_years=["2425"],
            skip_unchanged=True,
        )
    assert written == 0
    assert skipped == 1
    load_db.assert_not_called()
    save_row.assert_not_called()


def test_precompute_shards_writes_when_not_fresh():
    session = MagicMock()
    analytics = MagicMock()
    marks = pd.DataFrame(
        {
            "element_id": [1],
            "judge_id": [1],
            "judge_score": [1.0],
            "discipline_type_id": [1],
            "element_type_id": [1],
        }
    )
    with (
        patch(
            "element_ranking_cache.discipline_ids_for_element_ranking",
            return_value=[1],
        ),
        patch(
            "element_ranking_cache._shard_cache_is_fresh",
            return_value=False,
        ),
        patch(
            "element_ranking_cache._load_marks_from_db",
            return_value=marks,
        ),
        patch("element_ranking_cache._save_shard_row") as save_row,
    ):
        written, skipped = precompute_element_ranking_shards(
            session,
            analytics,
            competition_scope="international",
            season_years=["2425"],
            skip_unchanged=True,
        )
    assert written == 1
    assert skipped == 0
    save_row.assert_called_once()


def test_precompute_shard_summaries_summaries_only_uses_cached_sigma():
    session = MagicMock()
    analytics = MagicMock()
    run_params = build_precompute_element_ranking_run_params("international")
    with (
        patch(
            "element_ranking_cache.get_or_fit_benchmark_sigma_params",
            return_value=({"bucket": 1.0}, None, True),
        ) as sigma_fit,
        patch(
            "element_ranking_cache.iter_element_ranking_shards",
            return_value=[],
        ),
        patch("element_ranking_cache._persist_shard_summaries_for_scope") as persist,
    ):
        from element_ranking_cache import precompute_element_ranking_shard_summaries

        n = precompute_element_ranking_shard_summaries(
            session,
            analytics,
            run_params,
            summaries_only=True,
        )
    assert n == 0
    sigma_fit.assert_called_once_with(
        session,
        analytics,
        run_params,
        cache_only=True,
        persist_shards=False,
        persist_sigma=False,
    )
    persist.assert_called_once()
    assert persist.call_args.kwargs["cache_only_marks"] is True
