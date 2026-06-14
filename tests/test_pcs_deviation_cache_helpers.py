"""PCS deviation shard cache: identity map reuse and fingerprint passing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from pcs_deviation_analysis import PcsDeviationShard
from pcs_deviation_cache import (
    _load_marks_from_db,
    _save_shard_row,
    precompute_pcs_deviation_shards,
)


def test_load_marks_from_db_uses_provided_id_map():
    session = MagicMock()
    analytics = MagicMock()
    shard = PcsDeviationShard(
        season_year="2425",
        discipline_type_id=5,
        competition_scope="all",
        segment_level_preset="junior_senior",
    )
    raw = pd.DataFrame(
        {
            "judge_id": [1],
            "skater_segment_id": [10],
            "pcs_type_id": [1],
            "component": ["CO"],
            "discipline_type_id": [5],
            "judge_score": [8.0],
            "control_score": [7.5],
        }
    )
    id_map = pd.DataFrame(
        {
            "judge_id": [1],
            "judge_name": ["Merged Judge"],
            "judge_ids": ["1"],
        }
    )
    with (
        patch(
            "pcs_deviation_cache.load_pcs_deviation_marks",
            return_value=raw,
        ),
        patch("pcs_deviation_cache.load_judge_identity_map") as load_map,
    ):
        out = _load_marks_from_db(
            session, analytics, shard, id_map=id_map
        )
    load_map.assert_not_called()
    assert out.loc[0, "judge_name"] == "Merged Judge"


def test_save_shard_row_reuses_data_fingerprint():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    analytics = MagicMock()
    shard = PcsDeviationShard(
        season_year="2425",
        discipline_type_id=5,
        competition_scope="all",
    )
    marks = pd.DataFrame(
        {
            "judge_id": [1],
            "judge_name": ["J"],
            "skater_segment_id": [10],
            "pcs_type_id": [1],
            "component": ["CO"],
            "discipline_type_id": [5],
            "judge_score": [8.0],
            "control_score": [7.5],
        }
    )
    write_session = MagicMock()
    write_session.get.return_value = None
    maker = MagicMock(return_value=write_session)
    with (
        patch("pcs_deviation_cache.sessionmaker", return_value=maker),
        patch("pcs_deviation_cache._shard_fingerprint") as fp,
    ):
        _save_shard_row(
            session,
            analytics,
            shard,
            marks,
            data_fingerprint="fp-abc",
        )
    fp.assert_not_called()
    write_session.add.assert_called_once()
    saved = write_session.add.call_args[0][0]
    assert saved.data_fingerprint == "fp-abc"


def test_precompute_shards_skip_unchanged_without_reload():
    session = MagicMock()
    row = MagicMock()
    row.data_fingerprint = "fp-1"
    session.get.return_value = row
    analytics = MagicMock()
    id_map = pd.DataFrame(
        {"judge_id": [1], "judge_name": ["J"], "judge_ids": ["1"]}
    )
    with (
        patch(
            "pcs_deviation_cache.discipline_ids_for_pcs_deviation",
            return_value=[5],
        ),
        patch(
            "pcs_deviation_cache.load_judge_identity_map",
            return_value=id_map,
        ),
        patch(
            "pcs_deviation_cache._shard_fingerprint",
            return_value="fp-1",
        ),
        patch("pcs_deviation_cache._load_marks_from_db") as load_db,
        patch("pcs_deviation_cache._save_shard_row") as save_row,
    ):
        written, skipped = precompute_pcs_deviation_shards(
            session,
            analytics,
            competition_scope="all",
            season_years=["2425"],
            skip_unchanged=True,
        )
    assert written == 0
    assert skipped == 1
    load_db.assert_not_called()
    save_row.assert_not_called()


def test_precompute_shards_passes_fingerprint_to_save():
    session = MagicMock()
    session.get.return_value = None
    analytics = MagicMock()
    marks = pd.DataFrame(
        {
            "judge_id": [1],
            "judge_name": ["J"],
            "skater_segment_id": [10],
            "pcs_type_id": [1],
            "component": ["CO"],
            "discipline_type_id": [5],
            "judge_score": [8.0],
            "control_score": [7.5],
        }
    )
    id_map = pd.DataFrame(
        {"judge_id": [1], "judge_name": ["J"], "judge_ids": ["1"]}
    )
    with (
        patch(
            "pcs_deviation_cache.discipline_ids_for_pcs_deviation",
            return_value=[5],
        ),
        patch(
            "pcs_deviation_cache.load_judge_identity_map",
            return_value=id_map,
        ),
        patch(
            "pcs_deviation_cache._shard_fingerprint",
            return_value="fp-write",
        ),
        patch(
            "pcs_deviation_cache._load_marks_from_db",
            return_value=marks,
        ),
        patch("pcs_deviation_cache._save_shard_row") as save_row,
    ):
        written, skipped = precompute_pcs_deviation_shards(
            session,
            analytics,
            competition_scope="all",
            season_years=["2425"],
        )
    assert written == 1
    assert skipped == 0
    save_row.assert_called_once()
    assert save_row.call_args.kwargs["data_fingerprint"] == "fp-write"
