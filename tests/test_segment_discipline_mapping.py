"""Tests for directory → segment discipline mapping (TC/TS pair-skating credit)."""

import importlib
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "sqlite:////tmp/segment_discipline_mapping_tests.db")

_mod_name = "activityAnalysis.load_activity_data"
_existing = sys.modules.get(_mod_name)
if _existing is not None and isinstance(
    getattr(_existing, "segment_discipline_type_ids_for_directory", None), MagicMock
):
    sys.modules.pop(_mod_name, None)
    sys.modules.pop("load_activity_data", None)

lad = importlib.import_module(_mod_name)

from activityAnalysis.load_activity_data import (
    NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,
    NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,
    NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES,
    segment_discipline_type_ids_for_directory,
)


def test_tc_ts_pairs_count_for_singles_not_reverse():
    singles_judge = segment_discipline_type_ids_for_directory(1, 1)
    assert NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES in singles_judge
    assert NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS not in singles_judge

    singles_tc = segment_discipline_type_ids_for_directory(1, 11)
    assert NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES in singles_tc
    assert NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS in singles_tc

    singles_ts = segment_discipline_type_ids_for_directory(1, 9)
    assert NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS in singles_ts

    pairs_tc = segment_discipline_type_ids_for_directory(8, 11)
    assert pairs_tc == (NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,)

    dance_tc = segment_discipline_type_ids_for_directory(4, 11)
    assert dance_tc == (NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,)

    sp_tc = segment_discipline_type_ids_for_directory(9, 11)
    assert NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES in sp_tc
    assert NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS in sp_tc
