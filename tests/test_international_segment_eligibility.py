"""Tests for ISU Rule 411 segment eligibility."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "sqlite:////tmp/international_segment_eligibility_tests.db")

from activityAnalysis.international_segment_eligibility import (
    discipline_category_from_segment,
    enrich_panel_with_rule411_eligibility,
    evaluate_segment_rule411,
    extract_noc_from_skater_name,
    filter_panel_to_rule411_eligible,
    min_entries_for_discipline,
)


def test_extract_noc_from_skater_name():
    assert extract_noc_from_skater_name("Boglárka ZHANG HUN") == "HUN"
    assert extract_noc_from_skater_name("John SMITH USA") == "USA"
    assert extract_noc_from_skater_name("Anna Example") is None


def test_min_entries_for_discipline():
    assert min_entries_for_discipline("singles") == 6
    assert min_entries_for_discipline("pairs") == 4
    assert min_entries_for_discipline("dance") == 4
    assert min_entries_for_discipline("synchronized") is None
    assert min_entries_for_discipline("other") is None


def test_evaluate_segment_rule411_synchronized():
    stats = evaluate_segment_rule411(
        entry_count=7,
        distinct_noc_count=3,
        nocs_parsed_from_entries=7,
        discipline_category="synchronized",
    )
    assert stats.eligible
    assert stats.status_label == "Yes"
    assert "2" in stats.detail and "Members" in stats.detail
    stats_small_field = evaluate_segment_rule411(
        entry_count=2,
        distinct_noc_count=2,
        nocs_parsed_from_entries=2,
        discipline_category="synchronized",
    )
    assert stats_small_field.eligible
    stats_fail = evaluate_segment_rule411(
        entry_count=5,
        distinct_noc_count=1,
        nocs_parsed_from_entries=5,
        discipline_category="synchronized",
    )
    assert not stats_fail.eligible
    assert "ISU Members" in stats_fail.detail


def test_evaluate_segment_rule411_singles_pass():
    stats = evaluate_segment_rule411(
        entry_count=8,
        distinct_noc_count=3,
        nocs_parsed_from_entries=8,
        discipline_category="singles",
    )
    assert stats.eligible
    assert stats.status_label == "Yes"


def test_evaluate_segment_rule411_singles_fail_entries():
    stats = evaluate_segment_rule411(
        entry_count=5,
        distinct_noc_count=3,
        nocs_parsed_from_entries=5,
        discipline_category="singles",
    )
    assert not stats.eligible
    assert "5/6" in stats.detail


def test_evaluate_segment_rule411_unverified_nations():
    stats = evaluate_segment_rule411(
        entry_count=8,
        distinct_noc_count=0,
        nocs_parsed_from_entries=0,
        discipline_category="singles",
    )
    assert stats.eligible
    assert stats.status_label == "Unverified nations"


def test_evaluate_segment_rule411_fail_members():
    stats = evaluate_segment_rule411(
        entry_count=8,
        distinct_noc_count=1,
        nocs_parsed_from_entries=8,
        discipline_category="pairs",
    )
    assert not stats.eligible
    assert "ISU Members" in stats.detail


def test_discipline_category_from_segment_type_ids():
    assert discipline_category_from_segment(segment_discipline_type_id=1) == "singles"
    assert discipline_category_from_segment(segment_discipline_type_id=2) == "pairs"
    assert discipline_category_from_segment(segment_discipline_type_id=3) == "dance"


def test_filter_panel_to_rule411_eligible():
    panel = pd.DataFrame(
        [
            {
                "competition_type_id": 17,
                "segment_id": 1,
                "segment_discipline_type_id": 1,
                "segment_discipline": "Singles",
                "segment_level": "Senior",
            },
            {
                "competition_type_id": 4,
                "segment_id": 2,
                "segment_discipline_type_id": 1,
                "segment_discipline": "Singles",
                "segment_level": "Senior",
            },
        ]
    )
    enriched = enrich_panel_with_rule411_eligibility(panel)
    assert "rule411_status" in enriched.columns
    filtered = filter_panel_to_rule411_eligible(panel)
    assert len(filtered.loc[filtered["competition_type_id"] == 4]) == 1
