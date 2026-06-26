"""
Per-discipline activity and conditional-format thresholds for national judge reports.

Singles/Pairs values match the original manual workbook. Ice dance and synchronized
each have their own ``ReportActivityThresholds`` — tune independently after reviewing
draft reports.

Threshold map (analysis sheet columns):

| Field | Column(s) | Effect |
|-------|-----------|--------|
| ``total_comps_in_role_low`` | AM | "Low" total activity (Total Activity flag) |
| ``competition_count_low`` | H, I (red CF), hidden AJ, AN | qualifying competition activity |
| ``segment_count_low`` | J (red CF), hidden AK | red when segments below cutoff |
| ``junior_senior_segment_count_low`` | K (red CF), hidden AL, AP | "Low" jr/sr activity |
| ``qualifying_rule_errors_poor`` | M | red CF (Singles/Pairs only) |
| ``champs_rule_errors_poor`` | V | red CF (Singles/Pairs only) |
| ``anomaly_pct_poor`` | N, AF | ≥ → Poor rating / red CF |
| ``anomaly_pct_fair`` | N, AF | ≥ → Fair rating / yellow CF (below poor) |
| ``champs_anomaly_pct_poor`` | W, AK | champs block (same pattern) |
| ``champs_anomaly_pct_fair`` | W, AK | champs block |
| ``element_marking_score_fair`` | O, X, AG, AL | element dev ≥ → Fair (lookup + sort) |
| ``element_marking_score_poor`` | O, X, AG, AL | element dev ≥ → Poor |
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReportActivityThresholds:
    total_comps_in_role_low: int
    competition_count_low: int
    segment_count_low: int
    junior_senior_segment_count_low: int
    qualifying_rule_errors_poor: int
    champs_rule_errors_poor: int
    anomaly_pct_poor: float
    anomaly_pct_fair: float
    champs_anomaly_pct_poor: float
    champs_anomaly_pct_fair: float
    element_marking_score_fair: float
    element_marking_score_poor: float


# Singles/Pairs — locked to the manual workbook.
SINGLES_PAIRS_THRESHOLDS = ReportActivityThresholds(
    total_comps_in_role_low=5,
    competition_count_low=3,
    segment_count_low=20,
    junior_senior_segment_count_low=10,
    qualifying_rule_errors_poor=4,
    champs_rule_errors_poor=3,
    anomaly_pct_poor=2.0,
    anomaly_pct_fair=1.0,
    champs_anomaly_pct_poor=2.0,
    champs_anomaly_pct_fair=1.0,
    element_marking_score_fair=1.0,
    element_marking_score_poor=1.3,
)

# Ice dance — tune ``# TODO`` fields after reviewing draft report.
ICE_DANCE_THRESHOLDS = ReportActivityThresholds(
    total_comps_in_role_low=3,
    competition_count_low=3,
    segment_count_low=10,
    junior_senior_segment_count_low=6,
    qualifying_rule_errors_poor=4,  # unused (no rule-error columns)
    champs_rule_errors_poor=3,  # unused
    anomaly_pct_poor=2.0,
    anomaly_pct_fair=1.1,
    champs_anomaly_pct_poor=2.0,
    champs_anomaly_pct_fair=1.1,
    element_marking_score_fair=1.0,
    element_marking_score_poor=1.3,
)

# Synchronized — tune independently from ice dance.
SYNCHRO_THRESHOLDS = ReportActivityThresholds(
    total_comps_in_role_low=3,  # TODO
    competition_count_low=3,  # TODO
    segment_count_low=10,  # TODO
    junior_senior_segment_count_low=3,  # TODO
    qualifying_rule_errors_poor=4,  # unused
    champs_rule_errors_poor=3,  # unused
    anomaly_pct_poor=2.5,  # TODO
    anomaly_pct_fair=1.5,  # TODO
    champs_anomaly_pct_poor=2.5,  # TODO
    champs_anomaly_pct_fair=1.5,  # TODO
    element_marking_score_fair=1.1,
    element_marking_score_poor=1.3,
)
