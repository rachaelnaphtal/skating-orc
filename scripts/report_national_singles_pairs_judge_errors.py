#!/usr/bin/env python3
"""
Report rule errors, excess anomalies, anomaly rate, segment counts, and PCS/element
deviation marking scores for national Singles/Pairs Competition Judges.

Includes all active National-level Competition Judges in the USFS directory appointed in
Singles, Pairs, or Singles/Pairs. Scores are limited to Singles and Pairs segments in
qualifying (US domestic national) competitions over the N USFS seasons completed
before a listing anchor (default 2627 → 2526, 2425, 2324).
International competitions (types 15–17) are excluded from that block. The same metrics
are also reported for US Championships (competition type 4) from the 2018–19 season
(2018-07-01 GOE cutoff) through the present. Element (GOE) anomalies and deviation rates
exclude competitions before 2018-07-01. Deviation marking scores use
the all-competitions sigma benchmark pool.

Leading columns mirror the activity tracker qualifying report: appointment year, last
championships in role, total competitions in role (three USFS seasons), and 2027 US
Championships senior availability when that form is loaded. Total comps in role counts
distinct competitions where the official served as Competition Judge on Singles and/or
Pairs protocol segments (same discipline filter as the qualifying report; all segment
levels, not Junior/Senior only).

Deviation marking scores are computed twice (all segment levels): once over qualifying
competitions (last N seasons) and once over all championships since the GOE era, both
using the all-competitions sigma benchmark pool over the same season span as each
ranking scope.

Example::

    python scripts/report_national_singles_pairs_judge_errors.py
    python scripts/report_national_singles_pairs_judge_errors.py -o "analysisTemp/National SP Judge Analysis.xlsx"
    python scripts/report_national_singles_pairs_judge_errors.py -o analysisTemp/report.csv
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select, text

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from activityAnalysis.international_listing_seasons import (  # noqa: E402
    REPORT_LISTING_SEASON_DEFAULT,
    season_codes_preceding_listing,
)
from activityAnalysis.load_activity_data import (  # noqa: E402
    DISC_PAIRS_ID,
    DISC_SINGLES_PAIRS_ID,
    NATIONAL_LEVEL_ID,
    SINGLES_DISCIPLINE_ID,
    calendar_years_for_usfs_season_codes,
    count_official_segment_competitions_batch,
    get_official_ids_with_isu_appointment,
    segment_discipline_type_ids_for_directory,
)
from activityAnalysis.qualifying_form_store import (  # noqa: E402
    QUALIFYING_COMPETITION_GROUP_SPD,
    _BUCKET_LABELS,
    _availability_bucket,
    _batch_official_in_role_appointment_year,
    _fetch_assignment_years_combined,
    response_json_is_complete,
    response_json_not_interested_all,
)
from scripts.national_sp_judge_analysis_xlsx import write_national_sp_judge_analysis_xlsx  # noqa: E402
from analytics import JudgeAnalytics  # noqa: E402
from cross_judge_cache import (  # noqa: E402
    _load_shard_judge_aggregates_sql,
    shard_cache_populated,
)
from database import get_db_session  # noqa: E402
from element_deviation_ranking import FLOOR_SIGMA as ELEMENT_FLOOR_SIGMA  # noqa: E402
from element_deviation_ranking import MIN_BIN_COUNT as ELEMENT_MIN_BIN_COUNT  # noqa: E402
from element_deviation_ranking import (  # noqa: E402
    MIN_ELEMENT_RANKING_SEASON_YEAR,
    finish_element_deviation_rankings_from_marks,
    load_element_marking_data,
)
from element_ranking_cache import run_element_deviation_ranking_pipeline  # noqa: E402
from judge_excess_cache import ensure_judge_excess_cache, aggregate_excess_from_cache  # noqa: E402
from models import (  # noqa: E402
    Competition,
    Element,
    ElementScorePerJudge,
    JudgeOfficialLink,
    PcsScorePerJudge,
    Segment,
    SkaterSegment,
)
from officials_competition_types import (  # noqa: E402
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_QUALIFYING,
)
from rule_errors_policy import MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS  # noqa: E402
from pcs_deviation_analysis import FLOOR_SIGMA as PCS_FLOOR_SIGMA  # noqa: E402
from pcs_deviation_analysis import MIN_BIN_COUNT as PCS_MIN_BIN_COUNT  # noqa: E402
from pcs_deviation_analysis import (  # noqa: E402
    finish_pcs_deviation_rankings_from_marks,
    load_pcs_deviation_marks,
)
from pcs_deviation_cache import run_pcs_deviation_ranking_pipeline  # noqa: E402
from segment_level import LEVEL_JUNIOR, LEVEL_SENIOR  # noqa: E402

COMPETITION_JUDGE_APPOINTMENT_TYPE_ID = 1
SINGLES_PAIRS_DIRECTORY_DISCIPLINE_IDS = (
    SINGLES_DISCIPLINE_ID,
    DISC_PAIRS_ID,
    DISC_SINGLES_PAIRS_ID,
)
SEGMENT_DISCIPLINE_TYPE_IDS = (1, 2)  # public.discipline_type: Singles, Pairs
JUNIOR_SENIOR_LEVELS = frozenset({LEVEL_JUNIOR, LEVEL_SENIOR})
GOE_ELIGIBLE_FROM = MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS
DEFAULT_US_CHAMPS_AVAILABILITY_TITLE = (
    "2027 U.S. Figure Skating Championships (Senior level only)"
)
_JUDGE_DISCIPLINE_PRIORITY = (
    DISC_SINGLES_PAIRS_ID,
    SINGLES_DISCIPLINE_ID,
    DISC_PAIRS_ID,
)


def _season_years_for_listing(listing_season: int, n: int) -> list[str]:
    """USFS season codes (newest first) for the N seasons before a listing anchor."""
    codes = season_codes_preceding_listing(int(listing_season), int(n))
    return [str(c) for c in sorted(codes, reverse=True)]


def _competition_ids_for_seasons(
    analytics: JudgeAnalytics,
    seasons: list[str],
    *,
    competition_scope: str = COMPETITION_SCOPE_QUALIFYING,
) -> list[int]:
    q = analytics.session.query(Competition.id).filter(
        Competition.year.in_(seasons)
    )
    q = analytics._filter_orm_competition_scope(q, competition_scope)
    return [int(r[0]) for r in q.all()]


def _championships_goe_era_scope(
    analytics: JudgeAnalytics,
) -> tuple[list[int], list[str]]:
    """
    Championships competitions from the GOE era onward (season >= 1819, event >= 2018-07-01).
    """
    years = sorted(
        str(y)
        for y in analytics.get_years()
        if str(y).strip() >= MIN_ELEMENT_RANKING_SEASON_YEAR
    )
    if not years:
        return [], []
    raw_ids = _competition_ids_for_seasons(
        analytics,
        years,
        competition_scope=COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    )
    comp_ids = sorted(_goe_eligible_competition_ids(analytics, raw_ids))
    comp_to_year = _competition_year_map(analytics, comp_ids)
    champs_seasons = sorted(
        {comp_to_year[cid] for cid in comp_ids if cid in comp_to_year}
    )
    return comp_ids, champs_seasons


def _goe_eligible_competition_ids(
    analytics: JudgeAnalytics, competition_ids: list[int]
) -> set[int]:
    """Competitions on or after the IJS GOE scale cutoff (2018-07-01)."""
    if not competition_ids:
        return set()
    date_map = analytics._event_dates_for_competition_ids(competition_ids)
    out: set[int] = set()
    for cid in competition_ids:
        ev = date_map.get(int(cid))
        if ev is None or ev >= GOE_ELIGIBLE_FROM:
            out.add(int(cid))
    return out


def _official_primary_judge_discipline_ids(session, official_ids: list[int]) -> dict[int, int]:
    """Best directory discipline id per official for in-role activity columns."""
    if not official_ids:
        return {}
    rows = session.execute(
        text(
            """
            SELECT a.official_id, a.discipline_id
            FROM officials_analysis.appointments a
            WHERE a.official_id = ANY(:official_ids)
              AND a.active IS TRUE
              AND a.appointment_type_id = :appt_type_id
              AND a.level_id = :national_level_id
              AND a.discipline_id = ANY(:discipline_ids)
            """
        ),
        {
            "official_ids": [int(x) for x in official_ids],
            "appt_type_id": COMPETITION_JUDGE_APPOINTMENT_TYPE_ID,
            "national_level_id": NATIONAL_LEVEL_ID,
            "discipline_ids": list(SINGLES_PAIRS_DIRECTORY_DISCIPLINE_IDS),
        },
    ).all()
    by_official: dict[int, set[int]] = defaultdict(set)
    for oid, did in rows:
        by_official[int(oid)].add(int(did))
    out: dict[int, int] = {}
    for oid, disc_ids in by_official.items():
        for preferred in _JUDGE_DISCIPLINE_PRIORITY:
            if preferred in disc_ids:
                out[oid] = preferred
                break
        else:
            out[oid] = min(disc_ids)
    return out


def _international_judge_official_ids(
    official_ids: list[int],
    *,
    discipline_by_official: dict[int, int],
) -> set[int]:
    """Officials with an active International Judge appointment in Singles/Pairs."""
    if not official_ids:
        return set()
    by_discipline: dict[int, list[int]] = defaultdict(list)
    for oid in official_ids:
        disc_id = discipline_by_official.get(int(oid))
        if disc_id is not None:
            by_discipline[int(disc_id)].append(int(oid))
    isu_ids: set[int] = set()
    for disc_id, oids in by_discipline.items():
        isu_ids |= get_official_ids_with_isu_appointment(
            oids,
            COMPETITION_JUDGE_APPOINTMENT_TYPE_ID,
            disc_id,
            active_appointments_only=True,
        )
    return isu_ids


def _resolve_qualifying_champs_competition(
    session, *, title_substring: str
) -> tuple[int, int] | None:
    """Return ``(form_id, competition_id)`` for a qualifying availability competition title."""
    needle = (title_substring or "").strip()
    if not needle:
        return None
    row = session.execute(
        text(
            """
            SELECT c.form_id, c.id
            FROM officials_analysis.qualifying_availability_competition c
            JOIN officials_analysis.qualifying_availability_form f ON f.id = c.form_id
            WHERE c.title ILIKE :pattern
            ORDER BY f.loaded_at DESC NULLS LAST, c.sort_order
            LIMIT 1
            """
        ),
        {"pattern": f"%{needle}%"},
    ).first()
    if row is None:
        return None
    return int(row[0]), int(row[1])


def _batch_qualifying_availability_status(
    session,
    *,
    form_id: int,
    competition_id: int,
    official_ids: list[int],
) -> dict[int, str | None]:
    """Map official id → Available / Unavailable / Didn't reply (qualifying report labels)."""
    out: dict[int, str | None] = {int(oid): None for oid in official_ids}
    if not official_ids:
        return out
    rows = session.execute(
        text(
            """
            SELECT
                o.id AS official_id,
                r.id AS form_response_id,
                r.response_json,
                a.availability_code
            FROM officials_analysis.officials o
            LEFT JOIN officials_analysis.qualifying_official_form_response r
              ON r.official_id = o.id AND r.form_id = :form_id
            LEFT JOIN officials_analysis.qualifying_official_competition_availability a
              ON a.official_id = o.id
             AND a.form_id = :form_id
             AND a.competition_id = :competition_id
            WHERE o.id = ANY(:official_ids)
            """
        ),
        {
            "form_id": int(form_id),
            "competition_id": int(competition_id),
            "official_ids": [int(x) for x in official_ids],
        },
    ).mappings().all()
    for row in rows:
        oid = int(row["official_id"])
        payload = row["response_json"] if row["form_response_id"] is not None else {}
        if not isinstance(payload, dict):
            payload = {}
        form_is_complete = (
            row["form_response_id"] is not None and response_json_is_complete(payload)
        )
        bucket = _availability_bucket(
            has_form_response=row["form_response_id"] is not None,
            form_is_complete=form_is_complete,
            not_interested_all=form_is_complete
            and response_json_not_interested_all(payload),
            availability_code=row["availability_code"],
        )
        if bucket is None:
            out[oid] = None
        else:
            out[oid] = _BUCKET_LABELS.get(bucket, bucket)
    return out


def _activity_tracker_columns(
    session,
    official_ids: list[int],
    *,
    discipline_by_official: dict[int, int],
    availability_title: str,
    season_year_codes: list[int],
) -> dict[int, dict[str, Any]]:
    """Qualifying-report-style activity columns keyed by official id."""
    out: dict[int, dict[str, Any]] = {
        int(oid): {
            "appointment_year": None,
            "last_champs_in_role": None,
            "total_comps_in_role_3yr": None,
            "us_champs_senior_availability": None,
        }
        for oid in official_ids
    }
    if not official_ids:
        return out

    by_discipline: dict[int, list[int]] = defaultdict(list)
    for oid in official_ids:
        did = discipline_by_official.get(int(oid))
        if did is not None:
            by_discipline[int(did)].append(int(oid))

    for disc_id, oids in by_discipline.items():
        appt_years = _batch_official_in_role_appointment_year(
            session,
            oids,
            COMPETITION_JUDGE_APPOINTMENT_TYPE_ID,
            disc_id,
        )
        _, in_role_years = _fetch_assignment_years_combined(
            session,
            oids,
            competition_group=QUALIFYING_COMPETITION_GROUP_SPD,
            in_role_appointment_type_id=COMPETITION_JUDGE_APPOINTMENT_TYPE_ID,
            in_role_discipline_id=disc_id,
        )
        seg_disc_ids = segment_discipline_type_ids_for_directory(
            disc_id, COMPETITION_JUDGE_APPOINTMENT_TYPE_ID
        )
        # Count distinct competitions where the official judged as Competition Judge on
        # Singles and/or Pairs segments (per directory discipline), not other disciplines.
        comp_counts = count_official_segment_competitions_batch(
            oids,
            season_year_codes=season_year_codes,
            appointment_type_id=COMPETITION_JUDGE_APPOINTMENT_TYPE_ID,
            segment_discipline_type_ids=seg_disc_ids,
        )
        for oid in oids:
            bucket = out[int(oid)]
            bucket["appointment_year"] = appt_years.get(int(oid))
            ir_champ, _ir_sect = in_role_years.get(int(oid), (None, None))
            bucket["last_champs_in_role"] = ir_champ
            bucket["total_comps_in_role_3yr"] = comp_counts.get(int(oid), 0)

    resolved = _resolve_qualifying_champs_competition(
        session, title_substring=availability_title
    )
    if resolved is not None:
        form_id, comp_id = resolved
        avail = _batch_qualifying_availability_status(
            session,
            form_id=form_id,
            competition_id=comp_id,
            official_ids=official_ids,
        )
        for oid, status in avail.items():
            out[int(oid)]["us_champs_senior_availability"] = status

    return out


def _national_singles_pairs_judges(session) -> pd.DataFrame:
    rows = session.execute(
        text(
            """
            SELECT DISTINCT
                o.id AS official_id,
                o.full_name,
                o.mbr_number,
                d.name AS directory_discipline
            FROM officials_analysis.appointments a
            JOIN officials_analysis.officials o ON o.id = a.official_id
            LEFT JOIN officials_analysis.disciplines d ON d.id = a.discipline_id
            WHERE a.active IS TRUE
              AND a.appointment_type_id = :appt_type_id
              AND a.level_id = :national_level_id
              AND a.discipline_id = ANY(:discipline_ids)
            """
        ),
        {
            "appt_type_id": COMPETITION_JUDGE_APPOINTMENT_TYPE_ID,
            "national_level_id": NATIONAL_LEVEL_ID,
            "discipline_ids": list(SINGLES_PAIRS_DIRECTORY_DISCIPLINE_IDS),
        },
    ).mappings().all()
    if not rows:
        return pd.DataFrame(
            columns=[
                "official_id",
                "full_name",
                "mbr_number",
                "directory_disciplines",
            ]
        )

    by_official: dict[int, dict] = {}
    for row in rows:
        oid = int(row["official_id"])
        bucket = by_official.setdefault(
            oid,
            {
                "official_id": oid,
                "full_name": (row["full_name"] or "").strip(),
                "mbr_number": (row["mbr_number"] or "").strip(),
                "directory_disciplines": set(),
            },
        )
        disc = (row["directory_discipline"] or "").strip()
        if disc:
            bucket["directory_disciplines"].add(disc)

    out = []
    for bucket in by_official.values():
        out.append(
            {
                "official_id": bucket["official_id"],
                "full_name": bucket["full_name"],
                "mbr_number": bucket["mbr_number"],
                "directory_disciplines": ", ".join(
                    sorted(bucket["directory_disciplines"], key=str.lower)
                ),
            }
        )
    return pd.DataFrame(out).sort_values("full_name", key=lambda s: s.str.lower())


def _official_id_to_identity_label(analytics: JudgeAnalytics) -> dict[int, str]:
    id_to_label = analytics.get_judge_id_to_identity_label()
    out: dict[int, str] = {}
    rows = (
        analytics.session.query(
            JudgeOfficialLink.official_id, JudgeOfficialLink.judge_id
        )
        .filter(JudgeOfficialLink.status == "linked")
        .filter(JudgeOfficialLink.official_id.isnot(None))
        .all()
    )
    for oid, jid in rows:
        label = id_to_label.get(int(jid))
        if label:
            out[int(oid)] = label
    return out


def _linked_official_ids(session) -> set[int]:
    rows = (
        session.query(JudgeOfficialLink.official_id)
        .filter(JudgeOfficialLink.status == "linked")
        .filter(JudgeOfficialLink.official_id.isnot(None))
        .all()
    )
    return {int(r[0]) for r in rows}


def _competition_year_map(
    analytics: JudgeAnalytics, competition_ids: list[int]
) -> dict[int, str]:
    if not competition_ids:
        return {}
    rows = (
        analytics.session.query(Competition.id, Competition.year)
        .filter(Competition.id.in_(competition_ids))
        .all()
    )
    return {int(cid): str(year) for cid, year in rows}


def _accumulate_shard_stats(
    pcs_merged: dict,
    elem_merged: dict,
    comp_to_year: dict[int, str],
    seasons: list[str],
    *,
    goe_eligible_comp_ids: set[int] | None = None,
) -> tuple[
    dict[str, dict[str, int]],
    dict[str, dict[str, dict[str, int]]],
]:
    """Roll up shard stats to identity labels (pooled + per season)."""
    totals: dict[str, dict[str, int]] = defaultdict(
        lambda: {"scores": 0, "anomalies": 0, "rule_errors": 0}
    )
    by_season: dict[str, dict[str, dict[str, int]]] = {
        season: defaultdict(lambda: {"scores": 0, "anomalies": 0, "rule_errors": 0})
        for season in seasons
    }

    def _elem_counts(comp_id: int) -> bool:
        if goe_eligible_comp_ids is None:
            return True
        return int(comp_id) in goe_eligible_comp_ids

    def _add(
        label: str,
        season: str | None,
        scores: int,
        anomalies: int,
        rule_errors: int,
    ):
        bucket = totals[label]
        bucket["scores"] += scores
        bucket["anomalies"] += anomalies
        bucket["rule_errors"] += rule_errors
        if season in by_season:
            sb = by_season[season][label]
            sb["scores"] += scores
            sb["anomalies"] += anomalies
            sb["rule_errors"] += rule_errors

    for (comp_id, label), stats in pcs_merged.items():
        season = comp_to_year.get(int(comp_id))
        scores = int(stats.get("total", 0))
        anomalies = int(stats.get("anomalies", 0))
        rule_errors = int(stats.get("rule_errors", 0))
        _add(label, season, scores, anomalies, rule_errors)
    for (comp_id, label), stats in elem_merged.items():
        if not _elem_counts(int(comp_id)):
            continue
        season = comp_to_year.get(int(comp_id))
        scores = int(stats.get("total", 0))
        anomalies = int(stats.get("anomalies", 0))
        rule_errors = int(stats.get("rule_errors", 0))
        _add(label, season, scores, anomalies, rule_errors)

    return (
        {label: dict(vals) for label, vals in totals.items()},
        {season: {label: dict(vals) for label, vals in season_map.items()}
         for season, season_map in by_season.items()},
    )


def _merge_heatmap_rule_error_totals(
    totals_by_label: dict[str, dict[str, int]],
    heatmap_df: pd.DataFrame | None,
) -> None:
    if heatmap_df is None or heatmap_df.empty:
        return
    for _, row in heatmap_df.iterrows():
        label = str(row["judge_name"])
        bucket = totals_by_label.setdefault(
            label, {"scores": 0, "anomalies": 0, "rule_errors": 0}
        )
        bucket["rule_errors"] += int(row.get("metric_value", 0) or 0)
        bucket["scores"] += int(row.get("total_scores", 0) or 0)


def _merge_heatmap_anomaly_totals(
    totals_by_label: dict[str, dict[str, int]],
    heatmap_df: pd.DataFrame | None,
) -> None:
    if heatmap_df is None or heatmap_df.empty:
        return
    for _, row in heatmap_df.iterrows():
        label = str(row["judge_name"])
        bucket = totals_by_label.setdefault(
            label, {"scores": 0, "anomalies": 0, "rule_errors": 0}
        )
        scores = int(row.get("total_scores", 0) or 0)
        rate = float(row.get("metric_value", 0) or 0)
        bucket["scores"] += scores
        bucket["anomalies"] += int(round(scores * rate / 100)) if scores else 0


def _fallback_goe_aware_totals(
    analytics: JudgeAnalytics,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
    competition_scope: str,
) -> dict[str, dict[str, int]]:
    common = dict(
        competition_ids=competition_ids,
        discipline_type_ids=seg_discipline_ids,
        competition_scope=competition_scope,
    )
    totals_by_label: dict[str, dict[str, int]] = {}
    _merge_heatmap_rule_error_totals(
        totals_by_label,
        analytics.get_judge_performance_heatmap_data(
            metric="rule_errors", score_type="pcs", **common
        ),
    )
    _merge_heatmap_rule_error_totals(
        totals_by_label,
        analytics.get_judge_performance_heatmap_data(
            metric="rule_errors",
            score_type="element",
            event_start_date=GOE_ELIGIBLE_FROM,
            **common,
        ),
    )
    _merge_heatmap_anomaly_totals(
        totals_by_label,
        analytics.get_judge_performance_heatmap_data(
            metric="anomaly_rate", score_type="pcs", **common
        ),
    )
    _merge_heatmap_anomaly_totals(
        totals_by_label,
        analytics.get_judge_performance_heatmap_data(
            metric="anomaly_rate",
            score_type="element",
            event_start_date=GOE_ELIGIBLE_FROM,
            **common,
        ),
    )
    return totals_by_label


def _anomaly_rate_pct(anomalies: int, scores: int) -> float | None:
    if scores <= 0:
        return None
    return round((anomalies / scores) * 100, 2)


def _metrics_for_competitions(
    analytics: JudgeAnalytics,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
    seasons: list[str],
    competition_scope: str = COMPETITION_SCOPE_QUALIFYING,
    label: str = "qualifying",
) -> dict[str, dict]:
    """
    Return pooled and per-season judge metrics keyed by identity label.

    Keys in each stats dict: scores, anomalies, rule_errors, excess_anomalies.
    Element (GOE) counts exclude competitions before 2018-07-01.
    """
    empty = {
        "totals": {},
        "by_season": {season: {} for season in seasons},
    }
    if not competition_ids:
        return empty

    judge_id_to_label = analytics.get_judge_id_to_identity_label()
    comp_to_year = _competition_year_map(analytics, competition_ids)
    goe_eligible = _goe_eligible_competition_ids(analytics, competition_ids)

    totals_by_label: dict[str, dict[str, int]] = {}
    season_by_label: dict[str, dict[str, dict[str, int]]] = {
        season: {} for season in seasons
    }

    if shard_cache_populated(analytics.session):
        pcs_raw, elem_raw = _load_shard_judge_aggregates_sql(
            analytics.session,
            competition_ids,
            seg_discipline_ids,
            by_competition=True,
        )
        pcs_merged = analytics._merge_competition_judge_stat_dicts_by_identity(
            pcs_raw, judge_id_to_label
        )
        elem_merged = analytics._merge_competition_judge_stat_dicts_by_identity(
            elem_raw, judge_id_to_label
        )
        totals_by_label, season_by_label = _accumulate_shard_stats(
            pcs_merged,
            elem_merged,
            comp_to_year,
            seasons,
            goe_eligible_comp_ids=goe_eligible,
        )
    else:
        totals_by_label = _fallback_goe_aware_totals(
            analytics,
            competition_ids=competition_ids,
            seg_discipline_ids=seg_discipline_ids,
            competition_scope=competition_scope,
        )

    print(
        f"Computing excess anomalies for {len(competition_ids)} {label} competitions..."
    )
    segment_ids = analytics._segment_ids_for_excess_scope(
        competition_ids=competition_ids,
        discipline_ids=seg_discipline_ids,
        competition_scope=competition_scope,
    )
    goe_segment_ids = analytics._segment_ids_for_excess_scope(
        competition_ids=sorted(goe_eligible),
        discipline_ids=seg_discipline_ids,
        competition_scope=competition_scope,
    )
    print(f"  {len(segment_ids)} singles/pairs segments in scope")
    if segment_ids:
        ensure_judge_excess_cache(analytics.session, segment_ids, "pcs")
        if goe_segment_ids:
            ensure_judge_excess_cache(analytics.session, goe_segment_ids, "element")
        pcs_excess_raw = aggregate_excess_from_cache(
            analytics.session, segment_ids, "pcs", by_competition=True
        )
        elem_excess_raw = (
            aggregate_excess_from_cache(
                analytics.session, goe_segment_ids, "element", by_competition=True
            )
            if goe_segment_ids
            else {}
        )
        excess_raw: dict[tuple[int, int], int] = defaultdict(int)
        for key, val in pcs_excess_raw.items():
            excess_raw[key] += int(val or 0)
        for key, val in elem_excess_raw.items():
            excess_raw[key] += int(val or 0)
        excess_merged = analytics._merge_excess_map_by_identity(
            excess_raw, judge_id_to_label, by_competition=True
        )
        for (label_name, comp_id), val in excess_merged.items():
            season = comp_to_year.get(int(comp_id))
            exc = int(val or 0)
            bucket = totals_by_label.setdefault(
                label_name, {"scores": 0, "anomalies": 0, "rule_errors": 0}
            )
            bucket["excess_anomalies"] = bucket.get("excess_anomalies", 0) + exc
            if season in season_by_label:
                sb = season_by_label[season].setdefault(
                    label_name, {"scores": 0, "anomalies": 0, "rule_errors": 0}
                )
                sb["excess_anomalies"] = sb.get("excess_anomalies", 0) + exc

    return {"totals": totals_by_label, "by_season": season_by_label}


def _segment_counts_by_label(
    analytics: JudgeAnalytics,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
) -> dict[str, dict[str, int]]:
    """Distinct competitions and segments judged per identity label."""
    if not competition_ids:
        return {}

    judge_id_to_label = analytics.get_judge_id_to_identity_label()
    by_judge: dict[int, set[tuple[int, int, str | None]]] = defaultdict(set)

    pcs_rows = analytics.session.execute(
        select(
            PcsScorePerJudge.judge_id,
            Segment.id,
            Segment.competition_id,
            Segment.level,
        )
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .where(Segment.competition_id.in_(competition_ids))
        .where(Segment.discipline_type_id.in_(seg_discipline_ids))
        .distinct()
    ).all()
    for judge_id, segment_id, competition_id, level in pcs_rows:
        by_judge[int(judge_id)].add(
            (int(competition_id), int(segment_id), (level or "").strip() or None)
        )

    elem_rows = analytics.session.execute(
        select(
            ElementScorePerJudge.judge_id,
            Segment.id,
            Segment.competition_id,
            Segment.level,
        )
        .join(Element, ElementScorePerJudge.element_id == Element.id)
        .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .where(Segment.competition_id.in_(competition_ids))
        .where(Segment.discipline_type_id.in_(seg_discipline_ids))
        .distinct()
    ).all()
    for judge_id, segment_id, competition_id, level in elem_rows:
        by_judge[int(judge_id)].add(
            (int(competition_id), int(segment_id), (level or "").strip() or None)
        )

    by_label: dict[str, dict[str, set]] = defaultdict(
        lambda: {"competitions": set(), "all": set(), "junior_senior": set()}
    )
    for judge_id, entries in by_judge.items():
        label = judge_id_to_label.get(judge_id)
        if not label:
            continue
        for comp_id, seg_id, level in entries:
            by_label[label]["competitions"].add(comp_id)
            by_label[label]["all"].add(seg_id)
            if level in JUNIOR_SENIOR_LEVELS:
                by_label[label]["junior_senior"].add(seg_id)

    return {
        label: {
            "competition_count": len(buckets["competitions"]),
            "segment_count": len(buckets["all"]),
            "junior_senior_segment_count": len(buckets["junior_senior"]),
        }
        for label, buckets in by_label.items()
    }


def _marking_scores_by_label(ranking_result: dict) -> dict[str, float]:
    judge_summary = ranking_result.get("judge_summary_all")
    if not isinstance(judge_summary, pd.DataFrame) or judge_summary.empty:
        return {}
    if "marking_score" not in judge_summary.columns:
        return {}
    out: dict[str, float] = {}
    for row in judge_summary.itertuples(index=False):
        name = str(row.judge_name)
        out[name] = round(float(row.marking_score), 4)
    return out


def _deviation_marking_scores_for_scope(
    analytics: JudgeAnalytics,
    seasons: list[str],
    *,
    competition_scope: str,
    scope_label: str,
) -> tuple[dict[str, float], dict[str, float]]:
    """PCS and element deviation marking scores for one ranking scope (all segment levels)."""
    if not seasons:
        return {}, {}
    start_season = min(seasons)
    end_season = max(seasons)
    rank_kw = dict(
        start_season_year=start_season,
        end_season_year=end_season,
        discipline_type_ids=list(SEGMENT_DISCIPLINE_TYPE_IDS),
        competition_scope=competition_scope,
    )
    # Benchmark σ̂ uses the all-competitions pool; ranking competition scope varies per set.
    benchmark_kw = dict(
        benchmark_start_season_year=start_season,
        benchmark_end_season_year=end_season,
        benchmark_competition_scope_key=COMPETITION_SCOPE_ALL,
    )
    base_scope = {
        **rank_kw,
        **benchmark_kw,
        "min_marks": 0,
        "persist_shards": False,
    }
    elem_scope = {
        **base_scope,
        "event_start_date": GOE_ELIGIBLE_FROM,
    }

    print(
        f"Loading PCS deviation marking scores ({scope_label}; "
        "all segment levels; benchmark pool: all competitions)..."
    )
    pcs_result = run_pcs_deviation_ranking_pipeline(
        analytics,
        floor_sigma=PCS_FLOOR_SIGMA,
        min_bin_count=PCS_MIN_BIN_COUNT,
        cache_only=True,
        **base_scope,
    )
    if pcs_result.get("error"):
        print(f"  PCS cache miss ({pcs_result['error']}); computing from marks...")
        pcs_marks = load_pcs_deviation_marks(analytics, **rank_kw)
        pcs_result = finish_pcs_deviation_rankings_from_marks(
            analytics,
            pcs_marks,
            min_marks=0,
            floor_sigma=PCS_FLOOR_SIGMA,
            min_bin_count=PCS_MIN_BIN_COUNT,
        )
    elif pcs_result.get("_from_summary_cache"):
        print("  PCS marking scores assembled from shard summary cache.")
    else:
        print("  PCS marking scores loaded from deviation shard cache.")

    print(
        f"Loading element deviation marking scores ({scope_label}; "
        f"GOE marks from {GOE_ELIGIBLE_FROM.isoformat()} onward)..."
    )
    elem_result = run_element_deviation_ranking_pipeline(
        analytics,
        floor_sigma=ELEMENT_FLOOR_SIGMA,
        min_bin_count=ELEMENT_MIN_BIN_COUNT,
        include_judge_detail=False,
        cache_only=True,
        **elem_scope,
    )
    if elem_result.get("error"):
        print(f"  Element cache miss ({elem_result['error']}); computing from marks...")
        elem_marks = load_element_marking_data(
            analytics.session,
            analytics,
            event_start_date=GOE_ELIGIBLE_FROM,
            **rank_kw,
        )
        elem_result = finish_element_deviation_rankings_from_marks(
            analytics,
            elem_marks,
            min_marks=0,
            floor_sigma=ELEMENT_FLOOR_SIGMA,
            min_bin_count=ELEMENT_MIN_BIN_COUNT,
            include_judge_detail=False,
        )
    elif elem_result.get("_from_summary_cache"):
        print("  Element marking scores assembled from shard summary cache.")
    else:
        print("  Element marking scores loaded from deviation shard cache.")

    return _marking_scores_by_label(pcs_result), _marking_scores_by_label(elem_result)


def _metric_columns(
    *,
    prefix: str,
    totals: dict[str, dict[str, int]],
    by_season: dict[str, dict[str, dict[str, int]]],
    seasons: list[str],
    label: str,
    has_label: bool,
    seg: dict[str, int],
    pcs_marking: dict[str, float],
    element_marking: dict[str, float],
    include_marking_scores: bool,
) -> dict[str, Any]:
    t = totals.get(label, {}) if has_label else {}
    row: dict[str, Any] = {}
    if include_marking_scores:
        row[f"{prefix}pcs_marking_score"] = pcs_marking.get(label) if has_label else None
        row[f"{prefix}element_marking_score"] = (
            element_marking.get(label) if has_label else None
        )
    row[f"{prefix}competition_count"] = (
        int(seg.get("competition_count", 0)) if has_label else None
    )
    row[f"{prefix}segment_count"] = (
        int(seg.get("segment_count", 0)) if has_label else None
    )
    row[f"{prefix}junior_senior_segment_count"] = (
        int(seg.get("junior_senior_segment_count", 0)) if has_label else None
    )
    row[f"{prefix}total_scores"] = int(t.get("scores", 0)) if has_label else None
    row[f"{prefix}total_rule_errors"] = (
        int(t.get("rule_errors", 0)) if has_label else None
    )
    row[f"{prefix}total_excess_anomalies"] = (
        int(t.get("excess_anomalies", 0)) if has_label else None
    )
    row[f"{prefix}anomaly_rate_pct"] = (
        _anomaly_rate_pct(int(t.get("anomalies", 0)), int(t.get("scores", 0)))
        if has_label
        else None
    )
    for season in seasons:
        s = by_season.get(season, {}).get(label, {}) if has_label else {}
        row[f"{prefix}scores_{season}"] = int(s.get("scores", 0)) if has_label else None
        row[f"{prefix}rule_errors_{season}"] = (
            int(s.get("rule_errors", 0)) if has_label else None
        )
        row[f"{prefix}excess_anomalies_{season}"] = (
            int(s.get("excess_anomalies", 0)) if has_label else None
        )
        row[f"{prefix}anomaly_rate_pct_{season}"] = (
            _anomaly_rate_pct(int(s.get("anomalies", 0)), int(s.get("scores", 0)))
            if has_label
            else None
        )
    return row


def build_report(
    analytics: JudgeAnalytics,
    *,
    listing_season: int = REPORT_LISTING_SEASON_DEFAULT,
    n_seasons: int = 3,
    availability_title: str = DEFAULT_US_CHAMPS_AVAILABILITY_TITLE,
) -> pd.DataFrame:
    seasons = _season_years_for_listing(listing_season, n_seasons)
    if not seasons:
        raise RuntimeError(
            f"No season window for listing {listing_season} with --seasons {n_seasons}."
        )
    season_year_codes = season_codes_preceding_listing(int(listing_season), int(n_seasons))

    roster = _national_singles_pairs_judges(analytics.session)
    official_ids = [int(x) for x in roster["official_id"].tolist()]
    discipline_by_official = _official_primary_judge_discipline_ids(
        analytics.session, official_ids
    )
    international_judge_ids = _international_judge_official_ids(
        official_ids,
        discipline_by_official=discipline_by_official,
    )
    activity_cols = _activity_tracker_columns(
        analytics.session,
        official_ids,
        discipline_by_official=discipline_by_official,
        availability_title=availability_title,
        season_year_codes=season_year_codes,
    )

    official_to_label = _official_id_to_identity_label(analytics)
    linked_officials = _linked_official_ids(analytics.session)
    all_comp_ids = _competition_ids_for_seasons(
        analytics, seasons, competition_scope=COMPETITION_SCOPE_QUALIFYING
    )
    champs_comp_ids, champs_seasons = _championships_goe_era_scope(analytics)
    print(
        f"Listing season {listing_season}: qualifying window "
        f"{', '.join(seasons)} ({len(all_comp_ids)} qualifying competitions)"
    )
    print(
        f"  US Championships (GOE era): {len(champs_comp_ids)} competitions across "
        f"seasons {', '.join(champs_seasons) or '(none)'}"
    )
    cal_years = calendar_years_for_usfs_season_codes(season_year_codes)
    if cal_years:
        print(
            "Activity total comps in role: seasons "
            f"{', '.join(str(c) for c in sorted(season_year_codes, reverse=True))} "
            f"(calendar years {', '.join(str(y) for y in cal_years)})"
        )

    metrics = _metrics_for_competitions(
        analytics,
        competition_ids=all_comp_ids,
        seg_discipline_ids=list(SEGMENT_DISCIPLINE_TYPE_IDS),
        seasons=seasons,
        competition_scope=COMPETITION_SCOPE_QUALIFYING,
        label="qualifying",
    )
    champs_metrics = _metrics_for_competitions(
        analytics,
        competition_ids=champs_comp_ids,
        seg_discipline_ids=list(SEGMENT_DISCIPLINE_TYPE_IDS),
        seasons=champs_seasons,
        competition_scope=COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
        label="championships",
    )
    totals = metrics["totals"]
    by_season = metrics["by_season"]
    champs_totals = champs_metrics["totals"]
    champs_by_season = champs_metrics["by_season"]

    print("Counting segments judged...")
    segment_counts = _segment_counts_by_label(
        analytics,
        competition_ids=all_comp_ids,
        seg_discipline_ids=list(SEGMENT_DISCIPLINE_TYPE_IDS),
    )
    champs_segment_counts = _segment_counts_by_label(
        analytics,
        competition_ids=champs_comp_ids,
        seg_discipline_ids=list(SEGMENT_DISCIPLINE_TYPE_IDS),
    )
    pcs_marking, element_marking = _deviation_marking_scores_for_scope(
        analytics,
        seasons,
        competition_scope=COMPETITION_SCOPE_QUALIFYING,
        scope_label="qualifying",
    )
    if champs_seasons:
        champs_pcs_marking, champs_element_marking = _deviation_marking_scores_for_scope(
            analytics,
            champs_seasons,
            competition_scope=COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
            scope_label="championships (GOE era)",
        )
    else:
        champs_pcs_marking, champs_element_marking = {}, {}

    rows = []
    for _, off in roster.iterrows():
        oid = int(off["official_id"])
        label = official_to_label.get(oid) or ""
        has_label = bool(label)
        act = activity_cols.get(oid, {})
        row = {
            "appointment_year": act.get("appointment_year"),
            "last_champs_in_role": act.get("last_champs_in_role"),
            "total_comps_in_role_3yr": act.get("total_comps_in_role_3yr"),
            "us_champs_senior_availability": act.get("us_champs_senior_availability"),
            "international_judge": oid in international_judge_ids,
            "official_id": oid,
            "directory_name": off["full_name"],
            "mbr_number": off["mbr_number"],
            "directory_disciplines": off["directory_disciplines"],
            "linked_to_scoring_judge": oid in linked_officials,
            "judge_identity_label": label or None,
            "seasons_included": ", ".join(seasons),
            "champs_seasons_included": ", ".join(champs_seasons),
        }
        row.update(
            _metric_columns(
                prefix="",
                totals=totals,
                by_season=by_season,
                seasons=seasons,
                label=label,
                has_label=has_label,
                seg=segment_counts.get(label, {}) if has_label else {},
                pcs_marking=pcs_marking,
                element_marking=element_marking,
                include_marking_scores=True,
            )
        )
        row.update(
            _metric_columns(
                prefix="champs_",
                totals=champs_totals,
                by_season=champs_by_season,
                seasons=champs_seasons,
                label=label,
                has_label=has_label,
                seg=champs_segment_counts.get(label, {}) if has_label else {},
                pcs_marking=champs_pcs_marking,
                element_marking=champs_element_marking,
                include_marking_scores=True,
            )
        )
        rows.append(row)

    df = pd.DataFrame(rows)
    df = df.sort_values(
        ["total_excess_anomalies", "total_rule_errors", "directory_name"],
        ascending=[False, False, True],
        na_position="last",
    )
    return df


def main() -> None:
    default_xlsx = _REPO / "analysisTemp" / "National SP Judge Analysis.xlsx"
    parser = argparse.ArgumentParser(
        description=(
            "Report rule errors, excess anomalies, anomaly rate, segment counts, "
            "and PCS/element marking scores for national Singles/Pairs Competition Judges."
        )
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=default_xlsx,
        help=(
            "Output path (.xlsx for formatted workbook with analysis/raw/lookup sheets, "
            ".csv for raw tabular export)."
        ),
    )
    parser.add_argument(
        "--raw-csv",
        type=Path,
        default=None,
        help="When writing .xlsx, also write the raw tabular CSV to this path.",
    )
    parser.add_argument(
        "--listing-season",
        type=int,
        default=REPORT_LISTING_SEASON_DEFAULT,
        help=(
            "USFS listing anchor season code (default: %(default)s). Qualifying stats "
            "and total comps in role use the completed seasons before this anchor."
        ),
    )
    parser.add_argument(
        "--seasons",
        type=int,
        default=3,
        help=(
            "Number of USFS seasons before --listing-season to include "
            "(default: 3, e.g. 2627 → 2526, 2425, 2324)."
        ),
    )
    parser.add_argument(
        "--availability-competition-title",
        default=DEFAULT_US_CHAMPS_AVAILABILITY_TITLE,
        help=(
            "Substring match for the qualifying availability competition title "
            f"(default: {DEFAULT_US_CHAMPS_AVAILABILITY_TITLE!r})."
        ),
    )
    args = parser.parse_args()

    with get_db_session() as session:
        analytics = JudgeAnalytics(session)
        df = build_report(
            analytics,
            listing_season=args.listing_season,
            n_seasons=args.seasons,
            availability_title=args.availability_competition_title,
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.suffix.lower() == ".csv":
        df.to_csv(args.output, index=False)
        print(f"Wrote {len(df)} judges to {args.output}")
    else:
        write_national_sp_judge_analysis_xlsx(df, args.output)
        print(f"Wrote formatted workbook ({len(df)} judges) to {args.output}")
        if args.raw_csv is not None:
            args.raw_csv.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(args.raw_csv, index=False)
            print(f"Wrote raw CSV to {args.raw_csv}")
    if not df.empty and "seasons_included" in df.columns:
        print(f"Seasons: {df['seasons_included'].iloc[0]}")


if __name__ == "__main__":
    main()
