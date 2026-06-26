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
championships in role, total competitions in role (two USFS seasons), and 2027 US
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
    python scripts/report_national_singles_pairs_judge_errors.py --discipline ice_dance
    python scripts/report_national_singles_pairs_judge_errors.py --discipline synchro
    python scripts/report_national_singles_pairs_judge_errors.py -o "analysisTemp/National SP Judge Analysis.xlsx"
    python scripts/report_national_singles_pairs_judge_errors.py -o analysisTemp/report.csv
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import case, func, or_, select, text

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from activityAnalysis.international_listing_seasons import (  # noqa: E402
    REPORT_LISTING_SEASON_DEFAULT,
    season_codes_preceding_listing,
)
from activityAnalysis.load_activity_data import (  # noqa: E402
    DISC_DANCE_ID,
    DISC_PAIRS_ID,
    DISC_SINGLES_PAIRS_ID,
    DISC_SYNCHRO_ID,
    NATIONAL_LEVEL_ID,
    NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,
    SINGLES_DISCIPLINE_ID,
    _nqs_synchronized_segment_discipline_type_id,
    calendar_years_for_usfs_season_codes,
    count_official_segment_competitions_batch,
    get_official_ids_with_isu_appointment,
    segment_discipline_type_ids_for_directory,
)
from activityAnalysis.qualifying_form_store import (  # noqa: E402
    QUALIFYING_COMPETITION_GROUP_SPD,
    QUALIFYING_COMPETITION_GROUP_SYNCHRO,
    _BUCKET_LABELS,
    _availability_bucket,
    _batch_official_in_role_appointment_year,
    _fetch_assignment_years_combined,
    response_json_is_complete,
    response_json_not_interested_all,
)
from scripts.national_judge_report_thresholds import (  # noqa: E402
    ICE_DANCE_THRESHOLDS,
    SINGLES_PAIRS_THRESHOLDS,
    SYNCHRO_THRESHOLDS,
    ReportActivityThresholds,
)
from scripts.national_sp_judge_analysis_xlsx import write_national_sp_judge_analysis_xlsx  # noqa: E402
from analytics import JudgeAnalytics  # noqa: E402
from cross_judge_cache import (  # noqa: E402
    _load_shard_judge_aggregates_sql,
    shard_cache_populated,
)
from database import get_db_session  # noqa: E402
from database_loader import judge_person_match_key  # noqa: E402
from element_deviation_ranking import FLOOR_SIGMA as ELEMENT_FLOOR_SIGMA  # noqa: E402
from element_deviation_ranking import MIN_BIN_COUNT as ELEMENT_MIN_BIN_COUNT  # noqa: E402
from element_deviation_ranking import (  # noqa: E402
    ELEMENT_RANKING_LEVEL_FILTER_ALL,
    ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
    MIN_ELEMENT_RANKING_SEASON_YEAR,
    finish_element_deviation_rankings_from_marks,
    load_element_marking_data,
    segment_levels_for_ranking_preset,
)
from element_ranking_cache import run_element_deviation_ranking_pipeline  # noqa: E402
from judge_excess_cache import ensure_judge_excess_cache, aggregate_excess_from_cache  # noqa: E402
from models import (  # noqa: E402
    Competition,
    Element,
    ElementScorePerJudge,
    Judge,
    JudgeOfficialLink,
    PcsScorePerJudge,
    Segment,
    SkaterSegment,
)
from officials_competition_types import (  # noqa: E402
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_QUALIFYING,
    COMPETITION_SCOPE_SPD_SECTIONALS,
    COMPETITION_SCOPE_SYS_SECTIONALS,
    SPD_SECTIONAL_TYPE_IDS,
    SYS_SECTIONAL_TYPE_IDS,
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
# Synchro: count jr/sr segments only when more than this many teams competed.
SYNCHRO_JUNIOR_SENIOR_MIN_TEAM_COUNT = 3
DEFAULT_JUNIOR_SENIOR_SEGMENT_COUNT_HEADER = "# Junior/Senior Segments"
SYNCHRO_JUNIOR_SENIOR_SEGMENT_COUNT_HEADER = "# Jr/Sr Segments (>3 teams)"
DEFAULT_JUNIOR_SENIOR_ACTIVITY_LABEL = "Jr/Senior Activity"
SYNCHRO_JUNIOR_SENIOR_ACTIVITY_LABEL = "Jr/Sr Segments (>3 teams)"
# Activity Analysis columns (total comps in role, qualifying/js flags, sectionals recency).
ACTIVITY_WINDOW_SEASONS = 2
GOE_ELIGIBLE_FROM = MIN_COMPETITION_START_DATE_FOR_RULE_ERRORS
DEFAULT_US_CHAMPS_AVAILABILITY_TITLE = (
    "2027 U.S. Figure Skating Championships (Senior level only)"
)
_JUDGE_DISCIPLINE_PRIORITY = (
    DISC_SINGLES_PAIRS_ID,
    SINGLES_DISCIPLINE_ID,
    DISC_PAIRS_ID,
)
OFFICIALS_TYPE_US_CHAMPIONSHIPS = 4
OFFICIALS_TYPE_US_SYNCHRO_CHAMPIONSHIPS = 8
DEFAULT_SYNCHRO_CHAMPS_AVAILABILITY_TITLE = (
    "2027 U.S. Synchronized Skating Championships"
)
DISCIPLINE_CHOICES = ("singles_pairs", "ice_dance", "synchro")


@dataclass(frozen=True)
class ReportDisciplineConfig:
    key: str
    roster_label: str
    directory_discipline_ids: tuple[int, ...]
    segment_discipline_type_ids: tuple[int, ...]
    qualifying_competition_group: str
    championships_type_ids: tuple[int, ...]
    championships_label: str
    sectionals_type_ids: tuple[int, ...]
    sectionals_label: str
    sectionals_competition_scope: str
    discipline_priority: tuple[int, ...]
    include_rule_errors: bool
    default_availability_title: str
    default_output_filename: str
    # Primary (last N seasons) performance block — qualifying for SP, all activity for dance/synchro.
    activity_competition_scope: str
    activity_scope_label: str
    performance_block_header: str
    activity_column_label: str
    performance_analysis_header: str
    recent_period_header: str
    thresholds: ReportActivityThresholds
    # When set, jr/sr segment counts exclude segments with team_count <= this value.
    junior_senior_min_team_count: int | None = None
    junior_senior_segment_count_header: str = DEFAULT_JUNIOR_SENIOR_SEGMENT_COUNT_HEADER
    junior_senior_activity_label: str = DEFAULT_JUNIOR_SENIOR_ACTIVITY_LABEL
    # Champs performance block: optional jr/sr-only metrics (synchro has extra levels).
    champs_performance_segment_levels: frozenset[str] | None = None
    champs_segment_level_preset: str | None = None
    sectionals_performance_segment_levels: frozenset[str] | None = None
    sectionals_segment_level_preset: str | None = None


def _synchro_segment_discipline_type_ids() -> tuple[int, ...]:
    sid = _nqs_synchronized_segment_discipline_type_id()
    return (sid,) if sid is not None else ()


def _report_discipline_configs() -> dict[str, ReportDisciplineConfig]:
    return {
        "singles_pairs": ReportDisciplineConfig(
            key="singles_pairs",
            roster_label="Singles/Pairs",
            directory_discipline_ids=SINGLES_PAIRS_DIRECTORY_DISCIPLINE_IDS,
            segment_discipline_type_ids=SEGMENT_DISCIPLINE_TYPE_IDS,
            qualifying_competition_group=QUALIFYING_COMPETITION_GROUP_SPD,
            championships_type_ids=(OFFICIALS_TYPE_US_CHAMPIONSHIPS,),
            championships_label="US Championships",
            sectionals_type_ids=tuple(sorted(SPD_SECTIONAL_TYPE_IDS)),
            sectionals_label="Sectionals",
            sectionals_competition_scope=COMPETITION_SCOPE_SPD_SECTIONALS,
            discipline_priority=_JUDGE_DISCIPLINE_PRIORITY,
            include_rule_errors=True,
            default_availability_title=DEFAULT_US_CHAMPS_AVAILABILITY_TITLE,
            default_output_filename="National SP Judge Analysis.xlsx",
            activity_competition_scope=COMPETITION_SCOPE_QUALIFYING,
            activity_scope_label="qualifying",
            performance_block_header=(
                "Qualifying Competition Performance (Past three years)"
            ),
            activity_column_label="Qualifying Activity",
            performance_analysis_header="Qualifying Performance Analysis",
            recent_period_header="Last 3 years",
            thresholds=SINGLES_PAIRS_THRESHOLDS,
        ),
        "ice_dance": ReportDisciplineConfig(
            key="ice_dance",
            roster_label="Ice Dance",
            directory_discipline_ids=(DISC_DANCE_ID,),
            segment_discipline_type_ids=(NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,),
            qualifying_competition_group=QUALIFYING_COMPETITION_GROUP_SPD,
            championships_type_ids=(OFFICIALS_TYPE_US_CHAMPIONSHIPS,),
            championships_label="US Championships",
            sectionals_type_ids=tuple(sorted(SPD_SECTIONAL_TYPE_IDS)),
            sectionals_label="Sectionals",
            sectionals_competition_scope=COMPETITION_SCOPE_SPD_SECTIONALS,
            discipline_priority=(DISC_DANCE_ID,),
            include_rule_errors=False,
            default_availability_title=DEFAULT_US_CHAMPS_AVAILABILITY_TITLE,
            default_output_filename="National Ice Dance Judge Analysis.xlsx",
            activity_competition_scope=COMPETITION_SCOPE_ALL,
            activity_scope_label="all activity",
            performance_block_header=(
                "Competition Performance (Past three years, all activity)"
            ),
            activity_column_label="Competition Activity",
            performance_analysis_header="Performance Analysis",
            recent_period_header="Last 3 years (all activity)",
            thresholds=ICE_DANCE_THRESHOLDS,
        ),
        "synchro": ReportDisciplineConfig(
            key="synchro",
            roster_label="Synchronized",
            directory_discipline_ids=(DISC_SYNCHRO_ID,),
            segment_discipline_type_ids=_synchro_segment_discipline_type_ids(),
            qualifying_competition_group=QUALIFYING_COMPETITION_GROUP_SYNCHRO,
            championships_type_ids=(OFFICIALS_TYPE_US_SYNCHRO_CHAMPIONSHIPS,),
            championships_label="US Synchronized Skating Championships",
            sectionals_type_ids=tuple(sorted(SYS_SECTIONAL_TYPE_IDS)),
            sectionals_label="Synchronized Sectionals",
            sectionals_competition_scope=COMPETITION_SCOPE_SYS_SECTIONALS,
            discipline_priority=(DISC_SYNCHRO_ID,),
            include_rule_errors=False,
            default_availability_title=DEFAULT_SYNCHRO_CHAMPS_AVAILABILITY_TITLE,
            default_output_filename="National Synchro Judge Analysis.xlsx",
            activity_competition_scope=COMPETITION_SCOPE_ALL,
            activity_scope_label="all activity",
            performance_block_header=(
                "Competition Performance (Past three years, all activity)"
            ),
            activity_column_label="Competition Activity",
            performance_analysis_header="Performance Analysis",
            recent_period_header="Last 3 years (all activity)",
            thresholds=SYNCHRO_THRESHOLDS,
            junior_senior_min_team_count=SYNCHRO_JUNIOR_SENIOR_MIN_TEAM_COUNT,
            junior_senior_segment_count_header=SYNCHRO_JUNIOR_SENIOR_SEGMENT_COUNT_HEADER,
            junior_senior_activity_label=SYNCHRO_JUNIOR_SENIOR_ACTIVITY_LABEL,
            champs_performance_segment_levels=JUNIOR_SENIOR_LEVELS,
            champs_segment_level_preset=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
            sectionals_performance_segment_levels=JUNIOR_SENIOR_LEVELS,
            sectionals_segment_level_preset=ELEMENT_RANKING_LEVEL_FILTER_JUNIOR_SENIOR,
        ),
    }


REPORT_DISCIPLINE_CONFIGS = _report_discipline_configs()


def _season_years_for_listing(listing_season: int, n: int) -> list[str]:
    """USFS season codes (newest first) for the N seasons before a listing anchor."""
    codes = season_codes_preceding_listing(int(listing_season), int(n))
    return [str(c) for c in sorted(codes, reverse=True)]


def _competition_ids_for_seasons(
    analytics: JudgeAnalytics,
    seasons: list[str],
    *,
    competition_scope: str = COMPETITION_SCOPE_QUALIFYING,
    officials_competition_type_ids: tuple[int, ...] | None = None,
) -> list[int]:
    q = analytics.session.query(Competition.id).filter(
        Competition.year.in_(seasons)
    )
    q = analytics._filter_orm_competition_scope(q, competition_scope)
    if officials_competition_type_ids:
        q = q.filter(
            Competition.officials_analysis_competition_type_id.in_(
                officials_competition_type_ids
            )
        )
    return [int(r[0]) for r in q.all()]


def _goe_era_competition_scope(
    analytics: JudgeAnalytics,
    *,
    officials_competition_type_ids: tuple[int, ...],
) -> tuple[list[int], list[str]]:
    """Competitions of given officials type(s) from the GOE era onward."""
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
        competition_scope=COMPETITION_SCOPE_ALL,
        officials_competition_type_ids=officials_competition_type_ids,
    )
    comp_ids = sorted(_goe_eligible_competition_ids(analytics, raw_ids))
    comp_to_year = _competition_year_map(analytics, comp_ids)
    seasons = sorted(
        {comp_to_year[cid] for cid in comp_ids if cid in comp_to_year}
    )
    return comp_ids, seasons


def _championships_goe_era_scope(
    analytics: JudgeAnalytics,
    *,
    championships_type_ids: tuple[int, ...],
) -> tuple[list[int], list[str]]:
    """
    Championships competitions from the GOE era onward (season >= 1819, event >= 2018-07-01).
    """
    return _goe_era_competition_scope(
        analytics, officials_competition_type_ids=championships_type_ids
    )


def _sectionals_goe_era_scope(
    analytics: JudgeAnalytics,
    *,
    sectionals_type_ids: tuple[int, ...],
) -> tuple[list[int], list[str]]:
    """Sectional competitions for the discipline from the GOE era onward."""
    return _goe_era_competition_scope(
        analytics, officials_competition_type_ids=sectionals_type_ids
    )


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


def _official_primary_judge_discipline_ids(
    session,
    official_ids: list[int],
    *,
    config: ReportDisciplineConfig,
) -> dict[int, int]:
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
            "discipline_ids": list(config.directory_discipline_ids),
        },
    ).all()
    by_official: dict[int, set[int]] = defaultdict(set)
    for oid, did in rows:
        by_official[int(oid)].add(int(did))
    out: dict[int, int] = {}
    for oid, disc_ids in by_official.items():
        for preferred in config.discipline_priority:
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
    config: ReportDisciplineConfig,
    discipline_by_official: dict[int, int],
    availability_title: str,
    season_year_codes: list[int],
) -> dict[int, dict[str, Any]]:
    """Qualifying-report-style activity columns keyed by official id."""
    out: dict[int, dict[str, Any]] = {
        int(oid): {
            "appointment_year": None,
            "last_champs_in_role": None,
            "last_sectionals_in_role": None,
            "total_comps_in_role_2yr": None,
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
            competition_group=config.qualifying_competition_group,
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
            ir_champ, ir_sect = in_role_years.get(int(oid), (None, None))
            bucket["last_champs_in_role"] = ir_champ
            bucket["last_sectionals_in_role"] = ir_sect
            bucket["total_comps_in_role_2yr"] = comp_counts.get(int(oid), 0)

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


def _national_judges(session, *, config: ReportDisciplineConfig) -> pd.DataFrame:
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
            "discipline_ids": list(config.directory_discipline_ids),
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


def _judge_mark_score_totals(session, judge_ids: list[int]) -> dict[int, int]:
    """PCS + element score row counts per judge (for identity label tie-breaks)."""
    if not judge_ids:
        return {}
    rows = session.execute(
        text(
            """
            SELECT judge_id, COUNT(*)::bigint AS n
            FROM (
                SELECT judge_id FROM pcs_score_per_judge
                WHERE judge_id = ANY(:judge_ids)
                UNION ALL
                SELECT judge_id FROM element_score_per_judge
                WHERE judge_id = ANY(:judge_ids)
            ) s
            GROUP BY judge_id
            """
        ),
        {"judge_ids": [int(x) for x in judge_ids]},
    ).all()
    return {int(jid): int(n or 0) for jid, n in rows}


def _official_candidate_judge_ids(
    *,
    official_id: int,
    full_name: str,
    linked_jids_by_official: dict[int, set[int]],
    match_key_to_judge_ids: dict[str, set[int]],
    group_judge_ids_by_official: dict[int, set[int]],
) -> set[int]:
    """Scoring ``judge.id`` rows for one directory official (link + name match only)."""
    candidates: set[int] = set(linked_jids_by_official.get(int(official_id), ()))
    candidates |= group_judge_ids_by_official.get(int(official_id), set())
    mk = judge_person_match_key((full_name or "").strip())
    if mk:
        candidates |= match_key_to_judge_ids.get(mk, set())
    return candidates


def _official_id_to_identity_label(
    analytics: JudgeAnalytics,
    *,
    roster_official_ids: list[int] | None = None,
) -> dict[int, str]:
    """Primary identity label per official (first entry from :func:`_official_identity_labels`)."""
    all_labels = _official_identity_labels(
        analytics, roster_official_ids=roster_official_ids
    )
    return {oid: labels[0] for oid, labels in all_labels.items() if labels}


def _official_identity_labels(
    analytics: JudgeAnalytics,
    *,
    roster_official_ids: list[int] | None = None,
) -> dict[int, list[str]]:
    """
    Map directory officials to one or more scoring identity labels.

    Candidates: linked identity groups, ``judge_official_link``, and protocol name
    match keys. Multiple labels are returned when marks split across name variants;
    segment and anomaly metrics are de-duplicated across those judge ids.
    """
    id_to_label = analytics.get_judge_id_to_identity_label()
    out: dict[int, list[str]] = {}
    group_judge_ids_by_official: dict[int, set[int]] = defaultdict(set)

    for group in analytics.get_judge_analysis_identity_groups():
        oid = group.get("official_id")
        if oid is not None:
            oid = int(oid)
            group_judge_ids_by_official[oid] |= {int(j) for j in group["judge_ids"]}
            out[oid] = [str(group["label"])]

    linked_rows = (
        analytics.session.query(
            JudgeOfficialLink.official_id, JudgeOfficialLink.judge_id
        )
        .filter(JudgeOfficialLink.status == "linked")
        .filter(JudgeOfficialLink.official_id.isnot(None))
        .all()
    )
    linked_jids_by_official: dict[int, set[int]] = defaultdict(set)
    for oid, jid in linked_rows:
        linked_jids_by_official[int(oid)].add(int(jid))

    judge_rows = analytics.session.query(Judge.id, Judge.name).all()
    match_key_to_judge_ids: dict[str, set[int]] = defaultdict(set)
    for jid, name in judge_rows:
        mk = judge_person_match_key(name)
        if mk:
            match_key_to_judge_ids[mk].add(int(jid))

    official_ids = roster_official_ids
    if official_ids is None:
        official_ids = sorted(
            set(linked_jids_by_official.keys()) | set(group_judge_ids_by_official.keys())
        )
    name_rows = []
    if official_ids:
        name_rows = analytics.session.execute(
            text(
                """
                SELECT id, full_name
                FROM officials_analysis.officials
                WHERE id = ANY(:official_ids)
                """
            ),
            {"official_ids": [int(x) for x in official_ids]},
        ).all()

    score_totals = _judge_mark_score_totals(
        analytics.session,
        [int(jid) for jid, _ in judge_rows],
    )

    def _labels_for_judge_ids(candidate_jids: set[int]) -> list[str]:
        labels: dict[str, int] = {}
        for jid in candidate_jids:
            label = id_to_label.get(int(jid))
            if not label:
                continue
            labels[label] = labels.get(label, 0) + int(score_totals.get(int(jid), 0))
        if not labels:
            return []
        return [
            label
            for label, _ in sorted(
                labels.items(), key=lambda item: (-item[1], item[0].casefold())
            )
        ]

    for oid, full_name in name_rows:
        oid = int(oid)
        candidates = _official_candidate_judge_ids(
            official_id=oid,
            full_name=str(full_name or ""),
            linked_jids_by_official=linked_jids_by_official,
            match_key_to_judge_ids=match_key_to_judge_ids,
            group_judge_ids_by_official=group_judge_ids_by_official,
        )
        labels = _labels_for_judge_ids(candidates)
        if labels:
            out[oid] = labels

    return out


def _official_candidate_judge_ids_for_roster(
    analytics: JudgeAnalytics,
    roster: pd.DataFrame,
) -> dict[int, set[int]]:
    """Per-official scoring judge ids for the report roster."""
    linked_rows = (
        analytics.session.query(
            JudgeOfficialLink.official_id, JudgeOfficialLink.judge_id
        )
        .filter(JudgeOfficialLink.status == "linked")
        .filter(JudgeOfficialLink.official_id.isnot(None))
        .all()
    )
    linked_jids_by_official: dict[int, set[int]] = defaultdict(set)
    for oid, jid in linked_rows:
        linked_jids_by_official[int(oid)].add(int(jid))

    group_judge_ids_by_official: dict[int, set[int]] = defaultdict(set)
    for group in analytics.get_judge_analysis_identity_groups():
        oid = group.get("official_id")
        if oid is not None:
            group_judge_ids_by_official[int(oid)] |= {int(j) for j in group["judge_ids"]}

    judge_rows = analytics.session.query(Judge.id, Judge.name).all()
    match_key_to_judge_ids: dict[str, set[int]] = defaultdict(set)
    for jid, name in judge_rows:
        mk = judge_person_match_key(name)
        if mk:
            match_key_to_judge_ids[mk].add(int(jid))

    out: dict[int, set[int]] = {}
    for _, off in roster.iterrows():
        oid = int(off["official_id"])
        out[oid] = _official_candidate_judge_ids(
            official_id=oid,
            full_name=str(off.get("full_name") or ""),
            linked_jids_by_official=linked_jids_by_official,
            match_key_to_judge_ids=match_key_to_judge_ids,
            group_judge_ids_by_official=group_judge_ids_by_official,
        )
    return out


def _pool_judge_stats(
    totals: dict[str, dict[str, int]], labels: list[str]
) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for label in labels:
        bucket = totals.get(label, {})
        for key, val in bucket.items():
            out[key] += int(val or 0)
    return dict(out)


def _pool_season_stats(
    by_season: dict[str, dict[str, dict[str, int]]],
    labels: list[str],
    seasons: list[str],
) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for season in seasons:
        pooled: dict[str, int] = defaultdict(int)
        for label in labels:
            bucket = by_season.get(season, {}).get(label, {})
            for key, val in bucket.items():
                pooled[key] += int(val or 0)
        out[season] = dict(pooled)
    return out


def _segment_entries_by_judge(
    analytics: JudgeAnalytics,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
) -> dict[int, set[tuple[int, int, str | None]]]:
    """Distinct (competition_id, segment_id, level) per scoring judge id."""
    if not competition_ids:
        return {}
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
    return by_judge


def _segment_team_counts(
    session,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
) -> dict[int, int]:
    """Teams per segment (``skater_segment`` row count)."""
    if not competition_ids:
        return {}
    rows = session.execute(
        select(SkaterSegment.segment_id, func.count())
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .where(Segment.competition_id.in_(competition_ids))
        .where(Segment.discipline_type_id.in_(seg_discipline_ids))
        .group_by(SkaterSegment.segment_id)
    ).all()
    return {int(seg_id): int(cnt or 0) for seg_id, cnt in rows}


def _segment_counts_for_judge_ids(
    by_judge: dict[int, set[tuple[int, int, str | None]]],
    judge_ids: set[int] | list[int],
    *,
    competition_ids: set[int] | frozenset[int] | None = None,
    segment_team_counts: dict[int, int] | None = None,
    junior_senior_min_team_count: int | None = None,
) -> dict[str, int]:
    """Union segment/competition counts across judge ids (no double-counting)."""
    competitions: set[int] = set()
    all_segments: set[int] = set()
    junior_senior_segments: set[int] = set()
    comp_filter = set(competition_ids) if competition_ids is not None else None
    for jid in {int(x) for x in judge_ids}:
        for comp_id, seg_id, level in by_judge.get(jid, ()):
            if comp_filter is not None and comp_id not in comp_filter:
                continue
            competitions.add(comp_id)
            all_segments.add(seg_id)
            if level in JUNIOR_SENIOR_LEVELS:
                if junior_senior_min_team_count is not None:
                    teams = int((segment_team_counts or {}).get(seg_id, 0))
                    if teams <= junior_senior_min_team_count:
                        continue
                junior_senior_segments.add(seg_id)
    return {
        "competition_count": len(competitions),
        "segment_count": len(all_segments),
        "junior_senior_segment_count": len(junior_senior_segments),
    }


def _marking_score_for_labels(
    marking: dict[str, float], labels: list[str]
) -> float | None:
    for label in labels:
        val = marking.get(label)
        if val is not None:
            return float(val)
    return None


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


def _clear_rule_errors(stats: dict[str, dict[str, int]]) -> None:
    for bucket in stats.values():
        bucket["rule_errors"] = 0


def _fallback_goe_aware_totals(
    analytics: JudgeAnalytics,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
    competition_scope: str,
    include_rule_errors: bool,
) -> dict[str, dict[str, int]]:
    common = dict(
        competition_ids=competition_ids,
        discipline_type_ids=seg_discipline_ids,
        competition_scope=competition_scope,
    )
    totals_by_label: dict[str, dict[str, int]] = {}
    if include_rule_errors:
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


def _segment_ids_for_metrics_scope(
    analytics: JudgeAnalytics,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
    competition_scope: str,
    segment_levels: frozenset[str] | None = None,
) -> list[int]:
    segment_ids = analytics._segment_ids_for_excess_scope(
        competition_ids=competition_ids,
        discipline_ids=seg_discipline_ids,
        competition_scope=competition_scope,
    )
    if not segment_levels or not segment_ids:
        return segment_ids
    return [
        int(r)
        for r in analytics.session.execute(
            select(Segment.id)
            .where(Segment.id.in_(segment_ids))
            .where(Segment.level.in_(sorted(segment_levels)))
        )
        .scalars()
        .all()
    ]


def _judge_aggregates_from_segments(
    session,
    segment_ids: list[int],
    *,
    by_competition: bool,
    element_segment_ids: list[int] | None = None,
) -> tuple[dict[Any, dict[str, int]], dict[Any, dict[str, int]]]:
    """PCS/element score totals per judge (or per competition×judge) for segment ids."""
    if not segment_ids:
        return {}, {}

    pcs_group = [PcsScorePerJudge.judge_id]
    if by_competition:
        pcs_group = [Segment.competition_id, PcsScorePerJudge.judge_id]

    pcs_rows = session.execute(
        select(
            *pcs_group,
            func.count().label("total"),
            func.sum(
                case(
                    (
                        or_(
                            func.abs(PcsScorePerJudge.deviation) >= 1.5,
                            PcsScorePerJudge.is_rule_error,
                        ),
                        1,
                    ),
                    else_=0,
                )
            ).label("anomalies"),
            func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label(
                "rule_errors"
            ),
        )
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .where(SkaterSegment.segment_id.in_(segment_ids))
        .group_by(*pcs_group)
    ).all()

    pcs_raw: dict[Any, dict[str, int]] = {}
    for row in pcs_rows:
        if by_competition:
            key = (int(row[0]), int(row[1]))
            total = int(row[2] or 0)
            anomalies = int(row[3] or 0)
            rule_errors = int(row[4] or 0)
        else:
            key = int(row[0])
            total = int(row[1] or 0)
            anomalies = int(row[2] or 0)
            rule_errors = int(row[3] or 0)
        pcs_raw[key] = {
            "total": total,
            "anomalies": anomalies,
            "rule_errors": rule_errors,
        }

    elem_seg_ids = element_segment_ids if element_segment_ids is not None else segment_ids
    elem_raw: dict[Any, dict[str, int]] = {}
    if elem_seg_ids:
        elem_group = [ElementScorePerJudge.judge_id]
        if by_competition:
            elem_group = [Segment.competition_id, ElementScorePerJudge.judge_id]
        elem_rows = session.execute(
            select(
                *elem_group,
                func.count().label("total"),
                func.sum(
                    case(
                        (
                            or_(
                                func.abs(ElementScorePerJudge.deviation) >= 2,
                                ElementScorePerJudge.is_rule_error,
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("anomalies"),
                func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label(
                    "rule_errors"
                ),
            )
            .join(Element, ElementScorePerJudge.element_id == Element.id)
            .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
            .join(Segment, SkaterSegment.segment_id == Segment.id)
            .where(SkaterSegment.segment_id.in_(elem_seg_ids))
            .group_by(*elem_group)
        ).all()
        for row in elem_rows:
            if by_competition:
                key = (int(row[0]), int(row[1]))
                total = int(row[2] or 0)
                anomalies = int(row[3] or 0)
                rule_errors = int(row[4] or 0)
            else:
                key = int(row[0])
                total = int(row[1] or 0)
                anomalies = int(row[2] or 0)
                rule_errors = int(row[3] or 0)
            elem_raw[key] = {
                "total": total,
                "anomalies": anomalies,
                "rule_errors": rule_errors,
            }

    return pcs_raw, elem_raw


def _metrics_for_competitions(
    analytics: JudgeAnalytics,
    *,
    competition_ids: list[int],
    seg_discipline_ids: list[int],
    seasons: list[str],
    competition_scope: str = COMPETITION_SCOPE_QUALIFYING,
    label: str = "qualifying",
    discipline_label: str = "in-scope",
    include_rule_errors: bool = True,
    segment_levels: frozenset[str] | None = None,
) -> dict[str, dict]:
    """
    Return pooled and per-season judge metrics keyed by identity label.

    Keys in each stats dict: scores, anomalies, rule_errors, excess_anomalies.
    Element (GOE) counts exclude competitions before 2018-07-01.
    When ``segment_levels`` is set, PCS/element totals and excess anomalies are
    limited to segments at those levels (shard cache is not used).
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

    level_label = (
        f", levels: {', '.join(sorted(segment_levels))}"
        if segment_levels
        else ""
    )

    if segment_levels is not None:
        segment_ids = _segment_ids_for_metrics_scope(
            analytics,
            competition_ids=competition_ids,
            seg_discipline_ids=seg_discipline_ids,
            competition_scope=competition_scope,
            segment_levels=segment_levels,
        )
        goe_segment_ids = _segment_ids_for_metrics_scope(
            analytics,
            competition_ids=sorted(goe_eligible),
            seg_discipline_ids=seg_discipline_ids,
            competition_scope=competition_scope,
            segment_levels=segment_levels,
        )
        pcs_raw, elem_raw = _judge_aggregates_from_segments(
            analytics.session,
            segment_ids,
            by_competition=True,
            element_segment_ids=goe_segment_ids,
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
    elif shard_cache_populated(analytics.session):
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
            include_rule_errors=include_rule_errors,
        )

    print(
        f"Computing excess anomalies for {len(competition_ids)} {label} competitions"
        f"{level_label}..."
    )
    if segment_levels is None:
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
    print(f"  {len(segment_ids)} {discipline_label} segments in scope")
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

    if not include_rule_errors:
        _clear_rule_errors(totals_by_label)
        for season_stats in season_by_label.values():
            _clear_rule_errors(season_stats)

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
    by_judge = _segment_entries_by_judge(
        analytics,
        competition_ids=competition_ids,
        seg_discipline_ids=seg_discipline_ids,
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


def _deviation_benchmark_kwargs(
    *,
    start_season: str,
    end_season: str,
    segment_level_preset: str | None,
) -> dict[str, Any]:
    """σ̂ benchmark pool scope (all competitions; all levels unless ranking is unrestricted)."""
    kw = dict(
        benchmark_start_season_year=start_season,
        benchmark_end_season_year=end_season,
        benchmark_competition_scope_key=COMPETITION_SCOPE_ALL,
    )
    if segment_level_preset is not None:
        kw["benchmark_segment_level_preset"] = ELEMENT_RANKING_LEVEL_FILTER_ALL
    return kw


def _deviation_marking_scores_for_scope(
    analytics: JudgeAnalytics,
    seasons: list[str],
    *,
    seg_discipline_ids: list[int],
    competition_scope: str,
    scope_label: str,
    segment_level_preset: str | None = None,
) -> tuple[dict[str, float], dict[str, float]]:
    """PCS and element deviation marking scores for one ranking scope."""
    if not seasons:
        return {}, {}
    start_season = min(seasons)
    end_season = max(seasons)
    rank_kw = dict(
        start_season_year=start_season,
        end_season_year=end_season,
        discipline_type_ids=seg_discipline_ids,
        competition_scope=competition_scope,
    )
    if segment_level_preset is not None:
        rank_kw["segment_level_preset"] = segment_level_preset
    benchmark_kw = _deviation_benchmark_kwargs(
        start_season=start_season,
        end_season=end_season,
        segment_level_preset=segment_level_preset,
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
    level_note = (
        f"; {segment_level_preset.replace('_', ' ')}"
        if segment_level_preset
        else "; all segment levels"
    )

    print(
        f"Loading PCS deviation marking scores ({scope_label}{level_note}; "
        "benchmark pool: all competitions, all segment levels)..."
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
        elem_load_kw = dict(rank_kw)
        preset = elem_load_kw.pop("segment_level_preset", None)
        if preset is not None:
            elem_load_kw["segment_levels"] = segment_levels_for_ranking_preset(preset)
        elem_marks = load_element_marking_data(
            analytics.session,
            analytics,
            event_start_date=GOE_ELIGIBLE_FROM,
            **elem_load_kw,
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
    include_rule_errors: bool,
    pooled_stats: dict[str, int] | None = None,
    pooled_by_season: dict[str, dict[str, int]] | None = None,
    pooled_seg: dict[str, int] | None = None,
    pcs_marking_score: float | None = None,
    element_marking_score: float | None = None,
) -> dict[str, Any]:
    t = pooled_stats if pooled_stats is not None else (totals.get(label, {}) if has_label else {})
    seg_bucket = pooled_seg if pooled_seg is not None else (seg if has_label else {})
    row: dict[str, Any] = {}
    if include_marking_scores:
        row[f"{prefix}pcs_marking_score"] = (
            pcs_marking_score
            if pcs_marking_score is not None
            else (pcs_marking.get(label) if has_label else None)
        )
        row[f"{prefix}element_marking_score"] = (
            element_marking_score
            if element_marking_score is not None
            else (element_marking.get(label) if has_label else None)
        )
    row[f"{prefix}competition_count"] = (
        int(seg_bucket.get("competition_count", 0)) if has_label else None
    )
    row[f"{prefix}segment_count"] = (
        int(seg_bucket.get("segment_count", 0)) if has_label else None
    )
    row[f"{prefix}junior_senior_segment_count"] = (
        int(seg_bucket.get("junior_senior_segment_count", 0)) if has_label else None
    )
    row[f"{prefix}total_scores"] = int(t.get("scores", 0)) if has_label else None
    if include_rule_errors:
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
        s = (
            pooled_by_season.get(season, {})
            if pooled_by_season is not None
            else (by_season.get(season, {}).get(label, {}) if has_label else {})
        )
        row[f"{prefix}scores_{season}"] = int(s.get("scores", 0)) if has_label else None
        if include_rule_errors:
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
    discipline: str = "singles_pairs",
    listing_season: int = REPORT_LISTING_SEASON_DEFAULT,
    n_seasons: int = 3,
    availability_title: str | None = None,
) -> pd.DataFrame:
    if discipline not in REPORT_DISCIPLINE_CONFIGS:
        choices = ", ".join(REPORT_DISCIPLINE_CONFIGS)
        raise ValueError(f"Unknown discipline {discipline!r}; choose one of: {choices}")
    config = REPORT_DISCIPLINE_CONFIGS[discipline]
    if not config.segment_discipline_type_ids:
        raise RuntimeError(
            f"No segment discipline types configured for {config.roster_label} "
            "(synchronized segment type could not be resolved)."
        )
    if availability_title is None:
        availability_title = config.default_availability_title

    seg_discipline_ids = list(config.segment_discipline_type_ids)
    seasons = _season_years_for_listing(listing_season, n_seasons)
    if not seasons:
        raise RuntimeError(
            f"No season window for listing {listing_season} with --seasons {n_seasons}."
        )
    season_year_codes = season_codes_preceding_listing(int(listing_season), int(n_seasons))
    activity_seasons = _season_years_for_listing(listing_season, ACTIVITY_WINDOW_SEASONS)
    activity_season_year_codes = season_codes_preceding_listing(
        int(listing_season), ACTIVITY_WINDOW_SEASONS
    )
    if not activity_seasons:
        raise RuntimeError(
            f"No activity season window for listing {listing_season} "
            f"with {ACTIVITY_WINDOW_SEASONS} seasons."
        )

    roster = _national_judges(analytics.session, config=config)
    official_ids = [int(x) for x in roster["official_id"].tolist()]
    discipline_by_official = _official_primary_judge_discipline_ids(
        analytics.session, official_ids, config=config
    )
    international_judge_ids = _international_judge_official_ids(
        official_ids,
        discipline_by_official=discipline_by_official,
    )
    activity_cols = _activity_tracker_columns(
        analytics.session,
        official_ids,
        config=config,
        discipline_by_official=discipline_by_official,
        availability_title=availability_title,
        season_year_codes=activity_season_year_codes,
    )

    official_to_label = _official_id_to_identity_label(
        analytics, roster_official_ids=official_ids
    )
    official_identity_labels = _official_identity_labels(
        analytics, roster_official_ids=official_ids
    )
    linked_officials = _linked_official_ids(analytics.session)
    all_comp_ids = _competition_ids_for_seasons(
        analytics,
        seasons,
        competition_scope=config.activity_competition_scope,
    )
    activity_comp_ids = _competition_ids_for_seasons(
        analytics,
        activity_seasons,
        competition_scope=config.activity_competition_scope,
    )
    activity_comp_id_set = frozenset(activity_comp_ids)
    champs_comp_ids, champs_seasons = _championships_goe_era_scope(
        analytics, championships_type_ids=config.championships_type_ids
    )
    sectionals_comp_ids, sectionals_seasons = _sectionals_goe_era_scope(
        analytics, sectionals_type_ids=config.sectionals_type_ids
    )
    print(
        f"Listing season {listing_season}: {config.roster_label} "
        f"{config.activity_scope_label} window "
        f"{', '.join(seasons)} ({len(all_comp_ids)} competitions)"
    )
    print(
        f"  Activity window: {', '.join(activity_seasons)} "
        f"({len(activity_comp_ids)} competitions)"
    )
    print(
        f"  {config.sectionals_label} (GOE era): {len(sectionals_comp_ids)} competitions across "
        f"seasons {', '.join(sectionals_seasons) or '(none)'}"
    )
    print(
        f"  {config.championships_label} (GOE era): {len(champs_comp_ids)} competitions across "
        f"seasons {', '.join(champs_seasons) or '(none)'}"
    )
    cal_years = calendar_years_for_usfs_season_codes(activity_season_year_codes)
    if cal_years:
        print(
            "Activity total comps in role: seasons "
            f"{', '.join(str(c) for c in sorted(activity_season_year_codes, reverse=True))} "
            f"(calendar years {', '.join(str(y) for y in cal_years)})"
        )

    metrics = _metrics_for_competitions(
        analytics,
        competition_ids=all_comp_ids,
        seg_discipline_ids=seg_discipline_ids,
        seasons=seasons,
        competition_scope=config.activity_competition_scope,
        label=config.activity_scope_label,
        discipline_label=config.roster_label,
        include_rule_errors=config.include_rule_errors,
    )
    champs_metrics = _metrics_for_competitions(
        analytics,
        competition_ids=champs_comp_ids,
        seg_discipline_ids=seg_discipline_ids,
        seasons=champs_seasons,
        competition_scope=COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
        label="championships",
        discipline_label=config.roster_label,
        include_rule_errors=config.include_rule_errors,
        segment_levels=config.champs_performance_segment_levels,
    )
    sectionals_metrics = _metrics_for_competitions(
        analytics,
        competition_ids=sectionals_comp_ids,
        seg_discipline_ids=seg_discipline_ids,
        seasons=sectionals_seasons,
        competition_scope=config.sectionals_competition_scope,
        label="sectionals",
        discipline_label=config.roster_label,
        include_rule_errors=config.include_rule_errors,
        segment_levels=config.sectionals_performance_segment_levels,
    )
    totals = metrics["totals"]
    by_season = metrics["by_season"]
    champs_totals = champs_metrics["totals"]
    champs_by_season = champs_metrics["by_season"]
    sectionals_totals = sectionals_metrics["totals"]
    sectionals_by_season = sectionals_metrics["by_season"]

    print("Counting segments judged...")
    segment_entries_by_judge = _segment_entries_by_judge(
        analytics,
        competition_ids=all_comp_ids,
        seg_discipline_ids=seg_discipline_ids,
    )
    champs_segment_entries_by_judge = _segment_entries_by_judge(
        analytics,
        competition_ids=champs_comp_ids,
        seg_discipline_ids=seg_discipline_ids,
    )
    sectionals_segment_entries_by_judge = _segment_entries_by_judge(
        analytics,
        competition_ids=sectionals_comp_ids,
        seg_discipline_ids=seg_discipline_ids,
    )
    candidate_judge_ids_by_official = _official_candidate_judge_ids_for_roster(
        analytics, roster
    )
    segment_team_counts = None
    champs_segment_team_counts = None
    sectionals_segment_team_counts = None
    if config.junior_senior_min_team_count is not None:
        segment_team_counts = _segment_team_counts(
            analytics.session,
            competition_ids=all_comp_ids,
            seg_discipline_ids=seg_discipline_ids,
        )
        champs_segment_team_counts = _segment_team_counts(
            analytics.session,
            competition_ids=champs_comp_ids,
            seg_discipline_ids=seg_discipline_ids,
        )
        sectionals_segment_team_counts = _segment_team_counts(
            analytics.session,
            competition_ids=sectionals_comp_ids,
            seg_discipline_ids=seg_discipline_ids,
        )
    pcs_marking, element_marking = _deviation_marking_scores_for_scope(
        analytics,
        seasons,
        seg_discipline_ids=seg_discipline_ids,
        competition_scope=config.activity_competition_scope,
        scope_label=config.activity_scope_label,
    )
    if champs_seasons:
        champs_pcs_marking, champs_element_marking = _deviation_marking_scores_for_scope(
            analytics,
            champs_seasons,
            seg_discipline_ids=seg_discipline_ids,
            competition_scope=COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
            scope_label="championships (GOE era)",
            segment_level_preset=config.champs_segment_level_preset,
        )
    else:
        champs_pcs_marking, champs_element_marking = {}, {}
    if sectionals_seasons:
        sectionals_pcs_marking, sectionals_element_marking = (
            _deviation_marking_scores_for_scope(
                analytics,
                sectionals_seasons,
                seg_discipline_ids=seg_discipline_ids,
                competition_scope=config.sectionals_competition_scope,
                scope_label=f"sectionals ({config.sectionals_label}, GOE era)",
                segment_level_preset=config.sectionals_segment_level_preset,
            )
        )
    else:
        sectionals_pcs_marking, sectionals_element_marking = {}, {}

    rows = []
    for _, off in roster.iterrows():
        oid = int(off["official_id"])
        labels = official_identity_labels.get(oid, [])
        label = labels[0] if labels else (official_to_label.get(oid) or "")
        has_label = bool(labels)
        pooled_totals = _pool_judge_stats(totals, labels) if has_label else {}
        pooled_season = (
            _pool_season_stats(by_season, labels, seasons) if has_label else {}
        )
        candidate_jids = candidate_judge_ids_by_official.get(oid, set())
        pooled_seg = (
            _segment_counts_for_judge_ids(
                segment_entries_by_judge,
                candidate_jids,
                segment_team_counts=segment_team_counts,
                junior_senior_min_team_count=config.junior_senior_min_team_count,
            )
            if candidate_jids
            else {}
        )
        activity_pooled_seg = (
            _segment_counts_for_judge_ids(
                segment_entries_by_judge,
                candidate_jids,
                competition_ids=activity_comp_id_set,
                segment_team_counts=segment_team_counts,
                junior_senior_min_team_count=config.junior_senior_min_team_count,
            )
            if candidate_jids
            else {}
        )
        pooled_champs_totals = (
            _pool_judge_stats(champs_totals, labels) if has_label else {}
        )
        pooled_champs_season = (
            _pool_season_stats(champs_by_season, labels, champs_seasons)
            if has_label
            else {}
        )
        pooled_champs_seg = (
            _segment_counts_for_judge_ids(
                champs_segment_entries_by_judge,
                candidate_jids,
                segment_team_counts=champs_segment_team_counts,
                junior_senior_min_team_count=config.junior_senior_min_team_count,
            )
            if candidate_jids
            else {}
        )
        pooled_sectionals_totals = (
            _pool_judge_stats(sectionals_totals, labels) if has_label else {}
        )
        pooled_sectionals_season = (
            _pool_season_stats(sectionals_by_season, labels, sectionals_seasons)
            if has_label
            else {}
        )
        pooled_sectionals_seg = (
            _segment_counts_for_judge_ids(
                sectionals_segment_entries_by_judge,
                candidate_jids,
                segment_team_counts=sectionals_segment_team_counts,
                junior_senior_min_team_count=config.junior_senior_min_team_count,
            )
            if candidate_jids
            else {}
        )
        act = activity_cols.get(oid, {})
        row = {
            "appointment_year": act.get("appointment_year"),
            "last_champs_in_role": act.get("last_champs_in_role"),
            "last_sectionals_in_role": act.get("last_sectionals_in_role"),
            "total_comps_in_role_2yr": act.get("total_comps_in_role_2yr"),
            "activity_competition_count": (
                int(activity_pooled_seg.get("competition_count", 0)) if has_label else None
            ),
            "activity_segment_count": (
                int(activity_pooled_seg.get("segment_count", 0)) if has_label else None
            ),
            "activity_junior_senior_segment_count": (
                int(activity_pooled_seg.get("junior_senior_segment_count", 0))
                if has_label
                else None
            ),
            "us_champs_senior_availability": act.get("us_champs_senior_availability"),
            "international_judge": oid in international_judge_ids,
            "official_id": oid,
            "directory_name": off["full_name"],
            "mbr_number": off["mbr_number"],
            "directory_disciplines": off["directory_disciplines"],
            "linked_to_scoring_judge": oid in linked_officials,
            "judge_identity_label": label or None,
            "judge_identity_labels": ", ".join(labels) if labels else None,
            "discipline": config.key,
            "activity_scope": config.activity_scope_label,
            "seasons_included": ", ".join(seasons),
            "sectionals_seasons_included": ", ".join(sectionals_seasons),
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
                seg=pooled_seg if has_label else {},
                pcs_marking=pcs_marking,
                element_marking=element_marking,
                include_marking_scores=True,
                include_rule_errors=config.include_rule_errors,
                pooled_stats=pooled_totals,
                pooled_by_season=pooled_season,
                pooled_seg=pooled_seg,
                pcs_marking_score=_marking_score_for_labels(pcs_marking, labels),
                element_marking_score=_marking_score_for_labels(
                    element_marking, labels
                ),
            )
        )
        row.update(
            _metric_columns(
                prefix="sectionals_",
                totals=sectionals_totals,
                by_season=sectionals_by_season,
                seasons=sectionals_seasons,
                label=label,
                has_label=has_label,
                seg=pooled_sectionals_seg if has_label else {},
                pcs_marking=sectionals_pcs_marking,
                element_marking=sectionals_element_marking,
                include_marking_scores=True,
                include_rule_errors=config.include_rule_errors,
                pooled_stats=pooled_sectionals_totals,
                pooled_by_season=pooled_sectionals_season,
                pooled_seg=pooled_sectionals_seg,
                pcs_marking_score=_marking_score_for_labels(
                    sectionals_pcs_marking, labels
                ),
                element_marking_score=_marking_score_for_labels(
                    sectionals_element_marking, labels
                ),
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
                seg=pooled_champs_seg if has_label else {},
                pcs_marking=champs_pcs_marking,
                element_marking=champs_element_marking,
                include_marking_scores=True,
                include_rule_errors=config.include_rule_errors,
                pooled_stats=pooled_champs_totals,
                pooled_by_season=pooled_champs_season,
                pooled_seg=pooled_champs_seg,
                pcs_marking_score=_marking_score_for_labels(
                    champs_pcs_marking, labels
                ),
                element_marking_score=_marking_score_for_labels(
                    champs_element_marking, labels
                ),
            )
        )
        rows.append(row)

    df = pd.DataFrame(rows)
    sort_cols = ["total_excess_anomalies", "directory_name"]
    if config.include_rule_errors:
        sort_cols.insert(1, "total_rule_errors")
    df = df.sort_values(
        sort_cols,
        ascending=[False] * (len(sort_cols) - 1) + [True],
        na_position="last",
    )
    return df


def main() -> None:
    default_discipline = "singles_pairs"
    parser = argparse.ArgumentParser(
        description=(
            "Report excess anomalies, anomaly rate, segment counts, and PCS/element "
            "marking scores for national Competition Judges (Singles/Pairs, Ice Dance, "
            "or Synchronized)."
        )
    )
    parser.add_argument(
        "--discipline",
        choices=DISCIPLINE_CHOICES,
        default=default_discipline,
        help=(
            "Directory discipline roster and segment scope "
            "(default: singles_pairs)."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help=(
            "Output path (.xlsx for formatted workbook with analysis/raw/lookup sheets, "
            ".csv for raw tabular export). Default: analysisTemp/<discipline workbook>."
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
        default=None,
        help=(
            "Substring match for the qualifying availability competition title "
            "(default: discipline-specific US Championships or Synchro Champs title)."
        ),
    )
    args = parser.parse_args()
    config = REPORT_DISCIPLINE_CONFIGS[args.discipline]
    output_path = args.output or (
        _REPO / "analysisTemp" / config.default_output_filename
    )
    availability_title = (
        args.availability_competition_title or config.default_availability_title
    )
    season_year_codes = season_codes_preceding_listing(
        int(args.listing_season), ACTIVITY_WINDOW_SEASONS
    )
    cal_years = calendar_years_for_usfs_season_codes(season_year_codes)
    sectionals_activity_min_year = min(cal_years) if cal_years else 0

    with get_db_session() as session:
        analytics = JudgeAnalytics(session)
        df = build_report(
            analytics,
            discipline=args.discipline,
            listing_season=args.listing_season,
            n_seasons=args.seasons,
            availability_title=availability_title,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".csv":
        df.to_csv(output_path, index=False)
        print(f"Wrote {len(df)} judges to {output_path}")
    else:
        write_national_sp_judge_analysis_xlsx(
            df,
            output_path,
            include_rule_errors=config.include_rule_errors,
            thresholds=config.thresholds,
            performance_block_header=config.performance_block_header,
            activity_column_label=config.activity_column_label,
            performance_analysis_header=config.performance_analysis_header,
            recent_period_header=config.recent_period_header,
            junior_senior_segment_count_header=config.junior_senior_segment_count_header,
            junior_senior_activity_label=config.junior_senior_activity_label,
            sectionals_block_header=f"{config.sectionals_label} (since 2018 for GOEs and 2022 for PCS)",
            sectionals_performance_header=f"{config.sectionals_label} Performance Analysis",
            sectionals_activity_min_year=sectionals_activity_min_year,
        )
        print(f"Wrote formatted workbook ({len(df)} judges) to {output_path}")
        if args.raw_csv is not None:
            args.raw_csv.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(args.raw_csv, index=False)
            print(f"Wrote raw CSV to {args.raw_csv}")
    if not df.empty and "seasons_included" in df.columns:
        print(f"Seasons: {df['seasons_included'].iloc[0]}")


if __name__ == "__main__":
    main()
