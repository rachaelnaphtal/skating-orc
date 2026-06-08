"""
Evaluate ISU Rules 411–417 service requirements (maintain / promote) against
``segment_official`` panel activity.

Season windows use the N USFS seasons immediately before the report listing season
(e.g. listing season 2627, n=3 → 2324, 2425, 2526).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import json
import re
from typing import Any, Literal

import pandas as pd
from sqlalchemy import bindparam, select, text
from sqlalchemy.orm import Session

try:
    from activityAnalysis.international_officials_data import (
        COUNTABLE_SEGMENT_LEVELS,
        DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL,
        INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
        competition_scope_label,
        filter_panel_for_appointment,
        load_international_panel_segments_bulk,
        _discipline_match_sql,
        _nullable_int_for_sql,
        national_segment_appointment_type_id,
    )
    from activityAnalysis.international_listing_seasons import (
        competition_year_matches_seasons,
        format_usfs_season_code,
        season_codes_as_bind_strings,
        season_codes_preceding_listing,
        season_year_sql_predicate,
    )
    from activityAnalysis.international_official_seminars import (
        filter_seminars_for_appointment,
        filter_seminars_to_season_codes,
        load_official_seminars_bulk,
    )
    from activityAnalysis.load_activity_data import (
        activity_database_is_postgresql,
        engine,
    )
    from activityAnalysis.officials_analysis_models import (
        Appointments,
        Levels,
    )
except ModuleNotFoundError:
    from international_officials_data import (
        COUNTABLE_SEGMENT_LEVELS,
        DATA_OPERATOR_COMBINED_DISCIPLINE_LABEL,
        INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID,
        competition_scope_label,
        filter_panel_for_appointment,
        load_international_panel_segments_bulk,
        _discipline_match_sql,
        _nullable_int_for_sql,
        national_segment_appointment_type_id,
    )
    from international_listing_seasons import (
        competition_year_matches_seasons,
        format_usfs_season_code,
        season_codes_as_bind_strings,
        season_codes_preceding_listing,
        season_year_sql_predicate,
    )
    from international_official_seminars import (
        filter_seminars_for_appointment,
        filter_seminars_to_season_codes,
        load_official_seminars_bulk,
    )
    from load_activity_data import (
        activity_database_is_postgresql,
        engine,
    )
    from officials_analysis_models import (
        Appointments,
        Levels,
    )

from officials_competition_types import (
    OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP,
    OFFICIALS_COMPETITION_TYPE_IDS_ADULT_COLLEGIATE,
    OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL,
    OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL_COMPETITION_ONLY,
    OFFICIALS_COMPETITION_TYPE_IDS_ISU_EVENT,
)

Purpose = Literal["maintain", "promote"]
Sport = Literal["figure", "synchronized"]
INTERNATIONAL_LEVEL_NAME = "International"
ISU_LEVEL_NAME = "ISU"
# USFS directory ``levels.id`` (International officials appointments).
DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP = 16
DIRECTORY_LEVEL_ID_INTERNATIONAL = 17


def directory_listing_tier_for_level(
    appointment_level: Any,
    *,
    level_id: Any = None,
    isu_level_id: int | None = None,
    international_level_id: int | None = None,
) -> str:
    """Map directory appointment level to maintain rule listing tier."""
    isu_id = int(isu_level_id if isu_level_id is not None else DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP)
    intl_id = int(
        international_level_id
        if international_level_id is not None
        else DIRECTORY_LEVEL_ID_INTERNATIONAL
    )
    resolved = _nullable_int_for_sql(level_id)
    if resolved == isu_id:
        return "isu"
    if resolved == intl_id:
        return "international"

    name = str(appointment_level or "").strip().lower()
    if name == INTERNATIONAL_LEVEL_NAME.lower():
        return "international"
    if name in (ISU_LEVEL_NAME.lower(), "isu championship") or name.startswith("isu "):
        return "isu"
    return "international"


def listing_tier_display_label(listing_tier: str) -> str:
    if str(listing_tier or "").strip().lower() == "isu":
        return "ISU Championship"
    return "International"


def user_facing_requirement_label(label: str) -> str:
    """Strip internal competition-type id references from requirement labels."""
    text = str(label or "").strip()
    text = re.sub(r";\s*types\s+15\s*[–-]\s*17", "", text, flags=re.IGNORECASE)
    text = re.sub(r";\s*types\s+15\s*[–-]\s*16", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\(\s*types\s+15\s*[–-]\s*17\s*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" ;")


def is_isu_directory_level(appointment_level: Any) -> bool:
    name = str(appointment_level or "").strip().lower()
    if name == INTERNATIONAL_LEVEL_NAME.lower():
        return False
    if name in (ISU_LEVEL_NAME.lower(), "isu championship") or name.startswith("isu "):
        return True
    return False


def is_isu_appointment_level(
    level_name: Any,
    *,
    level_id: Any = None,
    isu_level_id: int | None = None,
    international_level_id: int | None = None,
) -> bool:
    """True when the directory appointment is ISU Championship (level id 16)."""
    return (
        directory_listing_tier_for_level(
            level_name,
            level_id=level_id,
            isu_level_id=isu_level_id,
            international_level_id=international_level_id,
        )
        == "isu"
    )


def is_international_appointment_level(
    level_name: Any,
    *,
    level_id: Any = None,
    isu_level_id: int | None = None,
    international_level_id: int | None = None,
) -> bool:
    """True when the directory appointment is International (level id 17)."""
    return (
        directory_listing_tier_for_level(
            level_name,
            level_id=level_id,
            isu_level_id=isu_level_id,
            international_level_id=international_level_id,
        )
        == "international"
    )


def is_isu_listed_appointment(
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
    isu_listing_keys: set[tuple[int, int, int | None]] | None,
) -> bool:
    """True when the ISU Communication roster shows an ISU listing for this appointment."""
    if not isu_listing_keys:
        return False
    disc = _nullable_int_for_sql(discipline_id)
    if disc is not None:
        return (int(official_id), int(appointment_type_id), disc) in isu_listing_keys
    return any(
        k[0] == int(official_id) and k[1] == int(appointment_type_id)
        for k in isu_listing_keys
    )


def should_evaluate_promote_requirements(
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
    level_name: Any,
    level_id: Any,
    *,
    isu_level_id: int | None,
    isu_listing_keys: set[tuple[int, int, int | None]] | None,
) -> bool:
    """Promote checks apply only to International-level appointments not yet ISU-listed."""
    if is_isu_appointment_level(level_name, level_id=level_id, isu_level_id=isu_level_id):
        return False
    if is_isu_listed_appointment(
        official_id, appointment_type_id, discipline_id, isu_listing_keys
    ):
        return False
    return True


# ``officials_analysis.disciplines.id``
DIRECTORY_DISC_SYNCHRONIZED_ID = 2

# Protocol ``segment_official.appointment_type_id`` (national roles).
NATIONAL_ROLE_JUDGE = 1
NATIONAL_ROLE_REFEREE = 4
NATIONAL_ROLE_DATA_OPERATOR = 8
NATIONAL_ROLE_TECH_SPECIALIST = 9
NATIONAL_ROLE_TECH_CONTROLLER = 11

NATIONAL_ROLE_DISPLAY_NAMES: dict[int, str] = {
    NATIONAL_ROLE_JUDGE: "Judge",
    NATIONAL_ROLE_REFEREE: "Referee",
    NATIONAL_ROLE_DATA_OPERATOR: "Data Operator",
    NATIONAL_ROLE_TECH_SPECIALIST: "Technical Specialist",
    NATIONAL_ROLE_TECH_CONTROLLER: "Technical Controller",
}

_QUALIFYING_COMPETITION_COLUMNS = [
    "competition_id",
    "competition_year",
    "competition_name",
    "competition_scope",
    "competition_type",
    "panel_roles",
    "results_url",
]


def _first_non_empty_url(series: pd.Series) -> str | None:
    for val in series:
        if val is not None and not pd.isna(val) and str(val).strip():
            return str(val).strip()
    return None

_ISU_ROSTER_APPOINTMENT_TYPE: dict[int, str] = {
    12: "Judge",
    13: "Referee",
    14: "Technical Specialist",
    15: "Technical Controller",
    16: "Data & Replay Operator",
}


@dataclass
class RuleCheckResult:
    metric: str
    display_label: str
    required: int
    actual: int | float
    met: bool
    detail: str = ""
    qualifying_competitions: pd.DataFrame | None = None


@dataclass
class RequirementEvaluation:
    rule_set_id: int
    isu_rule_ref: str
    purpose: Purpose
    label: str
    listing_tier: str
    season_window: int
    season_codes: list[int]
    meets: bool
    summary_note: str
    rule_results: list[RuleCheckResult] = field(default_factory=list)
    qualifying_activity: pd.DataFrame | None = None
    not_applicable: bool = False
    not_applicable_reason: str = ""


try:
    from activityAnalysis.international_listing_seasons import (
        REPORT_LISTING_SEASON_DEFAULT,
        listing_calendar_year,
        listing_calendar_year_from_season_code,
        season_codes_preceding_listing,
    )
except ModuleNotFoundError:
    from international_listing_seasons import (
        REPORT_LISTING_SEASON_DEFAULT,
        listing_calendar_year,
        listing_calendar_year_from_season_code,
        season_codes_preceding_listing,
    )

try:
    from activityAnalysis.international_official_demographics import (
        APPOINTMENT_TYPE_ID_JUDGE,
        APPOINTMENT_TYPE_ID_REFEREE,
        APPOINTMENT_TYPE_ID_TECHNICAL_SPECIALIST,
        first_listing_season_meeting_appointment_criteria,
        first_listing_season_meeting_min_years_on_grade_date,
        grade_date_for_time_in_grade,
        load_official_international_appointment_rows,
        max_years_for_appointment_criteria,
        max_years_for_tc_prerequisite_role,
        related_discipline_ids_for_tc_prerequisite,
        years_detail_label,
        years_in_grade_at_listing,
    )
except ModuleNotFoundError:
    from international_official_demographics import (
        APPOINTMENT_TYPE_ID_JUDGE,
        APPOINTMENT_TYPE_ID_REFEREE,
        APPOINTMENT_TYPE_ID_TECHNICAL_SPECIALIST,
        first_listing_season_meeting_appointment_criteria,
        first_listing_season_meeting_min_years_on_grade_date,
        grade_date_for_time_in_grade,
        load_official_international_appointment_rows,
        max_years_for_appointment_criteria,
        max_years_for_tc_prerequisite_role,
        related_discipline_ids_for_tc_prerequisite,
        years_detail_label,
        years_in_grade_at_listing,
    )

PROMOTE_YEARS_METRICS = frozenset(
    {
        "years_in_grade",
        "years_isu_judge",
        "years_intl_referee",
        "years_tc_prerequisite",
    }
)


def isu_season_codes_preceding_july1(listing_calendar_year: int, n: int) -> list[int]:
    """
    USFS/ISU season codes for the ``n`` seasons preceding July 1 of ``listing_calendar_year``.

    Example: listing year 2026 → ``[2324, 2425, 2526]`` for n=3.
    """
    if n <= 0:
        return []
    codes: list[int] = []
    for i in range(n - 1, -1, -1):
        end_year = listing_calendar_year - i
        start_year = end_year - 1
        codes.append(int(f"{start_year % 100:02d}{end_year % 100:02d}"))
    return codes


def _is_championship_or_olympic(competition_type_id: Any, competition_name: str) -> bool:
    try:
        if int(competition_type_id) == OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP:
            return True
    except (TypeError, ValueError):
        pass
    name = (competition_name or "").lower()
    return "olympic" in name or "owg" in name


def _competition_matches_scope(
    competition_type_id: Any,
    competition_qualifying: Any,
    competition_type_ids: tuple[int, ...],
    *,
    include_qualifying_national: bool,
) -> bool:
    """True when a panel row counts toward a requirement rule's competition scope."""
    try:
        if int(competition_type_id) in {int(x) for x in competition_type_ids}:
            return True
    except (TypeError, ValueError):
        pass
    if include_qualifying_national:
        try:
            if int(competition_type_id) in OFFICIALS_COMPETITION_TYPE_IDS_ADULT_COLLEGIATE:
                return False
        except (TypeError, ValueError):
            pass
        if competition_qualifying is True:
            return True
        if isinstance(competition_qualifying, (int, float)) and bool(competition_qualifying):
            return True
        if str(competition_qualifying or "").strip().lower() in ("true", "t", "1", "yes"):
            return True
    return False


def _competition_count_from_panel(
    panel: pd.DataFrame,
    *,
    season_codes: list[int],
    competition_type_ids: tuple[int, ...],
    segment_levels: frozenset[str],
    championship_or_olympic_only: bool = False,
    include_qualifying_national: bool = False,
) -> int:
    if panel.empty or not season_codes:
        return 0

    df = panel
    season_mask = df["competition_year"].apply(
        lambda y: competition_year_matches_seasons(y, season_codes)
    )
    df = df.loc[season_mask]
    if df.empty:
        return 0

    comp_ids = {int(x) for x in competition_type_ids}
    scope_mask = df.apply(
        lambda r: _competition_matches_scope(
            r.get("competition_type_id"),
            r.get("competition_qualifying"),
            tuple(comp_ids),
            include_qualifying_national=include_qualifying_national,
        ),
        axis=1,
    )
    df = df.loc[scope_mask]
    df = df.loc[df["segment_level"].isin(segment_levels)]
    if df.empty:
        return 0

    if championship_or_olympic_only:
        df = df.loc[
            df.apply(
                lambda r: _is_championship_or_olympic(
                    r["competition_type_id"], r["competition_name"]
                ),
                axis=1,
            )
        ]
    return int(df["competition_id"].nunique())


def _national_role_display(role_id: Any) -> str:
    try:
        return NATIONAL_ROLE_DISPLAY_NAMES.get(int(role_id), f"Role {int(role_id)}")
    except (TypeError, ValueError):
        return "Unknown"


def _role_ids_for_metric(metric: str, roles: tuple[int, ...]) -> tuple[int, ...] | None:
    if metric == "judge_promote_isu":
        return roles or (NATIONAL_ROLE_JUDGE,)
    if metric == "tc_ts_promote_isu":
        return roles or (NATIONAL_ROLE_TECH_CONTROLLER, NATIONAL_ROLE_TECH_SPECIALIST)
    if metric == "competition_alternatives":
        return roles if roles else None
    if metric == "combined_roles_competitions":
        return roles or (
            NATIONAL_ROLE_REFEREE,
            NATIONAL_ROLE_JUDGE,
            NATIONAL_ROLE_TECH_CONTROLLER,
        )
    if metric == "referee_competitions":
        return roles or (NATIONAL_ROLE_REFEREE,)
    if metric in ("judge_competitions", "judge_championship_or_olympic"):
        return roles or (NATIONAL_ROLE_JUDGE,)
    if metric == "tc_or_ts_competitions":
        return roles or (NATIONAL_ROLE_TECH_CONTROLLER, NATIONAL_ROLE_TECH_SPECIALIST)
    if metric == "data_operator_competitions":
        return roles or (NATIONAL_ROLE_DATA_OPERATOR,)
    return roles or None


def _filter_panel_for_competition_metric(
    panel: pd.DataFrame,
    *,
    season_codes: list[int],
    competition_type_ids: tuple[int, ...],
    segment_levels: frozenset[str],
    championship_or_olympic_only: bool = False,
    include_qualifying_national: bool = False,
) -> pd.DataFrame:
    if panel.empty or not season_codes:
        return panel.iloc[0:0]

    season_mask = panel["competition_year"].apply(
        lambda y: competition_year_matches_seasons(y, season_codes)
    )
    df = panel.loc[season_mask]
    if df.empty:
        return df

    comp_ids = {int(x) for x in competition_type_ids}
    scope_mask = df.apply(
        lambda r: _competition_matches_scope(
            r.get("competition_type_id"),
            r.get("competition_qualifying"),
            tuple(comp_ids),
            include_qualifying_national=include_qualifying_national,
        ),
        axis=1,
    )
    df = df.loc[scope_mask]
    df = df.loc[df["segment_level"].isin(segment_levels)]
    if df.empty:
        return df

    if championship_or_olympic_only:
        df = df.loc[
            df.apply(
                lambda r: _is_championship_or_olympic(
                    r["competition_type_id"], r["competition_name"]
                ),
                axis=1,
            )
        ]

    try:
        from activityAnalysis.international_segment_eligibility import (
            filter_panel_to_rule411_eligible,
        )
    except ModuleNotFoundError:
        from international_segment_eligibility import filter_panel_to_rule411_eligible

    return filter_panel_to_rule411_eligible(df)


def _aggregate_qualifying_competitions(panel_segments: pd.DataFrame) -> pd.DataFrame:
    if panel_segments.empty:
        return pd.DataFrame(columns=_QUALIFYING_COMPETITION_COLUMNS)

    def _roles_for_group(group: pd.DataFrame) -> str:
        role_ids = (
            pd.to_numeric(group["national_appointment_type_id"], errors="coerce")
            .dropna()
            .astype(int)
            .unique()
        )
        return ", ".join(sorted({_national_role_display(r) for r in role_ids}))

    grouped = (
        panel_segments.groupby("competition_id", as_index=False)
        .agg(
            competition_year=("competition_year", "first"),
            competition_name=("competition_name", "first"),
            competition_type_id=("competition_type_id", "first"),
            competition_qualifying=("competition_qualifying", "first"),
        )
    )
    if "results_url" in panel_segments.columns:
        urls = (
            panel_segments.groupby("competition_id")["results_url"]
            .apply(_first_non_empty_url)
            .reset_index(name="results_url")
        )
        grouped = grouped.merge(urls, on="competition_id", how="left")
    else:
        grouped["results_url"] = None
    roles = (
        panel_segments.groupby("competition_id")
        .apply(_roles_for_group, include_groups=False)
        .reset_index(name="panel_roles")
    )
    out = grouped.merge(roles, on="competition_id", how="left")
    out["competition_scope"] = out.apply(
        lambda r: competition_scope_label(
            r["competition_type_id"], r.get("competition_qualifying")
        ),
        axis=1,
    )
    try:
        from officials_competition_types import OFFICIALS_COMPETITION_TYPE_DISPLAY_NAMES
    except ModuleNotFoundError:
        from activityAnalysis.officials_competition_types import (
            OFFICIALS_COMPETITION_TYPE_DISPLAY_NAMES,
        )

    out["competition_type"] = out["competition_type_id"].map(
        lambda x: OFFICIALS_COMPETITION_TYPE_DISPLAY_NAMES.get(int(x), f"Type {x}")
        if pd.notna(x)
        else ""
    )
    out = out.sort_values(
        ["competition_year", "competition_name"],
        ascending=[False, True],
    ).reset_index(drop=True)
    return out[_QUALIFYING_COMPETITION_COLUMNS]


def _qualifying_competitions_from_panel(
    panel: pd.DataFrame,
    *,
    season_codes: list[int],
    competition_type_ids: tuple[int, ...],
    segment_levels: frozenset[str],
    championship_or_olympic_only: bool = False,
    include_qualifying_national: bool = False,
) -> pd.DataFrame:
    filtered = _filter_panel_for_competition_metric(
        panel,
        season_codes=season_codes,
        competition_type_ids=competition_type_ids,
        segment_levels=segment_levels,
        championship_or_olympic_only=championship_or_olympic_only,
        include_qualifying_national=include_qualifying_national,
    )
    return _aggregate_qualifying_competitions(filtered)


def _qualifying_competitions_for_alternatives(
    panel: pd.DataFrame,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
) -> pd.DataFrame:
    df = _panel_after_season_and_level(
        panel, season_codes=season_codes, segment_levels=segment_levels
    )
    if df.empty:
        return pd.DataFrame(columns=_QUALIFYING_COMPETITION_COLUMNS)

    scopes = (
        "international_competition",
        "isu_event",
        "international_all",
        "isu_championship",
        "national_qualifying",
    )
    mask = df.apply(
        lambda r: any(
            _row_matches_competition_scope(
                r.get("competition_type_id"),
                r.get("competition_qualifying"),
                scope,
            )
            for scope in scopes
        ),
        axis=1,
    )
    return _aggregate_qualifying_competitions(df.loc[mask])


def _union_qualifying_competitions(rule_results: list[RuleCheckResult]) -> pd.DataFrame | None:
    frames = [
        r.qualifying_competitions
        for r in rule_results
        if r.qualifying_competitions is not None and not r.qualifying_competitions.empty
    ]
    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        return None

    def _merge_roles(group: pd.DataFrame) -> pd.Series:
        roles: set[str] = set()
        for text in group["panel_roles"].dropna():
            roles.update(part.strip() for part in str(text).split(",") if part.strip())
        row = group.iloc[0].copy()
        row["panel_roles"] = ", ".join(sorted(roles))
        return row

    merged = (
        combined.groupby("competition_id", as_index=False)
        .apply(_merge_roles, include_groups=False)
        .reset_index(drop=True)
    )
    return merged.sort_values(
        ["competition_year", "competition_name"],
        ascending=[False, True],
    ).reset_index(drop=True)[_QUALIFYING_COMPETITION_COLUMNS]


def _panel_after_season_and_level(
    panel: pd.DataFrame,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
) -> pd.DataFrame:
    if panel.empty or not season_codes:
        return panel.iloc[0:0]
    df = panel.loc[
        panel["competition_year"].apply(
            lambda y: competition_year_matches_seasons(y, season_codes)
        )
    ]
    df = df.loc[df["segment_level"].isin(segment_levels)]
    if df.empty:
        return df

    try:
        from activityAnalysis.international_segment_eligibility import (
            filter_panel_to_rule411_eligible,
        )
    except ModuleNotFoundError:
        from international_segment_eligibility import filter_panel_to_rule411_eligible

    return filter_panel_to_rule411_eligible(df)


def _row_matches_competition_scope(
    competition_type_id: Any,
    competition_qualifying: Any,
    scope: str,
) -> bool:
    """ISU rule competition scopes for OR-alternative service requirements."""
    try:
        ct = int(competition_type_id)
    except (TypeError, ValueError):
        return False

    if scope == "international_competition":
        return ct in OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL_COMPETITION_ONLY
    if scope == "isu_event":
        return ct in OFFICIALS_COMPETITION_TYPE_IDS_ISU_EVENT
    if scope == "international_all":
        return ct in OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL
    if scope == "isu_championship":
        return _is_championship_or_olympic(ct, "")
    if scope == "national_qualifying":
        if ct in OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL:
            return False
        return _competition_matches_scope(
            ct, competition_qualifying, (), include_qualifying_national=True
        )
    return False


def _competition_count_after_rule411(
    panel: pd.DataFrame,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
    competition_type_ids: tuple[int, ...] = (15, 16, 17),
    include_qualifying_national: bool = False,
    scope: str | None = None,
) -> int:
    """Distinct competitions on Rule-411-eligible segments (optional ISU scope filter)."""
    df = _panel_after_season_and_level(
        panel, season_codes=season_codes, segment_levels=segment_levels
    )
    if df.empty:
        return 0
    if scope:
        mask = df.apply(
            lambda r: _row_matches_competition_scope(
                r.get("competition_type_id"),
                r.get("competition_qualifying"),
                scope,
            ),
            axis=1,
        )
        df = df.loc[mask]
        return int(df["competition_id"].nunique()) if not df.empty else 0

    comp_ids = {int(x) for x in competition_type_ids}
    mask = df.apply(
        lambda r: _competition_matches_scope(
            r.get("competition_type_id"),
            r.get("competition_qualifying"),
            tuple(comp_ids),
            include_qualifying_national=include_qualifying_national,
        ),
        axis=1,
    )
    return int(df.loc[mask, "competition_id"].nunique())


def _count_competitions_for_scope(
    panel: pd.DataFrame,
    scope: str,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
) -> int:
    df = _panel_after_season_and_level(
        panel, season_codes=season_codes, segment_levels=segment_levels
    )
    if df.empty:
        return 0
    mask = df.apply(
        lambda r: _row_matches_competition_scope(
            r.get("competition_type_id"),
            r.get("competition_qualifying"),
            scope,
        ),
        axis=1,
    )
    return int(df.loc[mask, "competition_id"].nunique())


def _parse_metric_config(value: Any) -> dict[str, Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _panel_for_alternative_roles(
    panel: pd.DataFrame,
    role_ids: list[int] | tuple[int, ...] | None,
) -> pd.DataFrame:
    if panel.empty:
        return panel
    if not role_ids:
        return panel.iloc[0:0]
    ids = {int(x) for x in role_ids}
    return panel.loc[panel["national_appointment_type_id"].isin(ids)]


def _alternatives_with_role_ids(alternatives: list[dict[str, Any]]) -> bool:
    return any(alt.get("role_ids") for alt in alternatives)


def _sort_competition_alternatives(
    alternatives: list[dict[str, Any]],
    *,
    intl_appointment_type_id: int | None,
) -> list[dict[str, Any]]:
    """Try the branch matching this directory appointment's panel role first."""
    if intl_appointment_type_id is None:
        return alternatives
    try:
        from activityAnalysis.international_officials_data import (
            national_segment_appointment_type_id,
        )
    except ModuleNotFoundError:
        from international_officials_data import national_segment_appointment_type_id

    nat_role = national_segment_appointment_type_id(int(intl_appointment_type_id))
    if nat_role is None:
        return alternatives

    def sort_key(alt: dict[str, Any]) -> tuple[int, int]:
        role_ids = alt.get("role_ids") or []
        try:
            roles = {int(x) for x in role_ids}
        except (TypeError, ValueError):
            roles = set()
        return (0 if int(nat_role) in roles else 1, 0)

    return sorted(alternatives, key=sort_key)


def _competition_ids_for_scope(
    panel: pd.DataFrame,
    scope: str,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
) -> set[int]:
    df = _panel_after_season_and_level(
        panel, season_codes=season_codes, segment_levels=segment_levels
    )
    if df.empty:
        return set()
    mask = df.apply(
        lambda r: _row_matches_competition_scope(
            r.get("competition_type_id"),
            r.get("competition_qualifying"),
            scope,
        ),
        axis=1,
    )
    return {int(x) for x in df.loc[mask, "competition_id"].dropna().unique()}


def _requirements_coverable_distinct(reqs: list[tuple[int, set[int]]]) -> bool:
    """True when each requirement can be met using distinct competition ids."""
    if not reqs:
        return True

    def dfs(index: int, used: set[int]) -> bool:
        if index == len(reqs):
            return True
        min_n, pool = reqs[index]
        available = sorted(pool - used)
        if len(available) < min_n:
            return False
        if min_n == 0:
            return dfs(index + 1, used)
        from itertools import combinations

        for picked in combinations(available, min_n):
            if dfs(index + 1, used | set(picked)):
                return True
        return False

    return dfs(0, set())


def _branch_meets_international_including_championship(
    branch_panel: pd.DataFrame,
    requirements: list[dict[str, Any]],
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
) -> tuple[bool, list[str]] | None:
    """
    ``international_all`` + ``isu_championship``: N international including M championships.

    The championship minimum applies to competitions already counted toward international,
    not as additional separate events.
    """
    if len(requirements) != 2:
        return None
    scopes = {str(r.get("scope") or "") for r in requirements}
    if scopes != {"international_all", "isu_championship"}:
        return None

    intl_req = next(r for r in requirements if r.get("scope") == "international_all")
    champ_req = next(r for r in requirements if r.get("scope") == "isu_championship")
    try:
        intl_min = int(intl_req.get("min", 0))
        champ_min = int(champ_req.get("min", 0))
    except (TypeError, ValueError):
        return None

    intl_ids = _competition_ids_for_scope(
        branch_panel,
        "international_all",
        season_codes=season_codes,
        segment_levels=segment_levels,
    )
    champ_ids = _competition_ids_for_scope(
        branch_panel,
        "isu_championship",
        season_codes=season_codes,
        segment_levels=segment_levels,
    )
    among = intl_ids & champ_ids
    parts = [
        f"{len(intl_ids)}/{intl_min} International",
        f"{len(among)}/{champ_min} ISU Championship among those",
    ]
    ok = len(intl_ids) >= intl_min and len(among) >= champ_min
    return ok, parts


def _branch_meets_requirements(
    branch_panel: pd.DataFrame,
    requirements: list[dict[str, Any]],
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
) -> tuple[bool, list[str]]:
    if len(requirements) > 1:
        included = _branch_meets_international_including_championship(
            branch_panel,
            requirements,
            season_codes=season_codes,
            segment_levels=segment_levels,
        )
        if included is not None:
            return included

    parts: list[str] = []
    req_sets: list[tuple[int, set[int]]] = []
    per_scope_ok = True
    for req in requirements:
        scope = str(req.get("scope") or "")
        try:
            min_n = int(req.get("min", 0))
        except (TypeError, ValueError):
            min_n = 0
        comp_ids = _competition_ids_for_scope(
            branch_panel,
            scope,
            season_codes=season_codes,
            segment_levels=segment_levels,
        )
        count = len(comp_ids)
        ok = count >= min_n
        per_scope_ok = per_scope_ok and ok
        req_sets.append((min_n, comp_ids))
        parts.append(f"{count}/{min_n} {_scope_display_name(scope)}")
    if not per_scope_ok:
        return False, parts
    if len(requirements) <= 1:
        return True, parts
    distinct_ok = _requirements_coverable_distinct(req_sets)
    if not distinct_ok:
        parts.append("need distinct competitions")
    return distinct_ok, parts


def _evaluate_competition_alternatives(
    panel: pd.DataFrame,
    metric_config: Any,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
    intl_appointment_type_id: int | None = None,
) -> tuple[bool, str, str]:
    """
    Evaluate OR branches; each branch is AND of scoped competition minimums.

    Returns (met, via_label, detail).
    """
    config = _parse_metric_config(metric_config)
    alternatives = config.get("alternatives") or []
    if not alternatives:
        return False, "", "No alternatives configured"

    role_scoped = _alternatives_with_role_ids(alternatives)
    alternatives = _sort_competition_alternatives(
        alternatives, intl_appointment_type_id=intl_appointment_type_id
    )

    branch_summaries: list[str] = []
    winning: tuple[str, list[str]] | None = None
    for alt in alternatives:
        label = str(alt.get("label") or "Option")
        role_ids = alt.get("role_ids")
        if role_ids:
            branch_panel = _panel_for_alternative_roles(panel, role_ids)
        elif role_scoped:
            branch_panel = panel.iloc[0:0]
        else:
            branch_panel = panel
        requirements = alt.get("requirements") or []
        branch_ok, parts = _branch_meets_requirements(
            branch_panel,
            requirements,
            season_codes=season_codes,
            segment_levels=segment_levels,
        )
        summary = f"{label}: {', '.join(parts)}"
        branch_summaries.append(summary)
        if branch_ok and winning is None:
            winning = (label, list(parts))

    if winning is not None:
        label, parts = winning
        others = [s for s in branch_summaries if not s.startswith(f"{label}:")]
        detail = f"Meets via {label} ({', '.join(parts)})"
        if others:
            detail += " — also checked: " + "; ".join(others)
        return True, label, detail

    return False, "", "Need one of: " + "; ".join(branch_summaries)


def format_competition_alternatives_detail(detail: str, *, met: bool) -> list[str]:
    """Split competition_alternatives progress text into display lines."""
    if not detail:
        return []
    if met and detail.startswith("Meets via "):
        return [detail.replace("Meets via ", "Satisfied by: ", 1)]
    if detail.startswith("Need one of: "):
        return [
            part.strip()
            for part in detail[len("Need one of: ") :].split("; ")
            if part.strip()
        ]
    return [detail]


def _scope_display_name(scope: str) -> str:
    names = {
        "international_competition": "International Competition",
        "isu_event": "ISU Event",
        "international_all": "International",
        "isu_championship": "ISU Championship",
        "national_qualifying": "National",
    }
    return names.get(scope, scope)


def _count_competitions_with_segment_level(
    panel: pd.DataFrame,
    level: str,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
    competition_type_ids: tuple[int, ...] = (15, 16, 17),
    include_qualifying_national: bool = False,
    last_season_only: bool = False,
) -> int:
    codes = list(season_codes)
    if last_season_only and codes:
        codes = [codes[-1]]
    df = _panel_after_season_and_level(
        panel, season_codes=codes, segment_levels=segment_levels
    )
    if df.empty:
        return 0

    comp_ids = {int(x) for x in competition_type_ids}
    mask = df.apply(
        lambda r: _competition_matches_scope(
            r.get("competition_type_id"),
            r.get("competition_qualifying"),
            tuple(comp_ids),
            include_qualifying_national=include_qualifying_national,
        ),
        axis=1,
    )
    df = df.loc[mask & (df["segment_level"] == level)]
    return int(df["competition_id"].nunique())


def _count_competitions_with_segment_discipline_type(
    panel: pd.DataFrame,
    discipline_type_id: int,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
    competition_type_ids: tuple[int, ...] = (15, 16, 17),
    include_qualifying_national: bool = False,
    last_season_only: bool = False,
) -> int:
    codes = list(season_codes)
    if last_season_only and codes:
        codes = [codes[-1]]
    df = _panel_after_season_and_level(
        panel, season_codes=codes, segment_levels=segment_levels
    )
    if df.empty or "segment_discipline_type_id" not in df.columns:
        return 0

    comp_ids = {int(x) for x in competition_type_ids}
    mask = df.apply(
        lambda r: _competition_matches_scope(
            r.get("competition_type_id"),
            r.get("competition_qualifying"),
            tuple(comp_ids),
            include_qualifying_national=include_qualifying_national,
        ),
        axis=1,
    )
    df = df.loc[mask]
    df = df.loc[pd.to_numeric(df["segment_discipline_type_id"], errors="coerce") == int(discipline_type_id)]
    return int(df["competition_id"].nunique())


def _evaluate_judge_promote_isu(
    panel: pd.DataFrame,
    metric_config: Any,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
    competition_type_ids: tuple[int, ...],
    include_qualifying_national: bool,
) -> tuple[bool, str]:
    """
    Judge promotion to ISU: minimum international competitions plus required mix
    (Senior/Junior segments, optional Pairs, ISU Event in last season).
    """
    config = _parse_metric_config(metric_config)
    min_total = int(config.get("min_competitions", 0))
    requirements = config.get("required") or []

    total = _competition_count_from_panel(
        panel,
        season_codes=season_codes,
        competition_type_ids=competition_type_ids,
        segment_levels=segment_levels,
        include_qualifying_national=include_qualifying_national,
    )

    parts: list[str] = [f"{total}/{min_total} international competitions"]
    checks: list[bool] = [total >= min_total]

    for req in requirements:
        kind = str(req.get("kind") or "")
        min_n = int(req.get("min_competitions", 1))
        last_only = bool(req.get("last_season_only"))

        if kind == "segment_level":
            level = str(req.get("level") or "")
            count = _count_competitions_with_segment_level(
                panel,
                level,
                season_codes=season_codes,
                segment_levels=segment_levels,
                competition_type_ids=competition_type_ids,
                include_qualifying_national=include_qualifying_national,
                last_season_only=last_only,
            )
            label = f"{level} segment"
        elif kind == "segment_discipline_type_id":
            disc_type_id = int(req.get("discipline_type_id", 0))
            count = _count_competitions_with_segment_discipline_type(
                panel,
                disc_type_id,
                season_codes=season_codes,
                segment_levels=segment_levels,
                competition_type_ids=competition_type_ids,
                include_qualifying_national=include_qualifying_national,
                last_season_only=last_only,
            )
            label = "Pairs segment" if disc_type_id == 2 else f"discipline type {disc_type_id}"
        elif kind == "scope":
            scope = str(req.get("scope") or "")
            codes = [season_codes[-1]] if last_only and season_codes else season_codes
            count = _count_competitions_for_scope(
                panel,
                scope,
                season_codes=codes,
                segment_levels=segment_levels,
            )
            label = _scope_display_name(scope)
            if last_only:
                label += " (last season)"
        else:
            continue

        checks.append(count >= min_n)
        parts.append(f"{count}/{min_n} {label}")

    met = all(checks)
    detail = ", ".join(parts)
    return met, detail


def _evaluate_tc_ts_promote_isu(
    panel: pd.DataFrame,
    metric_config: Any,
    *,
    season_codes: list[int],
    segment_levels: frozenset[str],
    competition_type_ids: tuple[int, ...],
    include_qualifying_national: bool,
) -> tuple[bool, str]:
    """
    TC/TS ISU promotion: minimum competitions with at least one International Competition.

    Per ISU Rule 411, "International Competition" includes ISU Events (types 15–16) and
    International Senior/Junior Competitions (type 17), not type 17 alone.
    """
    config = _parse_metric_config(metric_config)
    min_total = int(config.get("min_competitions", 3))
    min_intl = int(config.get("min_international_competition", 1))

    total = _competition_count_after_rule411(
        panel,
        season_codes=season_codes,
        segment_levels=segment_levels,
        competition_type_ids=competition_type_ids,
        include_qualifying_national=include_qualifying_national,
    )
    intl_only = _competition_count_after_rule411(
        panel,
        season_codes=season_codes,
        segment_levels=segment_levels,
        scope="international_all",
    )

    met = total >= min_total and intl_only >= min_intl
    detail = (
        f"{total}/{min_total} competitions, "
        f"{intl_only}/{min_intl} International Competition(s) "
        "(ISU Event or International Competition)"
    )
    return met, detail


def get_panel_competitions_for_requirements(
    official_id: int,
    *,
    intl_appointment_type_id: int,
    directory_discipline_id: int | None,
    national_role_ids: tuple[int, ...],
    season_codes: list[int],
    competition_type_ids: tuple[int, ...],
    segment_levels: frozenset[str] | None = None,
    championship_or_olympic_only: bool = False,
    include_qualifying_national: bool = False,
) -> pd.DataFrame:
    """
    Distinct competitions where the official served in any of ``national_role_ids``
    on at least one qualifying segment (Junior/Senior).
    """
    cols = [
        "competition_id",
        "competition_year",
        "competition_name",
        "competition_type_id",
        "roles",
    ]
    if not activity_database_is_postgresql() or not national_role_ids or not season_codes:
        return pd.DataFrame(columns=cols)

    levels = sorted(segment_levels or COUNTABLE_SEGMENT_LEVELS)

    disc_clause, disc_params = _discipline_match_sql(
        intl_appointment_type_id=intl_appointment_type_id,
        directory_discipline_id=directory_discipline_id,
    )

    if include_qualifying_national:
        try:
            from activityAnalysis.international_officials_data import (
                _national_qualifying_competition_sql_or,
            )
        except ModuleNotFoundError:
            from international_officials_data import _national_qualifying_competition_sql_or

        comp_scope_sql = f"""
              AND (
                c.officials_analysis_competition_type_id IN :competition_type_ids
                {_national_qualifying_competition_sql_or()}
              )
"""
    else:
        comp_scope_sql = """
              AND c.officials_analysis_competition_type_id IN :competition_type_ids
"""

    stmt = (
        text(
            f"""
            SELECT
                c.id AS competition_id,
                c.year AS competition_year,
                c.name AS competition_name,
                c.officials_analysis_competition_type_id AS competition_type_id,
                string_agg(DISTINCT so.role, ', ' ORDER BY so.role) AS roles
            FROM public.segment_official so
            INNER JOIN public.segment s ON s.id = so.segment_id
            INNER JOIN public.competition c ON c.id = s.competition_id
            WHERE so.official_id = :official_id
              AND so.appointment_type_id IN :national_role_ids
{comp_scope_sql}              AND s.level IN :segment_levels
{season_year_sql_predicate()}              {disc_clause}
            GROUP BY c.id, c.year, c.name, c.officials_analysis_competition_type_id
            """
        )
        .bindparams(
            bindparam("official_id"),
            bindparam("national_role_ids", expanding=True),
            bindparam("competition_type_ids", expanding=True),
            bindparam("segment_levels", expanding=True),
            bindparam("season_year_codes", expanding=True),
        )
    )
    params: dict[str, Any] = {
        "official_id": int(official_id),
        "national_role_ids": [int(x) for x in national_role_ids],
        "competition_type_ids": [int(x) for x in competition_type_ids],
        "segment_levels": levels,
        "season_year_codes": season_codes_as_bind_strings(season_codes),
    }
    if "segment_discipline_type_ids" in disc_params:
        stmt = stmt.bindparams(bindparam("segment_discipline_type_ids", expanding=True))
        params["segment_discipline_type_ids"] = disc_params["segment_discipline_type_ids"]
    if "idvo_segment_discipline_type_ids" in disc_params:
        stmt = stmt.bindparams(bindparam("idvo_segment_discipline_type_ids", expanding=True))
        params["idvo_segment_discipline_type_ids"] = disc_params[
            "idvo_segment_discipline_type_ids"
        ]

    try:
        with Session(engine) as session:
            rows = session.execute(stmt, params).mappings().all()
    except Exception:
        return pd.DataFrame(columns=cols)

    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)
    if championship_or_olympic_only:
        df = df[
            df.apply(
                lambda r: _is_championship_or_olympic(
                    r["competition_type_id"], r["competition_name"]
                ),
                axis=1,
            )
        ]
    return df.reset_index(drop=True)


def _international_level_id(session: Session) -> int | None:
    row = session.execute(
        select(Levels.id).where(
            Levels.id == DIRECTORY_LEVEL_ID_INTERNATIONAL,
        )
    ).scalar()
    if row is not None:
        return int(row)
    row = session.execute(
        select(Levels.id).where(Levels.name.ilike(INTERNATIONAL_LEVEL_NAME)).limit(1)
    ).scalar()
    return int(row) if row is not None else DIRECTORY_LEVEL_ID_INTERNATIONAL


def _isu_level_id(session: Session) -> int | None:
    row = session.execute(
        select(Levels.id).where(
            Levels.id == DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP,
        )
    ).scalar()
    if row is not None:
        return int(row)
    row = session.execute(
        select(Levels.id).where(Levels.name.ilike(f"{ISU_LEVEL_NAME}%")).limit(1)
    ).scalar()
    return int(row) if row is not None else DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP


def _appointment_sport(directory_discipline_id: Any) -> Sport:
    """Figure skating (Rules 412–416) vs synchronized (Rules 828–862)."""
    disc = _nullable_int_for_sql(directory_discipline_id)
    return "synchronized" if disc == DIRECTORY_DISC_SYNCHRONIZED_ID else "figure"


def _official_is_isu_listed(
    session: Session,
    official_id: int,
    appointment_type_id: int,
    discipline_id: int | None,
) -> bool:
    """True when linked ISU roster shows the matching ISU appointment (best-effort)."""
    roster_type = _ISU_ROSTER_APPOINTMENT_TYPE.get(int(appointment_type_id))
    if not roster_type:
        return False

    disc_filter = ""
    params: dict[str, Any] = {"oid": int(official_id), "appt_type": roster_type}
    if discipline_id is not None:
        disc_filter = " AND lower(btrim(ioa.discipline)) = lower(btrim(:disc))"
        params["disc"] = _discipline_name_for_id(session, int(discipline_id))

    stmt = text(
        f"""
        SELECT 1
        FROM officials_analysis.isu_official_appointment ioa
        INNER JOIN officials_analysis.isu_official io ON io.id = ioa.isu_official_id
        WHERE lower(btrim(ioa.appointment_type)) = lower(btrim(:appt_type))
          AND lower(btrim(ioa.level)) = 'isu'
          AND (
              EXISTS (
                  SELECT 1 FROM public.segment_official so
                  WHERE so.isu_official_id = io.id AND so.official_id = :oid
              )
              OR EXISTS (
                  SELECT 1 FROM officials_analysis.officials o
                  WHERE o.id = :oid
                    AND lower(btrim(io.name_normalized)) = lower(btrim(
                        regexp_replace(coalesce(o.full_name, ''), '\\s+', ' ', 'g')
                    ))
              )
          )
          {disc_filter}
        LIMIT 1
        """
    )
    try:
        return session.execute(stmt, params).first() is not None
    except Exception:
        return False


def _discipline_name_for_id(session: Session, discipline_id: int) -> str:
    try:
        from activityAnalysis.officials_analysis_models import Disciplines
    except ModuleNotFoundError:
        from officials_analysis_models import Disciplines

    name = session.execute(
        select(Disciplines.name).where(Disciplines.id == int(discipline_id))
    ).scalar()
    return (name or "").strip()


def _load_rule_sets(purpose: Purpose | None = None) -> pd.DataFrame:
    purpose_clause = ""
    params: dict[str, Any] = {}
    if purpose is not None:
        purpose_clause = " AND rs.purpose = :purpose"
        params["purpose"] = purpose

    stmt = text(
        f"""
        SELECT
            rs.id AS rule_set_id,
            rs.isu_rule_ref,
            rs.purpose,
            rs.label,
            rs.appointment_type_id,
            rs.directory_level_id,
            rs.discipline_id,
            rs.listing_tier,
            rs.season_window,
            rs.sport,
            r.id AS rule_id,
            r.metric,
            r.min_value,
            r.role_appointment_type_ids,
            r.competition_type_ids,
            r.segment_levels,
            r.require_championship_or_olympic,
            r.include_qualifying_national,
            r.metric_config,
            r.display_label,
            r.sort_order AS rule_sort_order
        FROM officials_analysis.international_requirement_rule_set rs
        INNER JOIN officials_analysis.international_requirement_rule r
            ON r.rule_set_id = rs.id
        WHERE rs.active
        {purpose_clause}
        ORDER BY rs.sort_order, rs.id, r.sort_order, r.id
        """
    )
    try:
        with Session(engine) as session:
            rows = session.execute(stmt, params).mappings().all()
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _appointment_context(
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
) -> dict[str, Any]:
    """Directory appointment fields needed for requirement checks."""
    with Session(engine) as session:
        stmt = (
            select(
                Appointments.appointed_date,
                Appointments.achieved_date,
                Appointments.level_id,
                Levels.name.label("level_name"),
            )
            .outerjoin(Levels, Appointments.level_id == Levels.id)
            .where(
                Appointments.official_id == int(official_id),
                Appointments.appointment_type_id == int(appointment_type_id),
            )
        )
        dir_disc = _nullable_int_for_sql(discipline_id)
        if dir_disc is not None:
            stmt = stmt.where(Appointments.discipline_id == dir_disc)
        stmt = stmt.order_by(Appointments.active.desc(), Appointments.appointed_date.desc().nulls_last())
        row = session.execute(stmt).mappings().first()

    if not row:
        return {}
    return dict(row)


def _batch_appointment_contexts(
    official_ids: list[int],
) -> dict[tuple[int, int, int | None], dict[str, Any]]:
    ids = sorted({int(x) for x in official_ids if x is not None})
    if not ids:
        return {}

    with Session(engine) as session:
        stmt = (
            select(
                Appointments.official_id,
                Appointments.appointment_type_id,
                Appointments.discipline_id,
                Appointments.appointed_date,
                Appointments.achieved_date,
                Appointments.level_id,
                Levels.name.label("level_name"),
                Appointments.active,
            )
            .outerjoin(Levels, Appointments.level_id == Levels.id)
            .where(
                Appointments.official_id.in_(ids),
                Appointments.appointment_type_id.in_(list(_ISU_ROSTER_APPOINTMENT_TYPE.keys())),
            )
            .order_by(
                Appointments.official_id,
                Appointments.appointment_type_id,
                Appointments.discipline_id,
                Appointments.active.desc(),
                Appointments.appointed_date.desc().nulls_last(),
            )
        )
        rows = session.execute(stmt).mappings().all()

    out: dict[tuple[int, int, int | None], dict[str, Any]] = {}
    for row in rows:
        key = (
            int(row["official_id"]),
            int(row["appointment_type_id"]),
            _nullable_int_for_sql(row["discipline_id"]),
        )
        if key not in out:
            out[key] = dict(row)
    return out


def _appointment_context_from_batch(
    contexts: dict[tuple[int, int, int | None], dict[str, Any]],
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
) -> dict[str, Any]:
    key = (
        int(official_id),
        int(appointment_type_id),
        _nullable_int_for_sql(discipline_id),
    )
    if key in contexts:
        return contexts[key]
    # Collapsed IDVO summary rows use discipline_id NULL; directory stores per-discipline rows.
    if (
        _nullable_int_for_sql(discipline_id) is None
        and int(appointment_type_id) == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID
    ):
        candidates = [
            ctx
            for (oid, atid, _disc), ctx in contexts.items()
            if oid == int(official_id) and atid == int(appointment_type_id)
        ]
        if not candidates:
            return {}
        for ctx in candidates:
            if _nullable_int_for_sql(ctx.get("level_id")) == DIRECTORY_LEVEL_ID_ISU_CHAMPIONSHIP:
                return ctx
        return candidates[0]
    return {}


def _batch_isu_listing_keys(official_ids: list[int]) -> set[tuple[int, int, int | None]]:
    """(official_id, directory appointment_type_id, discipline_id) with ISU roster listing."""
    ids = sorted({int(x) for x in official_ids if x is not None})
    if not ids:
        return set()

    roster_to_appt = {v.lower(): k for k, v in _ISU_ROSTER_APPOINTMENT_TYPE.items()}

    try:
        with Session(engine) as session:
            try:
                from activityAnalysis.officials_analysis_models import Disciplines
            except ModuleNotFoundError:
                from officials_analysis_models import Disciplines

            disc_rows = session.execute(select(Disciplines.id, Disciplines.name)).all()
            disc_by_name = {
                (name or "").strip().lower(): int(did)
                for did, name in disc_rows
                if name
            }

            stmt = text(
                """
                SELECT DISTINCT
                    o.id AS official_id,
                    ioa.appointment_type,
                    ioa.discipline
                FROM officials_analysis.isu_official_appointment ioa
                INNER JOIN officials_analysis.isu_official io ON io.id = ioa.isu_official_id
                INNER JOIN officials_analysis.officials o ON (
                    EXISTS (
                        SELECT 1 FROM public.segment_official so
                        WHERE so.isu_official_id = io.id AND so.official_id = o.id
                    )
                    OR lower(btrim(io.name_normalized)) = lower(btrim(
                        regexp_replace(coalesce(o.full_name, ''), '\\s+', ' ', 'g')
                    ))
                )
                WHERE o.id IN :official_ids
                  AND lower(btrim(ioa.level)) = 'isu'
                """
            ).bindparams(bindparam("official_ids", expanding=True))
            rows = session.execute(stmt, {"official_ids": ids}).mappings().all()
    except Exception:
        return set()

    keys: set[tuple[int, int, int | None]] = set()
    for row in rows:
        appt_type = roster_to_appt.get((row["appointment_type"] or "").strip().lower())
        if appt_type is None:
            continue
        disc_name = (row["discipline"] or "").strip().lower()
        disc_id = disc_by_name.get(disc_name) if disc_name else None
        keys.add((int(row["official_id"]), appt_type, disc_id))
    return keys


def _years_in_grade_for_current_appointment(
    appt_ctx: dict[str, Any],
    *,
    listing_season_code: int,
) -> int:
    grade_date = grade_date_for_time_in_grade(
        appt_ctx.get("achieved_date"),
        appt_ctx.get("appointed_date"),
    )
    return years_in_grade_at_listing(grade_date, listing_season_code=listing_season_code) or 0


def _tc_prerequisite_years_count(
    appointment_rows: list[dict[str, Any]],
    metric_config: Any,
    *,
    directory_discipline_id: Any,
    listing_season_code: int,
    international_level_id: int,
    isu_level_id: int,
) -> tuple[int, int, int, int]:
    """Intl/ISU Judge, Intl Referee, ISU TS years and best-of for TC promote prerequisite."""
    config = _parse_metric_config(metric_config)
    related = config.get("related_discipline_ids")
    if related:
        judge_referee_discipline_ids = [int(x) for x in related]
    else:
        judge_referee_discipline_ids = related_discipline_ids_for_tc_prerequisite(
            directory_discipline_id
        )
    ts_related = config.get("ts_discipline_ids")
    if ts_related:
        ts_discipline_ids = [int(x) for x in ts_related]
    else:
        ts_disc = _nullable_int_for_sql(directory_discipline_id)
        ts_discipline_ids = [ts_disc] if ts_disc is not None else []

    judge_referee_ids = judge_referee_discipline_ids or []
    judge_years = (
        max_years_for_tc_prerequisite_role(
            appointment_rows,
            listing_season_code=listing_season_code,
            appointment_type_id=APPOINTMENT_TYPE_ID_JUDGE,
            discipline_ids=judge_referee_ids,
            international_level_id=int(international_level_id),
            isu_level_id=int(isu_level_id),
        )
        if judge_referee_ids
        else 0
    )
    referee_years = (
        max_years_for_tc_prerequisite_role(
            appointment_rows,
            listing_season_code=listing_season_code,
            appointment_type_id=APPOINTMENT_TYPE_ID_REFEREE,
            discipline_ids=judge_referee_ids,
            international_level_id=int(international_level_id),
            isu_level_id=int(isu_level_id),
        )
        if judge_referee_ids
        else 0
    )
    ts_years = 0
    if ts_discipline_ids:
        ts_years = max_years_for_appointment_criteria(
            appointment_rows,
            listing_season_code=listing_season_code,
            appointment_type_id=APPOINTMENT_TYPE_ID_TECHNICAL_SPECIALIST,
            level_id=int(isu_level_id),
            discipline_ids=ts_discipline_ids,
        )
    return judge_years, referee_years, ts_years, max(judge_years, referee_years, ts_years)


def _evaluate_years_tc_prerequisite(
    appointment_rows: list[dict[str, Any]],
    metric_config: Any,
    *,
    directory_discipline_id: Any,
    listing_season_code: int,
    international_level_id: int,
    isu_level_id: int,
) -> tuple[int, str]:
    """TC promote: Judge/Referee (Intl+ISU) or ISU TS years in related disciplines."""
    judge_years, referee_years, ts_years, actual = _tc_prerequisite_years_count(
        appointment_rows,
        metric_config,
        directory_discipline_id=directory_discipline_id,
        listing_season_code=listing_season_code,
        international_level_id=international_level_id,
        isu_level_id=isu_level_id,
    )
    try:
        from activityAnalysis.international_listing_seasons import format_listing_reference_july1
    except ModuleNotFoundError:
        from international_listing_seasons import format_listing_reference_july1

    detail = (
        f"years ({format_listing_reference_july1(listing_season_code)}; "
        f"Judge {judge_years}, Intl Referee {referee_years}, ISU TS {ts_years})"
    )
    return actual, detail


def _promote_year_rule_rows_for_appointment(
    rules_df: pd.DataFrame,
    *,
    official_id: int,
    appointment_type_id: int,
    directory_discipline_id: Any,
    appointment_level_id: Any,
    international_level_id: int,
    isu_level_id: int,
    isu_listing_keys: set[tuple[int, int, int | None]] | None,
) -> list[pd.Series]:
    """Active promote rules with year-based metrics that apply to this appointment."""
    if rules_df.empty or "purpose" not in rules_df.columns:
        promote = rules_df
    else:
        promote = rules_df.loc[rules_df["purpose"] == "promote"]
    if promote.empty:
        return []

    out: list[pd.Series] = []
    for rule_set_id, group in promote.groupby("rule_set_id", sort=False):
        head = group.iloc[0]
        if int(head["appointment_type_id"]) != int(appointment_type_id):
            continue
        applies, _ = _rule_set_applies(
            head,
            appointment_type_id=appointment_type_id,
            directory_discipline_id=directory_discipline_id,
            appointment_level_id=_nullable_int_for_sql(appointment_level_id),
            international_level_id=international_level_id,
            isu_level_id=isu_level_id,
            purpose="promote",
            official_id=official_id,
            isu_listing_keys=isu_listing_keys,
        )
        if not applies:
            continue
        for _, rule_row in group.sort_values("rule_sort_order").iterrows():
            if str(rule_row["metric"]) in PROMOTE_YEARS_METRICS:
                out.append(rule_row)
    return out


def _first_listing_season_for_promote_year_rule(
    rule_row: pd.Series,
    *,
    appointment_rows: list[dict[str, Any]],
    appt_ctx: dict[str, Any],
    directory_discipline_id: Any,
    international_level_id: int,
    isu_level_id: int,
) -> int | None:
    """First listing season when one promote year rule is satisfied."""
    metric = str(rule_row["metric"])
    min_years = int(rule_row["min_value"])

    if metric == "years_in_grade":
        grade_date = grade_date_for_time_in_grade(
            appt_ctx.get("achieved_date"),
            appt_ctx.get("appointed_date"),
        )
        return first_listing_season_meeting_min_years_on_grade_date(grade_date, min_years)

    if metric == "years_isu_judge":
        return first_listing_season_meeting_appointment_criteria(
            appointment_rows,
            min_years,
            appointment_type_id=APPOINTMENT_TYPE_ID_JUDGE,
            level_id=int(isu_level_id),
        )

    if metric == "years_intl_referee":
        disc = _nullable_int_for_sql(directory_discipline_id)
        return first_listing_season_meeting_appointment_criteria(
            appointment_rows,
            min_years,
            appointment_type_id=APPOINTMENT_TYPE_ID_REFEREE,
            level_id=int(international_level_id),
            discipline_ids=[disc] if disc is not None else None,
        )

    if metric == "years_tc_prerequisite":
        config = _parse_metric_config(rule_row.get("metric_config"))
        related = config.get("related_discipline_ids")
        if related:
            discipline_ids = [int(x) for x in related]
        else:
            discipline_ids = related_discipline_ids_for_tc_prerequisite(
                directory_discipline_id
            )
        try:
            from activityAnalysis.international_listing_seasons import (
                listing_season_codes_for_projection,
            )
        except ModuleNotFoundError:
            from international_listing_seasons import listing_season_codes_for_projection

        for code in listing_season_codes_for_projection():
            _, _, _, actual = _tc_prerequisite_years_count(
                appointment_rows,
                rule_row.get("metric_config"),
                directory_discipline_id=directory_discipline_id,
                listing_season_code=int(code),
                international_level_id=international_level_id,
                isu_level_id=isu_level_id,
            )
            if actual >= min_years:
                return int(code)
        return None

    return None


def first_listing_season_eligible_for_promote_years(
    *,
    official_id: int,
    appointment_type_id: int,
    directory_discipline_id: Any,
    appointment_level: Any,
    appointment_level_id: Any,
    appointment_rows: list[dict[str, Any]],
    appt_ctx: dict[str, Any],
    listing_season_code: int,
    rules_df: pd.DataFrame | None,
    international_level_id: int,
    isu_level_id: int,
    isu_listing_keys: set[tuple[int, int, int | None]] | None,
) -> int | None:
    """
    First listing season when all promote year requirements are met.

    International IDVO promote rules are competition-based only (no year metrics);
    in that case the first promote year is the current listing season.

    Returns ``None`` when promotion does not apply or required dates are missing.
    """
    if not should_evaluate_promote_requirements(
        official_id,
        appointment_type_id,
        directory_discipline_id,
        appointment_level,
        appointment_level_id,
        isu_level_id=isu_level_id,
        isu_listing_keys=isu_listing_keys,
    ):
        return None

    active_rules = rules_df if rules_df is not None else _load_rule_sets(purpose="promote")
    year_rules = _promote_year_rule_rows_for_appointment(
        active_rules,
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        directory_discipline_id=directory_discipline_id,
        appointment_level_id=appointment_level_id,
        international_level_id=international_level_id,
        isu_level_id=isu_level_id,
        isu_listing_keys=isu_listing_keys,
    )
    if not year_rules:
        if int(appointment_type_id) == INTERNATIONAL_DATA_OPERATOR_APPOINTMENT_TYPE_ID:
            return int(listing_season_code)
        return None

    first_seasons: list[int] = []
    for rule_row in year_rules:
        first = _first_listing_season_for_promote_year_rule(
            rule_row,
            appointment_rows=appointment_rows,
            appt_ctx=appt_ctx,
            directory_discipline_id=directory_discipline_id,
            international_level_id=international_level_id,
            isu_level_id=isu_level_id,
        )
        if first is None:
            return None
        first_seasons.append(int(first))
    return max(first_seasons)


def enrich_summary_with_promote_first_eligible(
    summary: pd.DataFrame,
    *,
    listing_season_code: int,
    rules_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Add ``promote_first_eligible_listing`` (USFS season code int, e.g. 2627)."""
    if summary.empty or not activity_database_is_postgresql():
        return summary

    out = summary.copy()
    official_ids = out["official_id"].astype(int).unique().tolist()
    appointment_rows_by_official = load_official_international_appointment_rows(
        official_ids
    )
    appointment_contexts = _batch_appointment_contexts(official_ids)
    isu_listing_keys = _batch_isu_listing_keys(official_ids)
    active_rules = rules_df if rules_df is not None else _load_rule_sets()

    with Session(engine) as session:
        international_level_id = _international_level_id(session)
        isu_level_id = _isu_level_id(session)

    values: list[int | None] = []
    for _, row in out.iterrows():
        oid = int(row["official_id"])
        atid = int(row["appointment_type_id"])
        disc = row.get("discipline_id")
        appt_ctx = _appointment_context_from_batch(
            appointment_contexts, oid, atid, disc
        )
        first = first_listing_season_eligible_for_promote_years(
            official_id=oid,
            appointment_type_id=atid,
            directory_discipline_id=disc,
            appointment_level=row.get("appointment_level") or "",
            appointment_level_id=row.get("appointment_level_id"),
            appointment_rows=appointment_rows_by_official.get(oid, []),
            appt_ctx=appt_ctx,
            listing_season_code=listing_season_code,
            rules_df=active_rules,
            international_level_id=international_level_id,
            isu_level_id=isu_level_id,
            isu_listing_keys=isu_listing_keys,
        )
        values.append(first)
    out["promote_first_eligible_listing"] = values
    return out


def _seminars_for_rule_evaluation(
    seminars_bulk: pd.DataFrame | None,
    *,
    official_id: int,
    appointment_type_id: int,
    directory_discipline_id: Any,
    seminars_by_official: dict[int, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    if seminars_bulk is not None:
        if seminars_by_official is not None:
            sub = seminars_by_official.get(int(official_id), seminars_bulk.iloc[0:0])
        else:
            sub = seminars_bulk.loc[seminars_bulk["official_id"] == int(official_id)]
        return filter_seminars_for_appointment(
            sub,
            official_id=official_id,
            appointment_type_id=appointment_type_id,
            directory_discipline_id=directory_discipline_id,
        )
    return filter_seminars_for_appointment(
        load_official_seminars_bulk([official_id]),
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        directory_discipline_id=directory_discipline_id,
    )


def _seminar_season_codes_for_window(
    listing_season_code: int,
    season_window: int,
) -> list[int]:
    return season_codes_preceding_listing(int(listing_season_code), int(season_window))


def _seminar_delivery_label(in_person: bool | None, *, at_event: bool | None = None) -> str:
    if at_event:
        return "at event"
    if in_person is None:
        return "seminar"
    return "in person" if in_person else "online"


def _seminar_in_person_from_config(config: dict[str, Any]) -> bool | None:
    if "in_person" not in config:
        return None
    return bool(config["in_person"])


def _evaluate_seminar_count(
    seminars: pd.DataFrame,
    metric_config: Any,
    *,
    listing_season_code: int,
    min_value: int,
) -> tuple[bool, str]:
    config = _parse_metric_config(metric_config)
    in_person = _seminar_in_person_from_config(config)
    at_event = bool(config["at_event"]) if "at_event" in config else None
    season_window = int(config.get("season_window") or 0)
    if season_window <= 0:
        return False, "Seminar season window not configured"
    codes = _seminar_season_codes_for_window(listing_season_code, season_window)
    matched = filter_seminars_to_season_codes(
        seminars, codes, in_person=in_person, at_event=at_event
    )
    actual = len(matched)
    met = actual >= int(min_value)
    seasons_label = ", ".join(format_usfs_season_code(c) for c in codes)
    detail = (
        f"{actual}/{min_value} {_seminar_delivery_label(in_person, at_event=at_event)}"
        f" seminar{'s' if min_value != 1 else ''} ({seasons_label})"
    )
    return met, detail


def _evaluate_seminar_alternatives(
    seminars: pd.DataFrame,
    metric_config: Any,
    *,
    listing_season_code: int,
) -> tuple[bool, str, str]:
    config = _parse_metric_config(metric_config)
    alternatives = config.get("alternatives") or []
    if not alternatives:
        return False, "", "No seminar alternatives configured"

    branch_summaries: list[str] = []
    winning: tuple[str, str] | None = None
    for alt in alternatives:
        label = str(alt.get("label") or "Option")
        requirements = alt.get("requirements") or []
        branch_ok = True
        parts: list[str] = []
        for req in requirements:
            in_person = _seminar_in_person_from_config(req)
            at_event = bool(req["at_event"]) if "at_event" in req else None
            season_window = int(req.get("season_window") or 0)
            min_count = int(req.get("min") or 1)
            if season_window <= 0:
                branch_ok = False
                parts.append("season window not configured")
                continue
            codes = _seminar_season_codes_for_window(listing_season_code, season_window)
            matched = filter_seminars_to_season_codes(
                seminars, codes, in_person=in_person, at_event=at_event
            )
            actual = len(matched)
            seasons_label = ", ".join(format_usfs_season_code(c) for c in codes)
            parts.append(
                f"{actual}/{min_count} {_seminar_delivery_label(in_person, at_event=at_event)} "
                f"({seasons_label})"
            )
            if actual < min_count:
                branch_ok = False
        summary = f"{label}: {', '.join(parts)}"
        branch_summaries.append(summary)
        if branch_ok and winning is None:
            winning = (label, ", ".join(parts))

    if winning is not None:
        label, parts = winning
        others = [s for s in branch_summaries if not s.startswith(f"{label}:")]
        detail = f"Meets via {label} ({parts})"
        if others:
            detail += " — also checked: " + "; ".join(others)
        return True, label, detail

    return False, "", "Need one of: " + "; ".join(branch_summaries)


def format_seminar_alternatives_detail(detail: str, *, met: bool) -> list[str]:
    if not detail:
        return []
    if met and detail.startswith("Meets via "):
        return [detail.replace("Meets via ", "Satisfied by: ", 1)]
    if detail.startswith("Need one of: "):
        return [
            part.strip()
            for part in detail[len("Need one of: ") :].split("; ")
            if part.strip()
        ]
    return [detail]


def _panel_for_rule_evaluation(
    panel_bulk: pd.DataFrame | None,
    *,
    official_id: int,
    intl_appointment_type_id: int,
    directory_discipline_id: Any,
    national_role_ids: tuple[int, ...] | None,
    panel_by_official: dict[int, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    if panel_bulk is not None:
        if panel_by_official is not None:
            sub = panel_by_official.get(int(official_id), panel_bulk.iloc[0:0])
        else:
            sub = panel_bulk.loc[panel_bulk["official_id"] == int(official_id)]
        return filter_panel_for_appointment(
            sub,
            official_id=official_id,
            intl_appointment_type_id=intl_appointment_type_id,
            directory_discipline_id=directory_discipline_id,
            national_role_ids=national_role_ids,
        )
    full_panel = load_international_panel_segments_bulk([official_id])
    return filter_panel_for_appointment(
        full_panel,
        official_id=official_id,
        intl_appointment_type_id=intl_appointment_type_id,
        directory_discipline_id=directory_discipline_id,
        national_role_ids=national_role_ids,
    )


def _evaluate_rule(
    rule_row: pd.Series,
    *,
    official_id: int,
    appointment_type_id: int,
    directory_discipline_id: Any,
    season_codes: list[int],
    listing_season_code: int,
    appt_ctx: dict[str, Any],
    appointment_rows: list[dict[str, Any]],
    international_level_id: int,
    isu_level_id: int,
    panel_bulk: pd.DataFrame | None = None,
    panel_by_official: dict[int, pd.DataFrame] | None = None,
    seminars_bulk: pd.DataFrame | None = None,
    seminars_by_official: dict[int, pd.DataFrame] | None = None,
) -> RuleCheckResult:
    metric = str(rule_row["metric"])
    min_value = int(rule_row["min_value"])
    display = user_facing_requirement_label(str(rule_row["display_label"] or metric))
    roles = tuple(int(x) for x in (rule_row["role_appointment_type_ids"] or []) if x is not None)
    comp_types = tuple(int(x) for x in (rule_row["competition_type_ids"] or [15, 16, 17]))
    seg_levels = frozenset(rule_row["segment_levels"] or list(COUNTABLE_SEGMENT_LEVELS))
    include_qualifying_national = bool(rule_row.get("include_qualifying_national"))

    if metric in ("seminar_count", "seminar_alternatives"):
        seminars = _seminars_for_rule_evaluation(
            seminars_bulk,
            official_id=official_id,
            appointment_type_id=appointment_type_id,
            directory_discipline_id=directory_discipline_id,
            seminars_by_official=seminars_by_official,
        )
        if metric == "seminar_count":
            met, detail = _evaluate_seminar_count(
                seminars,
                rule_row.get("metric_config"),
                listing_season_code=int(listing_season_code),
                min_value=min_value,
            )
        else:
            met, _via, detail = _evaluate_seminar_alternatives(
                seminars,
                rule_row.get("metric_config"),
                listing_season_code=int(listing_season_code),
            )
        actual = 1 if met else 0
        return RuleCheckResult(metric, display, min_value, actual, met, detail)

    if metric == "seasons_since_appointed":
        actual = _years_in_grade_for_current_appointment(
            appt_ctx, listing_season_code=listing_season_code
        )
        met = actual >= min_value
        detail = years_detail_label(
            actual=actual, required=min_value, listing_season_code=listing_season_code
        )
        return RuleCheckResult(metric, display, min_value, actual, met, detail)

    if metric == "years_in_grade":
        actual = _years_in_grade_for_current_appointment(
            appt_ctx, listing_season_code=listing_season_code
        )
        met = actual >= min_value
        detail = years_detail_label(
            actual=actual, required=min_value, listing_season_code=listing_season_code
        )
        return RuleCheckResult(metric, display, min_value, actual, met, detail)

    if metric == "years_isu_judge":
        actual = max_years_for_appointment_criteria(
            appointment_rows,
            listing_season_code=listing_season_code,
            appointment_type_id=APPOINTMENT_TYPE_ID_JUDGE,
            level_id=int(isu_level_id),
        )
        met = actual >= min_value
        detail = years_detail_label(
            actual=actual, required=min_value, listing_season_code=listing_season_code
        )
        return RuleCheckResult(metric, display, min_value, actual, met, detail)

    if metric == "years_intl_referee":
        disc = _nullable_int_for_sql(directory_discipline_id)
        actual = max_years_for_appointment_criteria(
            appointment_rows,
            listing_season_code=listing_season_code,
            appointment_type_id=APPOINTMENT_TYPE_ID_REFEREE,
            level_id=int(international_level_id),
            discipline_ids=[disc] if disc is not None else None,
        )
        met = actual >= min_value
        detail = years_detail_label(
            actual=actual, required=min_value, listing_season_code=listing_season_code
        )
        return RuleCheckResult(metric, display, min_value, actual, met, detail)

    if metric == "years_tc_prerequisite":
        actual, detail_suffix = _evaluate_years_tc_prerequisite(
            appointment_rows,
            rule_row.get("metric_config"),
            directory_discipline_id=directory_discipline_id,
            listing_season_code=listing_season_code,
            international_level_id=international_level_id,
            isu_level_id=isu_level_id,
        )
        met = actual >= min_value
        detail = f"{actual}/{min_value} {detail_suffix}"
        return RuleCheckResult(metric, display, min_value, actual, met, detail)

    role_ids = _role_ids_for_metric(metric, roles)
    panel = _panel_for_rule_evaluation(
        panel_bulk,
        official_id=official_id,
        intl_appointment_type_id=appointment_type_id,
        directory_discipline_id=directory_discipline_id,
        national_role_ids=role_ids,
        panel_by_official=panel_by_official,
    )

    if metric == "judge_promote_isu":
        met, detail = _evaluate_judge_promote_isu(
            panel,
            rule_row.get("metric_config"),
            season_codes=season_codes,
            segment_levels=seg_levels,
            competition_type_ids=comp_types,
            include_qualifying_national=include_qualifying_national,
        )
        qualifying = _qualifying_competitions_from_panel(
            panel,
            season_codes=season_codes,
            competition_type_ids=comp_types,
            segment_levels=seg_levels,
            include_qualifying_national=include_qualifying_national,
        )
        actual = 1 if met else 0
        return RuleCheckResult(
            metric, display, 1, actual, met, detail, qualifying_competitions=qualifying
        )

    if metric == "tc_ts_promote_isu":
        met, detail = _evaluate_tc_ts_promote_isu(
            panel,
            rule_row.get("metric_config"),
            season_codes=season_codes,
            segment_levels=seg_levels,
            competition_type_ids=comp_types,
            include_qualifying_national=include_qualifying_national,
        )
        qualifying = _qualifying_competitions_from_panel(
            panel,
            season_codes=season_codes,
            competition_type_ids=comp_types,
            segment_levels=seg_levels,
            include_qualifying_national=include_qualifying_national,
        )
        actual = 1 if met else 0
        return RuleCheckResult(
            metric, display, 1, actual, met, detail, qualifying_competitions=qualifying
        )

    if metric == "competition_alternatives":
        met, _via, detail = _evaluate_competition_alternatives(
            panel,
            rule_row.get("metric_config"),
            season_codes=season_codes,
            segment_levels=seg_levels,
            intl_appointment_type_id=appointment_type_id,
        )
        qualifying = _qualifying_competitions_for_alternatives(
            panel,
            season_codes=season_codes,
            segment_levels=seg_levels,
        )
        actual = 1 if met else 0
        return RuleCheckResult(
            metric, display, 1, actual, met, detail, qualifying_competitions=qualifying
        )

    championship_only = bool(rule_row.get("require_championship_or_olympic")) or metric == (
        "judge_championship_or_olympic"
    )

    qualifying = _qualifying_competitions_from_panel(
        panel,
        season_codes=season_codes,
        competition_type_ids=comp_types,
        segment_levels=seg_levels,
        championship_or_olympic_only=championship_only,
        include_qualifying_national=include_qualifying_national,
    )
    actual = int(qualifying["competition_id"].nunique()) if not qualifying.empty else 0

    met = actual >= min_value
    detail = f"{actual}/{min_value} competitions ({', '.join(str(c) for c in season_codes)})"
    return RuleCheckResult(
        metric,
        display,
        min_value,
        actual,
        met,
        detail,
        qualifying_competitions=qualifying,
    )


def _rule_set_applies(
    rule_set: pd.Series,
    *,
    appointment_type_id: int,
    directory_discipline_id: Any,
    appointment_level_id: int | None,
    international_level_id: int | None,
    isu_level_id: int | None,
    purpose: Purpose,
    official_id: int,
    isu_listing_keys: set[tuple[int, int, int | None]] | None = None,
) -> tuple[bool, str]:
    if int(rule_set["appointment_type_id"]) != int(appointment_type_id):
        return False, "appointment type mismatch"

    rs_sport = str(rule_set.get("sport") or "figure")
    appt_sport = _appointment_sport(directory_discipline_id)
    if rs_sport != appt_sport:
        return False, f"sport mismatch ({rs_sport} vs {appt_sport})"

    rs_disc = rule_set.get("discipline_id")
    if pd.notna(rs_disc):
        appt_disc = _nullable_int_for_sql(directory_discipline_id)
        if appt_disc is None or int(rs_disc) != appt_disc:
            return False, "discipline mismatch"

    if purpose == "promote":
        req_level = rule_set.get("directory_level_id")
        resolved_level = _nullable_int_for_sql(appointment_level_id)
        intl_id = (
            int(international_level_id)
            if international_level_id is not None
            else DIRECTORY_LEVEL_ID_INTERNATIONAL
        )
        if pd.notna(req_level) and international_level_id is not None:
            if resolved_level != intl_id:
                return False, "promote applies at International level only"
        elif international_level_id is not None and resolved_level != intl_id:
            return False, "promote applies at International level only"

    listing_tier = str(rule_set.get("listing_tier") or "international")
    if listing_tier == "isu":
        # Directory ISU appointments: evaluate ISU maintain rules without roster link.
        if (
            purpose == "maintain"
            and isu_level_id is not None
            and appointment_level_id is not None
            and appointment_level_id == isu_level_id
        ):
            return True, ""

        disc = _nullable_int_for_sql(directory_discipline_id)
        if isu_listing_keys is not None:
            listed = (
                (int(official_id), int(appointment_type_id), disc) in isu_listing_keys
                if disc is not None
                else any(
                    k[0] == int(official_id) and k[1] == int(appointment_type_id)
                    for k in isu_listing_keys
                )
            )
            if not listed:
                return False, "ISU listing not found on ISU roster"
        else:
            with Session(engine) as session:
                if not _official_is_isu_listed(
                    session,
                    official_id,
                    appointment_type_id,
                    disc,
                ):
                    return False, "ISU listing not found on ISU roster"

    return True, ""


def evaluate_requirements_for_appointment(
    official_id: int,
    appointment_type_id: int,
    directory_discipline_id: Any,
    purpose: Purpose,
    *,
    listing_season_code: int | None = None,
    rules_df: pd.DataFrame | None = None,
    panel_bulk: pd.DataFrame | None = None,
    panel_by_official: dict[int, pd.DataFrame] | None = None,
    seminars_bulk: pd.DataFrame | None = None,
    seminars_by_official: dict[int, pd.DataFrame] | None = None,
    appointment_contexts: dict[tuple[int, int, int | None], dict[str, Any]] | None = None,
    appointment_rows_by_official: dict[int, list[dict[str, Any]]] | None = None,
    isu_listing_keys: set[tuple[int, int, int | None]] | None = None,
    international_level_id: int | None = None,
    isu_level_id: int | None = None,
) -> list[RequirementEvaluation]:
    """Evaluate all active rule sets matching this appointment row and purpose."""
    if not activity_database_is_postgresql():
        return []

    listing_season_code = (
        int(listing_season_code)
        if listing_season_code is not None
        else REPORT_LISTING_SEASON_DEFAULT
    )
    if rules_df is None:
        active_rules = _load_rule_sets(purpose=purpose)
    elif "purpose" in rules_df.columns:
        active_rules = rules_df.loc[rules_df["purpose"] == purpose]
    else:
        active_rules = rules_df
    if active_rules.empty:
        return []

    if appointment_contexts is not None:
        appt_ctx = _appointment_context_from_batch(
            appointment_contexts,
            official_id,
            appointment_type_id,
            directory_discipline_id,
        )
    else:
        appt_ctx = _appointment_context(official_id, appointment_type_id, directory_discipline_id)
    appointment_level_id = appt_ctx.get("level_id")

    if appointment_rows_by_official is not None:
        appointment_rows = appointment_rows_by_official.get(int(official_id), [])
    else:
        appointment_rows = load_official_international_appointment_rows(
            [int(official_id)]
        ).get(int(official_id), [])

    if international_level_id is None or isu_level_id is None:
        with Session(engine) as session:
            if international_level_id is None:
                international_level_id = _international_level_id(session)
            if isu_level_id is None:
                isu_level_id = _isu_level_id(session)

    out: list[RequirementEvaluation] = []
    for rule_set_id, group in active_rules.groupby("rule_set_id", sort=False):
        head = group.iloc[0]
        applies, skip_reason = _rule_set_applies(
            head,
            appointment_type_id=appointment_type_id,
            directory_discipline_id=directory_discipline_id,
            appointment_level_id=appointment_level_id,
            international_level_id=international_level_id,
            isu_level_id=isu_level_id,
            purpose=purpose,
            official_id=official_id,
            isu_listing_keys=isu_listing_keys,
        )
        season_window = int(head["season_window"])
        season_codes = season_codes_preceding_listing(listing_season_code, season_window)

        if not applies:
            out.append(
                RequirementEvaluation(
                    rule_set_id=int(rule_set_id),
                    isu_rule_ref=str(head["isu_rule_ref"]),
                    purpose=purpose,
                    label=str(head["label"]),
                    listing_tier=str(head["listing_tier"]),
                    season_window=season_window,
                    season_codes=season_codes,
                    meets=False,
                    summary_note=skip_reason,
                    not_applicable=True,
                    not_applicable_reason=skip_reason,
                )
            )
            continue

        rule_results: list[RuleCheckResult] = []
        for _, rule_row in group.sort_values("rule_sort_order").iterrows():
            rule_results.append(
                _evaluate_rule(
                    rule_row,
                    official_id=official_id,
                    appointment_type_id=appointment_type_id,
                    directory_discipline_id=directory_discipline_id,
                    season_codes=season_codes,
                    listing_season_code=listing_season_code,
                    appt_ctx=appt_ctx,
                    appointment_rows=appointment_rows,
                    international_level_id=int(international_level_id),
                    isu_level_id=int(isu_level_id),
                    panel_bulk=panel_bulk,
                    panel_by_official=panel_by_official,
                    seminars_bulk=seminars_bulk,
                    seminars_by_official=seminars_by_official,
                )
            )

        meets = all(r.met for r in rule_results)
        failed = [r for r in rule_results if not r.met]
        if meets:
            summary = "Meets requirements"
        else:
            summary = "; ".join(r.detail for r in failed[:3])
            if len(failed) > 3:
                summary += f" (+{len(failed) - 3} more)"

        out.append(
            RequirementEvaluation(
                rule_set_id=int(rule_set_id),
                isu_rule_ref=str(head["isu_rule_ref"]),
                purpose=purpose,
                label=str(head["label"]),
                listing_tier=str(head["listing_tier"]),
                season_window=season_window,
                season_codes=season_codes,
                meets=meets,
                summary_note=summary,
                rule_results=rule_results,
                qualifying_activity=_union_qualifying_competitions(rule_results),
            )
        )
    return out


def _primary_requirement_evaluation(
    evals: list[RequirementEvaluation],
) -> RequirementEvaluation | None:
    """Prefer the first applicable rule set; fall back to first N/A entry."""
    if not evals:
        return None
    applicable = [e for e in evals if not e.not_applicable]
    return applicable[0] if applicable else evals[0]


def evaluate_requirements_summary_df(
    summary: pd.DataFrame,
    *,
    panel_bulk: pd.DataFrame | None = None,
    seminars_bulk: pd.DataFrame | None = None,
    listing_season_code: int | None = None,
) -> pd.DataFrame:
    """
    Add maintain / promote columns to an international activity summary DataFrame.
    Expects columns: official_id, appointment_type_id, discipline_id.
    """
    if summary.empty:
        return summary

    listing_season_code = (
        int(listing_season_code)
        if listing_season_code is not None
        else REPORT_LISTING_SEASON_DEFAULT
    )
    rules_df = _load_rule_sets()
    official_ids = summary["official_id"].astype(int).unique().tolist()
    panel = panel_bulk if panel_bulk is not None else load_international_panel_segments_bulk(official_ids)
    panel_by_official: dict[int, pd.DataFrame] | None = None
    if panel is not None and not panel.empty:
        panel_by_official = {
            int(oid): group for oid, group in panel.groupby("official_id", sort=False)
        }
    seminars = (
        seminars_bulk
        if seminars_bulk is not None
        else load_official_seminars_bulk(official_ids)
    )
    seminars_by_official: dict[int, pd.DataFrame] | None = None
    if seminars is not None and not seminars.empty:
        seminars_by_official = {
            int(oid): group for oid, group in seminars.groupby("official_id", sort=False)
        }
    appointment_contexts = _batch_appointment_contexts(official_ids)
    appointment_rows_by_official = load_official_international_appointment_rows(
        official_ids
    )
    isu_listing_keys = _batch_isu_listing_keys(official_ids)

    with Session(engine) as session:
        international_level_id = _international_level_id(session)
        isu_level_id = _isu_level_id(session)

    batch_kwargs = {
        "listing_season_code": listing_season_code,
        "rules_df": rules_df,
        "panel_bulk": panel,
        "panel_by_official": panel_by_official,
        "seminars_bulk": seminars,
        "seminars_by_official": seminars_by_official,
        "appointment_contexts": appointment_contexts,
        "appointment_rows_by_official": appointment_rows_by_official,
        "isu_listing_keys": isu_listing_keys,
        "international_level_id": international_level_id,
        "isu_level_id": isu_level_id,
    }

    maintain_notes: list[str] = []
    maintain_meets: list[str] = []
    promote_notes: list[str] = []
    promote_meets: list[str] = []

    for row in summary.itertuples(index=False):
        oid = int(row.official_id)
        atid = int(row.appointment_type_id)
        disc = _nullable_int_for_sql(row.discipline_id)
        appt_level = getattr(row, "appointment_level", "") or ""
        appt_level_id = _nullable_int_for_sql(getattr(row, "appointment_level_id", None))

        maintain_evals = evaluate_requirements_for_appointment(
            oid, atid, disc, "maintain", **batch_kwargs
        )
        listing_tier = directory_listing_tier_for_level(
            appt_level,
            level_id=appt_level_id,
            isu_level_id=isu_level_id,
            international_level_id=international_level_id,
        )
        tier_rules = [e for e in maintain_evals if e.listing_tier == listing_tier]
        tier_applicable = [e for e in tier_rules if not e.not_applicable]

        if tier_applicable:
            best = tier_applicable[0]
            maintain_meets.append("Yes" if best.meets else "No")
            maintain_notes.append(best.summary_note)
        elif tier_rules:
            best = tier_rules[0]
            maintain_meets.append("N/A")
            maintain_notes.append(best.not_applicable_reason or best.summary_note)
        else:
            maintain_meets.append("N/A")
            maintain_notes.append("")

        if not should_evaluate_promote_requirements(
            oid,
            atid,
            disc,
            appt_level,
            appt_level_id,
            isu_level_id=isu_level_id,
            isu_listing_keys=isu_listing_keys,
        ):
            promote_meets.append("")
            promote_notes.append("")
        else:
            promote_evals = evaluate_requirements_for_appointment(
                oid, atid, disc, "promote", **batch_kwargs
            )
            promote_primary = _primary_requirement_evaluation(promote_evals)
            if promote_primary is not None and not promote_primary.not_applicable:
                promote_meets.append("Yes" if promote_primary.meets else "No")
                promote_notes.append(promote_primary.summary_note)
            else:
                promote_meets.append("N/A")
                promote_notes.append(
                    promote_primary.not_applicable_reason or promote_primary.summary_note
                    if promote_primary is not None
                    else ""
                )

    out = summary.copy()
    out["maintain"] = maintain_meets
    out["maintain_note"] = maintain_notes
    out["promote"] = promote_meets
    out["promote_note"] = promote_notes
    return out


def load_requirement_rule_sets_admin(*, include_inactive: bool = True) -> pd.DataFrame:
    """All rule sets for admin editing."""
    inactive_clause = "" if include_inactive else " WHERE rs.active"
    stmt = text(
        f"""
        SELECT
            rs.id,
            rs.isu_rule_ref,
            rs.purpose,
            rs.label,
            rs.appointment_type_id,
            rs.directory_level_id,
            rs.discipline_id,
            rs.listing_tier,
            rs.season_window,
            rs.sport,
            rs.sort_order,
            rs.active
        FROM officials_analysis.international_requirement_rule_set rs
        {inactive_clause}
        ORDER BY rs.sort_order, rs.id
        """
    )
    try:
        with Session(engine) as session:
            rows = session.execute(stmt).mappings().all()
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_requirement_rules_admin(rule_set_id: int | None = None) -> pd.DataFrame:
    """Rules for one rule set (or all when ``rule_set_id`` is None)."""
    where = " WHERE r.rule_set_id = :rsid" if rule_set_id is not None else ""
    params: dict[str, Any] = {}
    if rule_set_id is not None:
        params["rsid"] = int(rule_set_id)

    stmt = text(
        f"""
        SELECT
            r.id,
            r.rule_set_id,
            r.metric,
            r.min_value,
            r.role_appointment_type_ids,
            r.competition_type_ids,
            r.segment_levels,
            r.require_championship_or_olympic,
            r.include_qualifying_national,
            r.metric_config,
            r.display_label,
            r.sort_order
        FROM officials_analysis.international_requirement_rule r
        {where}
        ORDER BY r.rule_set_id, r.sort_order, r.id
        """
    )
    try:
        with Session(engine) as session:
            rows = session.execute(stmt, params).mappings().all()
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _parse_int_list(value: Any) -> list[int] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (list, tuple)):
        return [int(x) for x in value if x is not None and str(x).strip()]
    text_val = str(value).strip()
    if not text_val:
        return None
    return [int(x.strip()) for x in text_val.split(",") if x.strip()]


def _parse_str_list(value: Any) -> list[str] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (list, tuple)):
        return [str(x).strip() for x in value if str(x).strip()]
    text_val = str(value).strip()
    if not text_val:
        return None
    return [x.strip() for x in text_val.split(",") if x.strip()]


def update_requirement_rule_set(
    rule_set_id: int,
    *,
    isu_rule_ref: str | None = None,
    label: str | None = None,
    season_window: int | None = None,
    sort_order: int | None = None,
    active: bool | None = None,
) -> None:
    fields: dict[str, Any] = {}
    if isu_rule_ref is not None:
        fields["isu_rule_ref"] = isu_rule_ref
    if label is not None:
        fields["label"] = label
    if season_window is not None:
        fields["season_window"] = int(season_window)
    if sort_order is not None:
        fields["sort_order"] = int(sort_order)
    if active is not None:
        fields["active"] = bool(active)
    if not fields:
        return

    set_clause = ", ".join(f"{k} = :{k}" for k in fields)
    params = {**fields, "id": int(rule_set_id)}
    with Session(engine) as session:
        session.execute(
            text(
                f"""
                UPDATE officials_analysis.international_requirement_rule_set
                SET {set_clause}
                WHERE id = :id
                """
            ),
            params,
        )
        session.commit()


def update_requirement_rule(
    rule_id: int,
    *,
    min_value: int | None = None,
    display_label: str | None = None,
    sort_order: int | None = None,
    role_appointment_type_ids: Any = None,
    competition_type_ids: Any = None,
    segment_levels: Any = None,
    require_championship_or_olympic: bool | None = None,
    include_qualifying_national: bool | None = None,
) -> None:
    fields: dict[str, Any] = {}
    if min_value is not None:
        fields["min_value"] = int(min_value)
    if display_label is not None:
        fields["display_label"] = display_label
    if sort_order is not None:
        fields["sort_order"] = int(sort_order)
    if role_appointment_type_ids is not None:
        fields["role_appointment_type_ids"] = _parse_int_list(role_appointment_type_ids)
    if competition_type_ids is not None:
        fields["competition_type_ids"] = _parse_int_list(competition_type_ids)
    if segment_levels is not None:
        fields["segment_levels"] = _parse_str_list(segment_levels)
    if require_championship_or_olympic is not None:
        fields["require_championship_or_olympic"] = bool(require_championship_or_olympic)
    if include_qualifying_national is not None:
        fields["include_qualifying_national"] = bool(include_qualifying_national)
    if not fields:
        return

    set_clause = ", ".join(f"{k} = :{k}" for k in fields)
    params = {**fields, "id": int(rule_id)}
    with Session(engine) as session:
        session.execute(
            text(
                f"""
                UPDATE officials_analysis.international_requirement_rule
                SET {set_clause}
                WHERE id = :id
                """
            ),
            params,
        )
        session.commit()
