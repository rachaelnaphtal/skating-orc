"""
ISU Rule 411(b) segment eligibility for international service requirements.

International Competition segments (competition types 15–17) count toward maintain /
promote requirements only when the segment meets the ISU service definitions:

- Single Skating: at least six entries, two or more ISU Members
- Pair Skating and Ice Dance: at least four entries, two or more ISU Members
- Synchronized Skating: two or more ISU Members (no entry minimum)

NOC codes are parsed from the last token of ``public.skater.name`` when it is three
uppercase letters (common on ISU detailed results, e.g. ``Boglárka ZHANG HUN`` or
``Haydenettes USA``). When nations cannot be verified from names, entry counts still
apply; member participation is marked unverified rather than failing the segment.

Segments with **no** ``skater_segment`` rows are treated as hand-entered for
international service tracking (officials/panel only, no scraped results) and pass
Rule 411.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

try:
    from activityAnalysis.load_activity_data import (
        NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,
        NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,
        NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES,
        activity_database_is_postgresql,
        engine,
    )
except ModuleNotFoundError:
    from load_activity_data import (
        NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE,
        NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,
        NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES,
        activity_database_is_postgresql,
        engine,
    )

from officials_competition_types import OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL

DisciplineCategory = Literal["singles", "pairs", "dance", "synchronized", "other"]

RULE411_MIN_ENTRIES: dict[DisciplineCategory, int | None] = {
    "singles": 6,
    "pairs": 4,
    "dance": 4,
    "synchronized": None,
    "other": None,
}
RULE411_MIN_ISU_MEMBERS = 2

_NOC_TOKEN_RE = re.compile(r"^[A-Z]{3}$")
# False positives such as "THE" are rare on ISU results; keep a small denylist.
_NOC_DENYLIST = frozenset({"THE", "AND", "FOR"})


@dataclass(frozen=True)
class SegmentRule411Stats:
    segment_id: int
    entry_count: int
    distinct_noc_count: int
    nocs_parsed_from_entries: int
    discipline_category: DisciplineCategory
    min_entries_required: int | None
    meets_entry_minimum: bool
    meets_member_minimum: bool | None
    eligible: bool
    status_label: str
    detail: str


def extract_noc_from_skater_name(name: str) -> str | None:
    """Return a three-letter NOC suffix from an ISU-style skater name, if present."""
    text = (name or "").strip()
    if not text:
        return None
    token = text.split()[-1].upper()
    if _NOC_TOKEN_RE.fullmatch(token) and token not in _NOC_DENYLIST:
        return token
    return None


def discipline_category_from_segment(
    *,
    segment_discipline: Any = None,
    segment_discipline_type_id: Any = None,
) -> DisciplineCategory:
    try:
        dt_id = int(segment_discipline_type_id)
    except (TypeError, ValueError):
        dt_id = None

    if dt_id == NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES:
        return "singles"
    if dt_id == NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS:
        return "pairs"
    if dt_id == NQS_SEGMENT_DISCIPLINE_TYPE_ICE_DANCE:
        return "dance"
    if dt_id == 5:
        return "synchronized"

    label = str(segment_discipline or "").strip().lower()
    if "single" in label:
        return "singles"
    if "pair" in label:
        return "pairs"
    if "dance" in label:
        return "dance"
    if "synch" in label:
        return "synchronized"
    return "other"


def min_entries_for_discipline(category: DisciplineCategory) -> int | None:
    return RULE411_MIN_ENTRIES.get(category)


_HAND_ENTERED_SEGMENT_DETAIL = (
    "Hand-entered segment (no skater results loaded); counts for international service"
)


def _hand_entered_segment_rule411_stats(
    discipline_category: DisciplineCategory,
    *,
    segment_id: int = -1,
) -> SegmentRule411Stats:
    """Segments with zero entries are manually tracked; pass Rule 411."""
    return SegmentRule411Stats(
        segment_id=segment_id,
        entry_count=0,
        distinct_noc_count=0,
        nocs_parsed_from_entries=0,
        discipline_category=discipline_category,
        min_entries_required=min_entries_for_discipline(discipline_category),
        meets_entry_minimum=True,
        meets_member_minimum=None,
        eligible=True,
        status_label="Yes",
        detail=_HAND_ENTERED_SEGMENT_DETAIL,
    )


def _evaluate_synchronized_rule411(
    *,
    entry_count: int,
    distinct_noc_count: int,
    nocs_parsed_from_entries: int,
) -> SegmentRule411Stats:
    """Synchronized: two ISU Member countries only (no team-count minimum)."""
    if entry_count == 0:
        return _hand_entered_segment_rule411_stats("synchronized")
    if nocs_parsed_from_entries == 0:
        return SegmentRule411Stats(
            segment_id=-1,
            entry_count=entry_count,
            distinct_noc_count=distinct_noc_count,
            nocs_parsed_from_entries=nocs_parsed_from_entries,
            discipline_category="synchronized",
            min_entries_required=None,
            meets_entry_minimum=True,
            meets_member_minimum=None,
            eligible=True,
            status_label="Unverified nations",
            detail=(
                f"{entry_count} teams; need {RULE411_MIN_ISU_MEMBERS} ISU Members — "
                "member count not verified from team names"
            ),
        )
    meets_members = distinct_noc_count >= RULE411_MIN_ISU_MEMBERS
    if meets_members:
        return SegmentRule411Stats(
            segment_id=-1,
            entry_count=entry_count,
            distinct_noc_count=distinct_noc_count,
            nocs_parsed_from_entries=nocs_parsed_from_entries,
            discipline_category="synchronized",
            min_entries_required=None,
            meets_entry_minimum=True,
            meets_member_minimum=True,
            eligible=True,
            status_label="Yes",
            detail=(
                f"{entry_count} teams, {distinct_noc_count} ISU Members "
                f"(need {RULE411_MIN_ISU_MEMBERS} Members)"
            ),
        )
    return SegmentRule411Stats(
        segment_id=-1,
        entry_count=entry_count,
        distinct_noc_count=distinct_noc_count,
        nocs_parsed_from_entries=nocs_parsed_from_entries,
        discipline_category="synchronized",
        min_entries_required=None,
        meets_entry_minimum=True,
        meets_member_minimum=False,
        eligible=False,
        status_label="No",
        detail=(
            f"{distinct_noc_count}/{RULE411_MIN_ISU_MEMBERS} ISU Members "
            f"({entry_count} teams)"
        ),
    )


def evaluate_segment_rule411(
    *,
    entry_count: int,
    distinct_noc_count: int,
    nocs_parsed_from_entries: int,
    discipline_category: DisciplineCategory,
) -> SegmentRule411Stats:
    if entry_count == 0:
        return _hand_entered_segment_rule411_stats(discipline_category)
    if discipline_category == "synchronized":
        return _evaluate_synchronized_rule411(
            entry_count=entry_count,
            distinct_noc_count=distinct_noc_count,
            nocs_parsed_from_entries=nocs_parsed_from_entries,
        )

    min_entries = min_entries_for_discipline(discipline_category)
    meets_entries = min_entries is None or entry_count >= min_entries

    meets_members: bool | None
    if min_entries is None:
        meets_members = None
    elif nocs_parsed_from_entries == 0:
        meets_members = None
    else:
        meets_members = distinct_noc_count >= RULE411_MIN_ISU_MEMBERS

    if min_entries is None:
        eligible = True
        status = "N/A (discipline)"
        detail = "Rule 411 entry minimums apply to Singles, Pairs, and Ice Dance."
    elif not meets_entries:
        eligible = False
        status = "No"
        detail = (
            f"{entry_count}/{min_entries} entries "
            f"({discipline_category.replace('_', ' ')})"
        )
    elif meets_members is False:
        eligible = False
        status = "No"
        detail = (
            f"{distinct_noc_count}/{RULE411_MIN_ISU_MEMBERS} ISU Members "
            f"({entry_count} entries)"
        )
    elif meets_members is None:
        eligible = True
        status = "Unverified nations"
        detail = (
            f"{entry_count} entries (need {min_entries}); "
            "ISU Member count not verified from skater names"
        )
    else:
        eligible = True
        status = "Yes"
        detail = (
            f"{entry_count} entries, {distinct_noc_count} ISU Members "
            f"(need {min_entries} entries, {RULE411_MIN_ISU_MEMBERS} Members)"
        )

    return SegmentRule411Stats(
        segment_id=-1,
        entry_count=entry_count,
        distinct_noc_count=distinct_noc_count,
        nocs_parsed_from_entries=nocs_parsed_from_entries,
        discipline_category=discipline_category,
        min_entries_required=min_entries,
        meets_entry_minimum=meets_entries,
        meets_member_minimum=meets_members,
        eligible=eligible,
        status_label=status,
        detail=detail,
    )


def load_segment_rule411_stats(segment_ids: list[int]) -> pd.DataFrame:
    """Bulk load entry / NOC stats for ``segment.id`` values."""
    cols = [
        "segment_id",
        "rule411_entry_count",
        "rule411_distinct_noc_count",
        "rule411_nocs_parsed",
        "rule411_discipline_category",
        "rule411_min_entries",
        "rule411_meets_entry_minimum",
        "rule411_meets_member_minimum",
        "rule411_eligible",
        "rule411_status",
        "rule411_detail",
    ]
    ids = sorted({int(x) for x in segment_ids if x is not None and pd.notna(x)})
    if not ids or not activity_database_is_postgresql():
        return pd.DataFrame(columns=cols)

    stmt = text(
        """
        SELECT ss.segment_id, sk.name AS skater_name
        FROM public.skater_segment ss
        INNER JOIN public.skater sk ON sk.id = ss.skater_id
        WHERE ss.segment_id IN :segment_ids
        """
    ).bindparams(bindparam("segment_ids", expanding=True))

    try:
        with Session(engine) as session:
            rows = session.execute(stmt, {"segment_ids": ids}).mappings().all()
    except Exception:
        return pd.DataFrame(columns=cols)

    if not rows:
        empty_stats = pd.DataFrame({"segment_id": ids})
        for col in cols[1:]:
            empty_stats[col] = pd.NA
        empty_stats["rule411_entry_count"] = 0
        empty_stats["rule411_distinct_noc_count"] = 0
        empty_stats["rule411_nocs_parsed"] = 0
        empty_stats["rule411_discipline_category"] = "other"
        empty_stats["rule411_meets_entry_minimum"] = True
        empty_stats["rule411_meets_member_minimum"] = pd.NA
        empty_stats["rule411_eligible"] = True
        empty_stats["rule411_status"] = "Yes"
        empty_stats["rule411_detail"] = _HAND_ENTERED_SEGMENT_DETAIL
        return empty_stats[cols]

    names_by_segment: dict[int, list[str]] = {}
    for row in rows:
        sid = int(row["segment_id"])
        names_by_segment.setdefault(sid, []).append(str(row["skater_name"] or ""))

    records: list[dict[str, Any]] = []
    for sid in ids:
        names = names_by_segment.get(sid, [])
        nocs = [n for n in (extract_noc_from_skater_name(nm) for nm in names) if n]
        stats = evaluate_segment_rule411(
            entry_count=len(names),
            distinct_noc_count=len(set(nocs)),
            nocs_parsed_from_entries=len(nocs),
            discipline_category="other",
        )
        records.append(
            {
                "segment_id": sid,
                "rule411_entry_count": stats.entry_count,
                "rule411_distinct_noc_count": stats.distinct_noc_count,
                "rule411_nocs_parsed": stats.nocs_parsed_from_entries,
                "rule411_discipline_category": stats.discipline_category,
                "rule411_min_entries": stats.min_entries_required,
                "rule411_meets_entry_minimum": stats.meets_entry_minimum,
                "rule411_meets_member_minimum": stats.meets_member_minimum,
                "rule411_eligible": stats.eligible,
                "rule411_status": stats.status_label,
                "rule411_detail": stats.detail,
            }
        )

    return pd.DataFrame(records)


def enrich_panel_with_rule411_eligibility(panel: pd.DataFrame) -> pd.DataFrame:
    """Attach Rule 411 columns; re-evaluate using segment discipline metadata."""
    if panel.empty:
        return panel

    out = panel.copy()
    if "rule411_eligible" in out.columns:
        return out

    out["rule411_eligible"] = True
    out["rule411_entry_count"] = pd.NA
    out["rule411_distinct_noc_count"] = pd.NA
    out["rule411_status"] = "N/A"
    out["rule411_detail"] = pd.NA

    if "segment_id" not in out.columns:
        return out

    intl_mask = out["competition_type_id"].apply(
        lambda x: int(x) in OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL
        if pd.notna(x)
        else False
    )
    if not intl_mask.any():
        return out

    segment_ids = (
        pd.to_numeric(out.loc[intl_mask, "segment_id"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    stats = load_segment_rule411_stats(segment_ids)
    if stats.empty:
        return out

    stats_by_segment = stats.set_index("segment_id", drop=False)
    for idx, row in out.loc[intl_mask].iterrows():
        try:
            sid = int(row["segment_id"])
        except (TypeError, ValueError):
            out.at[idx, "rule411_eligible"] = False
            out.at[idx, "rule411_status"] = "Unknown"
            out.at[idx, "rule411_detail"] = "Missing segment id"
            continue
        if sid not in stats_by_segment.index:
            category = discipline_category_from_segment(
                segment_discipline=row.get("segment_discipline"),
                segment_discipline_type_id=row.get("segment_discipline_type_id"),
            )
            manual = _hand_entered_segment_rule411_stats(category, segment_id=sid)
            out.at[idx, "rule411_entry_count"] = manual.entry_count
            out.at[idx, "rule411_distinct_noc_count"] = manual.distinct_noc_count
            out.at[idx, "rule411_status"] = manual.status_label
            out.at[idx, "rule411_detail"] = manual.detail
            out.at[idx, "rule411_eligible"] = manual.eligible
            continue
        base = stats_by_segment.loc[sid]
        category = discipline_category_from_segment(
            segment_discipline=row.get("segment_discipline"),
            segment_discipline_type_id=row.get("segment_discipline_type_id"),
        )
        evaluated = evaluate_segment_rule411(
            entry_count=int(base["rule411_entry_count"]),
            distinct_noc_count=int(base["rule411_distinct_noc_count"]),
            nocs_parsed_from_entries=int(base["rule411_nocs_parsed"]),
            discipline_category=category,
        )
        out.at[idx, "rule411_entry_count"] = evaluated.entry_count
        out.at[idx, "rule411_distinct_noc_count"] = evaluated.distinct_noc_count
        out.at[idx, "rule411_status"] = evaluated.status_label
        out.at[idx, "rule411_detail"] = evaluated.detail
        out.at[idx, "rule411_eligible"] = evaluated.eligible

    return out


def filter_panel_to_rule411_eligible(panel: pd.DataFrame) -> pd.DataFrame:
    """Drop international segments (types 15–17) that fail Rule 411 entry/member checks."""
    if panel.empty:
        return panel
    enriched = enrich_panel_with_rule411_eligibility(panel)
    intl_mask = enriched["competition_type_id"].apply(
        lambda x: int(x) in OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL
        if pd.notna(x)
        else False
    )
    keep = (~intl_mask) | enriched["rule411_eligible"].fillna(False)
    return enriched.loc[keep].reset_index(drop=True)
