"""
USFS ``officials_analysis.competition_type`` ids used when linking ``public.competition``.

Qualifying analytics scope: every linked type **except** ``NON_QUALIFYING`` (11);
that includes Adult / Collegiate types **12–14**. They are **not** part of
**Sectionals & championships** or **Championships only** scope lists (types **1–9** / **4, 8** only).
NQS scope: type id **10** (National Qualifying Series).
"""

from __future__ import annotations

from typing import Optional

# Officials-analysis competition_type.id values (US Figure Skating taxonomy)
OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING = 11
OFFICIALS_COMPETITION_TYPE_ID_NQS = 10
OFFICIALS_COMPETITION_TYPE_ID_ADULT_CHAMPIONSHIPS = 12
OFFICIALS_COMPETITION_TYPE_ID_ADULT_SECTIONAL = 13
OFFICIALS_COMPETITION_TYPE_ID_COLLEGIATE_CHAMPIONSHIPS = 14

# Analytics competition-scope keys (single select → SQL filter on public.competition link)
COMPETITION_SCOPE_ALL = "all"
COMPETITION_SCOPE_QUALIFYING = "qualifying"
COMPETITION_SCOPE_NQS = "nqs"
COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS = "sectionals_and_championships"
COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY = "championships_only"

# Every analytics scope key (matches Streamlit competition-scope filters).
ALL_COMPETITION_SCOPES: tuple[str, ...] = (
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_QUALIFYING,
    COMPETITION_SCOPE_NQS,
    COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
)

# Types 1–3 SPD sectionals, 4 US Champs, 5–7,9 SYS sectionals, 8 US Synchro Champs.
# Adult / Collegiate (12–14) are **not** in this bucket — use "Qualifying only" to include them.
# Excludes 10 NQS, 11 nonqualifying.
OFFICIALS_COMPETITION_TYPE_IDS_SECTIONALS_AND_CHAMPIONSHIPS = frozenset(
    {1, 2, 3, 4, 5, 6, 7, 8, 9}
)
OFFICIALS_COMPETITION_TYPE_IDS_CHAMPIONSHIPS_ONLY = frozenset({4, 8})

SPD_SECTIONAL_TYPE_IDS = frozenset({1, 2, 3})
SYS_SECTIONAL_TYPE_IDS = frozenset({5, 6, 7, 9})
ADULT_AND_COLLEGIATE_TYPE_IDS = frozenset(
    {
        OFFICIALS_COMPETITION_TYPE_ID_ADULT_CHAMPIONSHIPS,
        OFFICIALS_COMPETITION_TYPE_ID_ADULT_SECTIONAL,
        OFFICIALS_COMPETITION_TYPE_ID_COLLEGIATE_CHAMPIONSHIPS,
    }
)

OFFICIALS_COMPETITION_TYPE_DISPLAY_NAMES: dict[int, str] = {
    14: "Collegiate Championships",
    13: "Adult Sectional",
    12: "Adult Championships",
    11: "Nonqualifying Competition",
    10: "National Qualifying Series",
    9: "Midwestern/Pacific Synchro Sectional",
    8: "US Synchronized Skating Championships",
    7: "Pacific Coast Synchro Sectional",
    6: "Midwestern Synchro Sectional",
    5: "Eastern Synchro Sectional",
    4: "US Championships",
    3: "Pacific Coast Sectional",
    2: "Midwestern Sectional",
    1: "Eastern Sectional",
}


def base_officials_competition_type_name(type_id: int, name_from_db: Optional[str] = None) -> str:
    raw = (name_from_db or "").strip()
    if raw:
        return raw
    return OFFICIALS_COMPETITION_TYPE_DISPLAY_NAMES.get(type_id, f"Type {type_id}")


def format_officials_competition_type_select_label(
    type_id: int, name_from_db: Optional[str] = None
) -> str:
    """Grouped label for selects (SPD / SYS sectionals + canonical names)."""
    base = base_officials_competition_type_name(type_id, name_from_db)
    if type_id in ADULT_AND_COLLEGIATE_TYPE_IDS:
        return f"Adult & Collegiate — {base}"
    if type_id in SPD_SECTIONAL_TYPE_IDS:
        return f"SPD Sectional — {base}"
    if type_id in SYS_SECTIONAL_TYPE_IDS:
        return f"SYS Sectional — {base}"
    return base


def competition_load_flags_from_officials_type_id(type_id: int) -> tuple[bool, bool]:
    """
    (qualifying_column, nqs_column) for ``public.competition`` when loading results.

    qualifying=True for all types except nonqualifying (11). NQS=True only for type 10.
    Adult / Collegiate types (12–14) are qualifying and not NQS, same as other non-11 types.
    """
    nqs = type_id == OFFICIALS_COMPETITION_TYPE_ID_NQS
    qualifying = type_id != OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING
    return qualifying, nqs
