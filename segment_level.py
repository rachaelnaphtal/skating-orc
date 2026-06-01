"""
Classify ``public.segment.level`` from segment and competition names.

International (ISU) rules follow explicit tokens in ``segment.name`` first, then
competition-name fallbacks. Domestic (USFS) rules recognize common qualifying levels
and treat ``Excel Juvenile`` separately from ``Juvenile``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
# Standard labels stored on ``public.segment.level``.
LEVEL_SENIOR = "Senior"
LEVEL_JUNIOR = "Junior"
LEVEL_ADVANCED_NOVICE = "Advanced Novice"
LEVEL_INTERMEDIATE_NOVICE = "Intermediate Novice"
LEVEL_BASIC_NOVICE = "Basic Novice"
LEVEL_JUVENILE = "Juvenile"
LEVEL_EXCEL_PRELIMINARY_PLUS = "Excel Preliminary Plus"
LEVEL_EXCEL_PRE_JUVENILE_PLUS = "Excel Pre-Juvenile Plus"
LEVEL_EXCEL_PRE_JUVENILE = "Excel Pre-Juvenile"
LEVEL_EXCEL_HIGH_BEGINNER = "Excel High Beginner"
LEVEL_EXCEL_BEGINNER = "Excel Beginner"
LEVEL_EXCEL_PRELIMINARY = "Excel Preliminary"
LEVEL_EXCEL_INTERMEDIATE = "Excel Intermediate"
LEVEL_EXCEL_NOVICE = "Excel Novice"
LEVEL_EXCEL_JUNIOR = "Excel Junior"
LEVEL_EXCEL_SENIOR = "Excel Senior"
LEVEL_EXCEL_JUVENILE = "Excel Juvenile"
LEVEL_OPEN_COLLEGIATE = "Open Collegiate"
LEVEL_OPEN_ADULT = "Open Adult"
LEVEL_OPEN_MASTERS = "Open Masters"
LEVEL_OPEN_JUVENILE = "Open Juvenile"
LEVEL_ASPIRE = "Aspire"
LEVEL_UNIFIED = "Unified"
LEVEL_LTS = "LTS"
LEVEL_INTERNATIONAL = "International"
LEVEL_OPEN_LEVEL = "Open Level"
LEVEL_PRE_BRONZE = "Pre Bronze"
LEVEL_BRONZE = "Bronze"
LEVEL_PRE_SILVER = "Pre Silver"
LEVEL_SILVER = "Silver"
LEVEL_PRE_GOLD = "Pre Gold"
LEVEL_GOLD = "Gold"
LEVEL_NO_TEST = "No Test"
LEVEL_COLLEGIATE = "Collegiate"
LEVEL_MASTERS = "Masters"
LEVEL_ADULT = "Adult"
LEVEL_PRE_PRELIMINARY = "Pre-Preliminary"
LEVEL_PRE_JUVENILE = "Pre-Juvenile"
LEVEL_PRELIMINARY = "Preliminary"
LEVEL_INTERMEDIATE = "Intermediate"
LEVEL_NOVICE = "Novice"
LEVEL_MIXED_AGE = "Mixed Age"
LEVEL_UNSPECIFIED = "Unspecified"

SOURCE_SEGMENT_TOKEN = "segment_token"
SOURCE_COMPETITION_NAME = "competition_name"
SOURCE_DEFAULT_INTERNATIONAL = "default_international"
SOURCE_UNSPECIFIED = "unspecified"

# ``public.discipline_type.id`` (Ice Dance = 3, Solo Dance = 4 in this project).
DISCIPLINE_TYPE_ID_ICE_DANCE = 3
DISCIPLINE_TYPE_ID_SOLO_DANCE = 4
DANCE_DISCIPLINE_TYPE_IDS = frozenset(
    {DISCIPLINE_TYPE_ID_ICE_DANCE, DISCIPLINE_TYPE_ID_SOLO_DANCE}
)


@dataclass(frozen=True)
class SegmentLevelResult:
    level: str
    source: str


def normalize_segment_name(s: str | None) -> str:
    """Uppercase; punctuation/whitespace → single underscores."""
    if not s:
        return ""
    t = str(s).upper()
    t = re.sub(r"[^\w]+", "_", t)
    t = re.sub(r"_+", "_", t)
    return t.strip("_")


def _segment_level_token_pattern(token: str) -> re.Pattern[str]:
    """
    Match a level token in a normalized segment name (underscore-delimited).

    ``\\b`` is unreliable here because ``\\w`` includes ``_`` in Python.
    """
    body = re.escape(token.upper().replace(" ", "_"))
    return re.compile(rf"(?:^|_){body}(?:_|$)")


def _segment_level_embedded_pattern(token: str) -> re.Pattern[str]:
    """Match level tokens glued after event numbers (e.g. ``151BRONZE_SOLO_…``)."""
    body = re.escape(token.upper().replace(" ", "_"))
    return re.compile(rf"(?:^|_|\d){body}(?:_|$)")


def _segment_has_level_token(segment_norm: str, token: str) -> bool:
    return bool(
        _segment_level_token_pattern(token).search(segment_norm)
        or _segment_level_embedded_pattern(token).search(segment_norm)
    )


_OPEN_COMPOUND = re.compile(
    r"OPEN_(?:COLLEGIATE|ADULT|MASTERS|JUVENILE|LEVEL)"
)


def _segment_token_rules() -> list[tuple[re.Pattern[str], str]]:
    """Ordered longest-/most-specific-first."""
    return [
        (_segment_level_token_pattern("INTERMEDIATE NOVICE"), LEVEL_INTERMEDIATE_NOVICE),
        (_segment_level_token_pattern("ADVANCED NOVICE"), LEVEL_ADVANCED_NOVICE),
        (_segment_level_token_pattern("BASIC NOVICE"), LEVEL_BASIC_NOVICE),
        (_segment_level_token_pattern("EXCEL PRELIMINARY PLUS"), LEVEL_EXCEL_PRELIMINARY_PLUS),
        (_segment_level_token_pattern("EXCEL PRE JUVENILE PLUS"), LEVEL_EXCEL_PRE_JUVENILE_PLUS),
        (_segment_level_token_pattern("EXCEL PRELIMINARY"), LEVEL_EXCEL_PRELIMINARY),
        (_segment_level_token_pattern("EXCEL INTERMEDIATE"), LEVEL_EXCEL_INTERMEDIATE),
        (_segment_level_token_pattern("EXCEL NOVICE"), LEVEL_EXCEL_NOVICE),
        (_segment_level_token_pattern("EXCEL JUVENILE"), LEVEL_EXCEL_JUVENILE),
        (_segment_level_token_pattern("EXCEL JUNIOR"), LEVEL_EXCEL_JUNIOR),
        (_segment_level_token_pattern("EXCEL SENIOR"), LEVEL_EXCEL_SENIOR),
        (_segment_level_token_pattern("OPEN COLLEGIATE"), LEVEL_OPEN_COLLEGIATE),
        (_segment_level_token_pattern("OPEN ADULT"), LEVEL_OPEN_ADULT),
        (_segment_level_token_pattern("OPEN MASTERS"), LEVEL_OPEN_MASTERS),
        (_segment_level_token_pattern("OPEN JUVENILE"), LEVEL_OPEN_JUVENILE),
        (_segment_level_token_pattern("MIXED AGE"), LEVEL_MIXED_AGE),
        (_segment_level_token_pattern("NO TEST"), LEVEL_NO_TEST),
        (_segment_level_token_pattern("COLLEGIATE"), LEVEL_COLLEGIATE),
        (_segment_level_token_pattern("PRE PRELIMINARY"), LEVEL_PRE_PRELIMINARY),
        (_segment_level_token_pattern("PRELIMINARY"), LEVEL_PRELIMINARY),
        (_segment_level_token_pattern("PRE JUVENILE"), LEVEL_PRE_JUVENILE),
        (_segment_level_token_pattern("INTERMEDIATE"), LEVEL_INTERMEDIATE),
        (_segment_level_token_pattern("MASTERS"), LEVEL_MASTERS),
        (_segment_level_token_pattern("ADULT"), LEVEL_ADULT),
        (_segment_level_token_pattern("SENIOR"), LEVEL_SENIOR),
        (_segment_level_token_pattern("JUNIOR"), LEVEL_JUNIOR),
        (_segment_level_token_pattern("JUVENILE"), LEVEL_JUVENILE),
        (_segment_level_token_pattern("NOVICE"), LEVEL_NOVICE),
    ]


# Abbreviations / typos / multi-token Excel levels (checked before catch-all).
_EXCEL_SPECIAL_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"EXCEL_PRELIMNARY_PLUS"), LEVEL_EXCEL_PRELIMINARY_PLUS),
    (re.compile(r"EXCEL_PRE_JUVENILE_PLUS"), LEVEL_EXCEL_PRE_JUVENILE_PLUS),
    (re.compile(r"EXCEL_PRE_JUV_PLUS"), LEVEL_EXCEL_PRE_JUVENILE),
    (re.compile(r"EXCEL_PRE_JUVENILE(?:_|$)"), LEVEL_EXCEL_PRE_JUVENILE),
    (re.compile(r"EXCEL_PRE_JUV(?:_|$)"), LEVEL_EXCEL_PRE_JUVENILE),
    (re.compile(r"EXCEL_HIGH_BEGINNER"), LEVEL_EXCEL_HIGH_BEGINNER),
    (
        re.compile(r"EXCEL(?:_[A-Z0-9]+)*_BEGINNER(?:_|$)"),
        LEVEL_EXCEL_BEGINNER,
    ),
]

_EXCEL_CATCHALL = re.compile(r"(?:^|_)\d*_?EXCEL_([A-Z][A-Z0-9_]*)(?:_|$)")


def _excel_special_level(segment_norm: str) -> str | None:
    if "EXCEL" not in segment_norm:
        return None
    for pattern, label in _EXCEL_SPECIAL_RULES:
        if pattern.search(segment_norm):
            return label
    return None


def _excel_catchall_level(segment_norm: str) -> str | None:
    """
    Other ``Excel_*`` segment names (e.g. ``Excel_Intermediate_…``) → ``Excel <rest>``.
    """
    if "_EXCEL_" not in segment_norm:
        return None
    m = _EXCEL_CATCHALL.search(segment_norm)
    if not m:
        return None
    rest = m.group(1).strip("_")
    if not rest:
        return "Excel"
    words = [w.capitalize() for w in rest.split("_") if w]
    return "Excel " + " ".join(words) if words else "Excel"


_COMPETITION_JUNIOR = re.compile(
    r"\bJUNIOR\b|\bJGP\b|\bJUNIOR\s+GRAND\s+PRIX\b|\bJUNIOR\s+WORLD\b|\bWORLD\s+JUNIOR\b",
    re.I,
)
_COMPETITION_SENIOR = re.compile(
    r"\bGRAND\s+PRIX\b|\bGP\s+FINAL\b|\bWORLD\s+CHAMPIONSHIPS\b|\bWORLDS\b|\bEUROPEAN\s+CHAMPIONSHIPS\b|"
    r"\bFOUR\s+CONTINENTS\b|\bOLYMPIC\b|\bCHALLENGER\s+SERIES\b|\bWORLD\s+TEAM\s+TROPHY\b|"
    r"\bISU\s+GRAND\s+PRIX\b|\bINTERNATIONAL\s+CHALLENGE\s+CUP\b",
    re.I,
)
_TEAM_EVENT_SEGMENT = _segment_level_token_pattern("TEAM EVENT")

# US Championships (and similar) segment names: ``CHAMPIONSHIP_PAIRS_FREE_SKATING``, etc.
_US_CHAMPIONSHIP_SEGMENT = re.compile(
    r"(?:^|_)CHAMPIONSHIP_(?:MEN|WOMEN|PAIRS|ICE_DANCE)(?:_|$)"
)

# Synchro / domestic program labels (checked before generic tokens).
_ELITE_LEVEL_SEGMENT = re.compile(r"(?:^|_)ELITE_\d+(?:_|$)")
_UNIFIED_PROGRAM_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"SPECIAL_OLYMPICS"), LEVEL_UNIFIED),
    (re.compile(r"SKATE_UNITED"), LEVEL_UNIFIED),
    (re.compile(r"(?:^|_)UNIFIED(?:_|$)"), LEVEL_UNIFIED),
]

_LTS_BASIC = re.compile(r"(?:^|_)BASIC(?:_|$)")
_LTS_SNOWPLOW_SAM = re.compile(r"SNOWPLOW_SAM")
_LTS_BEGINNER = re.compile(r"BEGINNER")

_THEATRE_CONTEXT = re.compile(r"THEATRE|CHOREOGRAPHIC")
_DANCE_CONTEXT = re.compile(
    r"SHADOW_DANCE|SOLO_PATTERN_DANCE|SOLO_PATTERN(?:_|$)|SOLO_ICE_DANCE|"
    r"(?:^|_)SOLO_DANCE(?:_|$)|(?:^|_)SOLO(?:_|$)|"
    r"PARTNERED_PATTERN_DANCE|(?:^|_)PATTERN_DANCE(?:_|$)|(?:^|_)ICE_DANCE(?:_|$)"
)
_DANCE_AND_THEATRE_LEVEL_TOKENS: list[tuple[str, str]] = [
    ("INTERNATIONAL", LEVEL_INTERNATIONAL),
    ("OPEN", LEVEL_OPEN_LEVEL),
    ("PRE BRONZE", LEVEL_PRE_BRONZE),
    ("PRE SILVER", LEVEL_PRE_SILVER),
    ("PRE GOLD", LEVEL_PRE_GOLD),
    ("BRONZE", LEVEL_BRONZE),
    ("SILVER", LEVEL_SILVER),
    ("GOLD", LEVEL_GOLD),
]


def _context_level_from_tokens(
    segment_norm: str,
    tokens: list[tuple[str, str]],
    *,
    allow_open: bool = True,
) -> str | None:
    for token, label in tokens:
        if token == "OPEN":
            if not allow_open or _OPEN_COMPOUND.search(segment_norm):
                continue
        if _segment_has_level_token(segment_norm, token):
            return label
    return None

_COMPETITION_US_NATIONALS = re.compile(
    r"U\.?S\.?\s+(?:FIGURE\s+SKATING\s+)?CHAMPIONSHIPS|PREVAGEN\s+U\.?S\.?",
    re.I,
)
_COMPETITION_NOT_US_NATIONALS = re.compile(
    r"SECTIONAL|ADULT|SYNCHRONIZED|SYNCHRO|COLLEGIATE|NON-?QUAL|\bNQS\b|JUNIOR|NOVICE",
    re.I,
)


def _us_championship_segment_level(segment_norm: str) -> str | None:
    if _US_CHAMPIONSHIP_SEGMENT.search(segment_norm):
        return LEVEL_SENIOR
    return None


def _is_theatre_on_ice_context(segment_norm: str, competition_name: str) -> bool:
    if _THEATRE_CONTEXT.search(segment_norm):
        return True
    return bool(_THEATRE_CONTEXT.search((competition_name or "").upper()))


def _is_ice_or_solo_dance_context(
    segment_norm: str,
    competition_name: str,
    *,
    discipline_type_id: int | None = None,
) -> bool:
    if discipline_type_id is not None and int(discipline_type_id) in DANCE_DISCIPLINE_TYPE_IDS:
        return True
    if _DANCE_CONTEXT.search(segment_norm):
        return True
    comp = (competition_name or "").upper()
    return bool(
        re.search(r"SOLO\s+DANCE|SOLO\s+ICE\s+DANCE|SHADOW\s+DANCE", comp)
        or "SOLO_DANCE" in normalize_segment_name(competition_name)
    )


def _ice_or_solo_dance_segment_level(
    segment_norm: str,
    competition_name: str,
    *,
    discipline_type_id: int | None = None,
) -> str | None:
    """
    USFS ice dance / solo dance track levels (International, Open, metal tiers).

    Context: ``discipline_type_id`` 3 (ice dance) or 4 (solo dance), or segment/competition
    name cues (incl. shadow dance). Not inferred from competition names alone.
    """
    if not _is_ice_or_solo_dance_context(
        segment_norm, competition_name, discipline_type_id=discipline_type_id
    ):
        return None
    return _context_level_from_tokens(segment_norm, _DANCE_AND_THEATRE_LEVEL_TOKENS)


def _theatre_on_ice_segment_level(
    segment_norm: str, competition_name: str
) -> str | None:
    """
    Theatre on Ice levels (segment or competition must indicate theatre).

    ``Open Level`` and metal tiers apply only in this context—not on adult/singles events.
    """
    if not _is_theatre_on_ice_context(segment_norm, competition_name):
        return None
    return _context_level_from_tokens(
        segment_norm,
        [(t, l) for t, l in _DANCE_AND_THEATRE_LEVEL_TOKENS if t != "INTERNATIONAL"],
    )


def _lts_segment_level(segment_norm: str, competition_name: str = "") -> str | None:
    """
    Learn to Skate (LTS) when segment or competition mentions basic skills,
    Snowplow Sam, or beginner (not Excel beginner / ISU Basic Novice).
    """
    names = [segment_norm]
    comp_norm = normalize_segment_name(competition_name)
    if comp_norm:
        names.append(comp_norm)
    for text in names:
        if _LTS_SNOWPLOW_SAM.search(text):
            return LEVEL_LTS
        if _LTS_BASIC.search(text):
            return LEVEL_LTS
        if _LTS_BEGINNER.search(text) and "EXCEL" not in text:
            return LEVEL_LTS
    return None


def _domestic_program_segment_level(segment_norm: str) -> str | None:
    """Aspire, Unified (incl. Skate United / Special Olympics), Elite N → Senior."""
    if _ELITE_LEVEL_SEGMENT.search(segment_norm):
        return LEVEL_SENIOR
    if _segment_level_token_pattern("ASPIRE").search(segment_norm):
        return LEVEL_ASPIRE
    for pattern, label in _UNIFIED_PROGRAM_RULES:
        if pattern.search(segment_norm):
            return label
    return None


def _domestic_competition_fallback(competition_name: str) -> SegmentLevelResult | None:
    comp = competition_name or ""
    if not comp or _COMPETITION_NOT_US_NATIONALS.search(comp):
        return None
    if _COMPETITION_US_NATIONALS.search(comp):
        return SegmentLevelResult(LEVEL_SENIOR, SOURCE_COMPETITION_NAME)
    return None


def _level_from_segment_tokens(
    segment_norm: str,
    *,
    competition_name: str = "",
    discipline_type_id: int | None = None,
) -> str | None:
    us_champs = _us_championship_segment_level(segment_norm)
    if us_champs:
        return us_champs
    dance = _ice_or_solo_dance_segment_level(
        segment_norm,
        competition_name,
        discipline_type_id=discipline_type_id,
    )
    if dance:
        return dance
    theatre = _theatre_on_ice_segment_level(segment_norm, competition_name)
    if theatre:
        return theatre
    program = _domestic_program_segment_level(segment_norm)
    if program:
        return program
    for pattern, label in _segment_token_rules():
        if pattern.search(segment_norm):
            return label
    excel = _excel_special_level(segment_norm)
    if excel:
        return excel
    catchall = _excel_catchall_level(segment_norm)
    if catchall:
        return catchall
    return _lts_segment_level(segment_norm, competition_name)


def _international_competition_fallback(
    segment_norm: str, competition_name: str
) -> SegmentLevelResult | None:
    comp = competition_name or ""
    if _TEAM_EVENT_SEGMENT.search(segment_norm):
        if _COMPETITION_JUNIOR.search(comp):
            return SegmentLevelResult(LEVEL_JUNIOR, SOURCE_COMPETITION_NAME)
        if _COMPETITION_SENIOR.search(comp) or re.search(r"\bOWG\b|\bOLYMPIC\b", comp, re.I):
            return SegmentLevelResult(LEVEL_SENIOR, SOURCE_COMPETITION_NAME)
    if _COMPETITION_JUNIOR.search(comp):
        return SegmentLevelResult(LEVEL_JUNIOR, SOURCE_COMPETITION_NAME)
    if _COMPETITION_SENIOR.search(comp):
        return SegmentLevelResult(LEVEL_SENIOR, SOURCE_COMPETITION_NAME)
    return None


def classify_segment_level(
    segment_name: str,
    *,
    competition_name: str = "",
    international: bool = False,
    discipline_type_id: int | None = None,
) -> SegmentLevelResult:
    """
    Return a standardized level label and how it was inferred.

    Explicit tokens in ``segment_name`` always win over competition-name inference.
    """
    segment_norm = normalize_segment_name(segment_name)
    if not segment_norm:
        return SegmentLevelResult(LEVEL_UNSPECIFIED, SOURCE_UNSPECIFIED)

    token_level = _level_from_segment_tokens(
        segment_norm,
        competition_name=competition_name,
        discipline_type_id=discipline_type_id,
    )
    if token_level:
        return SegmentLevelResult(token_level, SOURCE_SEGMENT_TOKEN)

    if international:
        comp_result = _international_competition_fallback(segment_norm, competition_name)
        if comp_result:
            return comp_result
        return SegmentLevelResult(LEVEL_SENIOR, SOURCE_DEFAULT_INTERNATIONAL)

    domestic = _domestic_competition_fallback(competition_name)
    if domestic:
        return domestic

    return SegmentLevelResult(LEVEL_UNSPECIFIED, SOURCE_UNSPECIFIED)


def classify_segment_level_for_row(
    segment_name: str,
    competition_name: str,
    international: bool | None,
    *,
    discipline_type_id: int | None = None,
) -> SegmentLevelResult:
    """Convenience wrapper when ``international`` may be unknown (treated as false)."""
    return classify_segment_level(
        segment_name,
        competition_name=competition_name or "",
        international=bool(international),
        discipline_type_id=discipline_type_id,
    )
