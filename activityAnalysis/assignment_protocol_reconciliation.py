"""
Bulk compare officials_analysis assignments to public segment_official panels.

Matches assignment competitions (calendar year) to protocol competitions (USFS season
code) on competition type + season, then by name when possible.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session


try:
    from activityAnalysis.international_listing_seasons import (
        usfs_season_code_ending_in_calendar_year,
    )
    from activityAnalysis.international_requirements import (
        NATIONAL_ROLE_DATA_OPERATOR,
        NATIONAL_ROLE_JUDGE,
        NATIONAL_ROLE_REFEREE,
        NATIONAL_ROLE_TECH_CONTROLLER,
        NATIONAL_ROLE_TECH_SPECIALIST,
    )
    from activityAnalysis.load_activity_data import (
        activity_database_is_postgresql,
        engine,
    )
except ModuleNotFoundError:
    from international_listing_seasons import (  # type: ignore[no-redef]
        usfs_season_code_ending_in_calendar_year,
    )
    from international_requirements import (  # type: ignore[no-redef]
        NATIONAL_ROLE_DATA_OPERATOR,
        NATIONAL_ROLE_JUDGE,
        NATIONAL_ROLE_REFEREE,
        NATIONAL_ROLE_TECH_CONTROLLER,
        NATIONAL_ROLE_TECH_SPECIALIST,
    )
    from load_activity_data import (  # type: ignore[no-redef]
        activity_database_is_postgresql,
        engine,
    )

# Directory / protocol panel roles that appear on IJS segment protocols.
PROTOCOL_PANEL_APPOINTMENT_TYPE_NAMES: tuple[str, ...] = (
    "Competition Judge",
    "Referee",
    "Technical Controller",
    "Technical Specialist",
    "Data Operator",
)
PROTOCOL_PANEL_APPOINTMENT_TYPE_IDS: tuple[int, ...] = (
    NATIONAL_ROLE_JUDGE,
    NATIONAL_ROLE_REFEREE,
    NATIONAL_ROLE_DATA_OPERATOR,
    NATIONAL_ROLE_TECH_SPECIALIST,
    NATIONAL_ROLE_TECH_CONTROLLER,
)


def panel_appointment_type_ids(*, include_data_operator: bool) -> tuple[int, ...]:
    """Panel role ids used for SQL filters; Data Operator optional (often absent on protocol)."""
    if include_data_operator:
        return PROTOCOL_PANEL_APPOINTMENT_TYPE_IDS
    return tuple(
        x for x in PROTOCOL_PANEL_APPOINTMENT_TYPE_IDS if x != NATIONAL_ROLE_DATA_OPERATOR
    )


def panel_appointment_type_names(*, include_data_operator: bool) -> tuple[str, ...]:
    if include_data_operator:
        return PROTOCOL_PANEL_APPOINTMENT_TYPE_NAMES
    return tuple(
        n for n in PROTOCOL_PANEL_APPOINTMENT_TYPE_NAMES if n != "Data Operator"
    )

SUMMARY_COLUMNS = [
    "oa_competition_id",
    "oa_name",
    "oa_calendar_year",
    "season_code",
    "competition_type_id",
    "competition_type_name",
    "pub_competition_id",
    "pub_name",
    "pub_year",
    "match_status",
    "assignment_officials",
    "protocol_officials",
    "in_both",
    "role_mismatch",
    "assignments_only",
    "protocol_only",
]

DETAIL_COLUMNS = [
    "oa_competition_id",
    "oa_name",
    "oa_calendar_year",
    "pub_competition_id",
    "pub_name",
    "official_id",
    "official_name",
    "status",
    "assignments",
    "protocol_roles",
    "roles_assign_only",
    "roles_protocol_only",
]


def normalize_competition_name(value: object) -> str:
    """Lowercase, collapse whitespace — for loose competition title matching."""
    return " ".join(str(value or "").lower().split())


def normalize_person_name_key(value: object) -> str:
    """Lowercase compare-key for official names."""
    s = str(value or "")
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", s)
    t = t.strip().lower()
    t = re.sub(r"[-_/.,;:'\"`]+", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


try:
    from analytics import _person_names_equivalent_for_display
except ImportError:  # pragma: no cover

    def _person_names_equivalent_for_display(a: str, b: str) -> bool:
        return normalize_person_name_key(a) == normalize_person_name_key(b)


def season_code_for_calendar_year(calendar_year: int) -> int:
    return int(usfs_season_code_ending_in_calendar_year(int(calendar_year)))


def pub_year_to_season_code(year_val: object) -> int | None:
    """Map ``public.competition.year`` to a USFS season code."""
    if year_val is None or (isinstance(year_val, float) and pd.isna(year_val)):
        return None
    text_val = str(year_val).strip()
    if not text_val.isdigit():
        return None
    n = int(text_val)
    if 1000 <= n <= 9999:
        start_yy, end_yy = divmod(n, 100)
        if 0 <= start_yy <= 99 and 0 <= end_yy <= 99:
            return n
    if 2000 <= n <= 2100:
        return season_code_for_calendar_year(n)
    return None


def competition_names_compatible(oa_name: object, pub_name: object) -> bool:
    a = normalize_competition_name(oa_name)
    b = normalize_competition_name(pub_name)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        return True
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if len(a_tokens) >= 2 and len(b_tokens) >= 2:
        overlap = a_tokens & b_tokens
        if len(overlap) >= min(len(a_tokens), len(b_tokens)) - 1:
            return True
    return False


def _name_match_score(oa_name: object, pub_name: object) -> int:
    if competition_names_compatible(oa_name, pub_name):
        a = normalize_competition_name(oa_name)
        b = normalize_competition_name(pub_name)
        if a == b:
            return 100
        if a in b or b in a:
            return 80
        return 60
    return 0


def _pick_public_competition(
    oa_row: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    if not candidates:
        return None, "no_protocol"
    scored = sorted(
        candidates,
        key=lambda c: (
            -_name_match_score(oa_row["oa_name"], c["pub_name"]),
            str(c.get("pub_name") or "").lower(),
            int(c["pub_competition_id"]),
        ),
    )
    best = scored[0]
    best_score = _name_match_score(oa_row["oa_name"], best["pub_name"])
    strong = [c for c in candidates if _name_match_score(oa_row["oa_name"], c["pub_name"]) == best_score]
    if best_score >= 80 and len(strong) == 1:
        return best, "matched"
    if best_score >= 60 and len(strong) == 1:
        return best, "name_partial"
    if len(candidates) == 1:
        return best, "type_year_only"
    if best_score > 0 and len(strong) == 1:
        return best, "name_partial"
    return best, "ambiguous"


def _official_identity_key(official_id: object, name: object) -> tuple[str, int | str]:
    try:
        oid = int(official_id)
    except (TypeError, ValueError):
        oid = None
    if oid is not None and oid > 0:
        return ("id", oid)
    return ("name", normalize_person_name_key(name))


@dataclass
class _OfficialRosterEntry:
    official_id: int | None
    official_name: str
    all_names: set[str] = field(default_factory=set)
    labels: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        name = str(self.official_name or "").strip()
        if name:
            self.all_names.add(name)


def _int_official_id(value: object) -> int | None:
    try:
        oid = int(value)
    except (TypeError, ValueError):
        return None
    return oid if oid > 0 else None


def _fetch_official_identity_context(session: Session) -> dict[str, dict[int, set[str]]]:
    """
    Linked protocol judge names and directory aliases keyed by ``official_id``.

    Same sources the judge analysis identity groups use for merged spellings.
    """
    judge_names_by_official: dict[int, set[str]] = {}
    for row in session.execute(
        text(
            """
            SELECT jol.official_id, j.name AS judge_name
            FROM public.judge_official_link jol
            INNER JOIN public.judge j ON j.id = jol.judge_id
            WHERE jol.status = 'linked'
              AND jol.official_id IS NOT NULL
            """
        )
    ).mappings():
        oid = _int_official_id(row.get("official_id"))
        judge_name = str(row.get("judge_name") or "").strip()
        if oid is None or not judge_name:
            continue
        judge_names_by_official.setdefault(oid, set()).add(judge_name)

    alias_keys_by_official: dict[int, set[str]] = {}
    try:
        alias_rows = session.execute(
            text(
                """
                SELECT official_id, alias_normalized
                FROM public.official_name_alias
                """
            )
        ).mappings()
    except Exception:
        alias_rows = []
    for row in alias_rows:
        oid = _int_official_id(row.get("official_id"))
        alias = str(row.get("alias_normalized") or "").strip()
        if oid is None or not alias:
            continue
        alias_keys_by_official.setdefault(oid, set()).add(alias)

    return {
        "judge_names_by_official": judge_names_by_official,
        "alias_keys_by_official": alias_keys_by_official,
    }


def _fetch_retired_official_name_keys(session: Session) -> set[str]:
    """Normalized display names from ``officials_analysis.retired_official``."""
    try:
        from activityAnalysis.load_activity_data import (
            load_retired_official_name_keys_from_database,
        )
    except ModuleNotFoundError:
        from load_activity_data import (  # type: ignore[no-redef]
            load_retired_official_name_keys_from_database,
        )
    return load_retired_official_name_keys_from_database(session)


def _official_row_is_retired(
    row: dict[str, Any],
    *,
    retired_name_keys: set[str],
    name_fields: tuple[str, ...] = ("full_name", "directory_name", "official_name"),
) -> bool:
    if not retired_name_keys:
        return False
    for field in name_fields:
        name = str(row.get(field) or "").strip()
        if name and normalize_person_name_key(name) in retired_name_keys:
            return True
    return False


def _filter_retired_official_rows(
    frame: pd.DataFrame,
    *,
    retired_name_keys: set[str],
    name_fields: tuple[str, ...],
) -> pd.DataFrame:
    if frame.empty or not retired_name_keys:
        return frame
    keep_mask = [
        not _official_row_is_retired(
            row, retired_name_keys=retired_name_keys, name_fields=name_fields
        )
        for row in frame.to_dict("records")
    ]
    return frame.loc[keep_mask].copy()


def _name_matches_official_identity(
    name: str,
    official_id: int | None,
    *,
    identity_ctx: dict[str, dict[int, set[str]]],
) -> bool:
    if not name.strip() or official_id is None:
        return False
    for judge_name in identity_ctx.get("judge_names_by_official", {}).get(official_id, ()):
        if _person_names_equivalent_for_display(judge_name, name):
            return True
    name_key = normalize_person_name_key(name)
    for alias_key in identity_ctx.get("alias_keys_by_official", {}).get(official_id, ()):
        if name_key == alias_key:
            return True
        if _person_names_equivalent_for_display(alias_key, name):
            return True
    return False


def _roster_entries_match(
    assign: _OfficialRosterEntry,
    proto: _OfficialRosterEntry,
    *,
    identity_ctx: dict[str, dict[int, set[str]]],
) -> bool:
    a_oid = _int_official_id(assign.official_id)
    p_oid = _int_official_id(proto.official_id)
    if a_oid is not None and p_oid is not None and a_oid == p_oid:
        return True

    for an in assign.all_names:
        for pn in proto.all_names:
            if _person_names_equivalent_for_display(an, pn):
                return True

    if a_oid is not None:
        for pn in proto.all_names:
            if _name_matches_official_identity(pn, a_oid, identity_ctx=identity_ctx):
                return True
    if p_oid is not None:
        for an in assign.all_names:
            if _name_matches_official_identity(an, p_oid, identity_ctx=identity_ctx):
                return True

    return False


def _pair_roster_entries(
    assign_entries: list[_OfficialRosterEntry],
    proto_entries: list[_OfficialRosterEntry],
    *,
    identity_ctx: dict[str, dict[int, set[str]]],
) -> tuple[list[tuple[_OfficialRosterEntry, _OfficialRosterEntry]], list[_OfficialRosterEntry], list[_OfficialRosterEntry]]:
    """Greedy pairing: official_id first, then linked judge names / aliases / fuzzy names."""
    used_assign: set[int] = set()
    used_proto: set[int] = set()
    pairs: list[tuple[_OfficialRosterEntry, _OfficialRosterEntry]] = []

    for ai, assign in enumerate(assign_entries):
        a_oid = _int_official_id(assign.official_id)
        if a_oid is None:
            continue
        for pi, proto in enumerate(proto_entries):
            if pi in used_proto:
                continue
            p_oid = _int_official_id(proto.official_id)
            if p_oid is not None and p_oid == a_oid:
                pairs.append((assign, proto))
                used_assign.add(ai)
                used_proto.add(pi)
                break

    for ai, assign in enumerate(assign_entries):
        if ai in used_assign:
            continue
        for pi, proto in enumerate(proto_entries):
            if pi in used_proto:
                continue
            if _roster_entries_match(assign, proto, identity_ctx=identity_ctx):
                pairs.append((assign, proto))
                used_assign.add(ai)
                used_proto.add(pi)
                break

    assign_only = [e for i, e in enumerate(assign_entries) if i not in used_assign]
    proto_only = [e for i, e in enumerate(proto_entries) if i not in used_proto]
    return pairs, assign_only, proto_only


def _assignment_label(
    appt_name: object,
    discipline_name: object,
    *,
    chief: bool,
    lower_levels_only: bool,
) -> str:
    an = str(appt_name or "").strip()
    if chief:
        role = f"Chief {an}" if an else "Chief"
    else:
        role = an or "Assignment"
    dn = str(discipline_name or "").strip()
    out = f"{role} – {dn}" if dn else role
    if lower_levels_only:
        return f"{out} (lower)"
    return out


def _panel_role_summary_label(role: object) -> str:
    r = str(role or "").strip()
    if not r:
        return ""
    rl = r.lower()
    if rl.startswith("judge"):
        return "Judge"
    if "referee" in rl:
        return "Referee"
    if "technical controller" in rl:
        return "Technical Controller"
    if "assistant technical specialist" in rl or "technical specialist" in rl:
        return "Technical Specialist"
    if "data operator" in rl or "replay operator" in rl:
        return "Data/Replay operator"
    return r


def _protocol_role_label(appointment_name: object, protocol_role: object) -> str:
    appt = str(appointment_name or "").strip()
    if appt:
        return appt
    proto = str(protocol_role or "").strip()
    if proto:
        return _panel_role_summary_label(proto) or proto
    return ""


def _canonical_panel_role(role_text: str) -> str:
    """Normalize assignment / protocol role text to a panel role family."""
    r = str(role_text or "").strip()
    if not r:
        return ""
    while r.lower().startswith("chief "):
        r = r[6:].strip()
    rl = r.lower()
    if rl in ("chief referee", "referee"):
        return "Referee"
    if rl in ("chief competition judge", "competition judge", "judge") or rl.startswith(
        "judge"
    ):
        return "Judge"
    if "competition judge" in rl:
        return "Judge"
    if "referee" in rl:
        return "Referee"
    if "technical controller" in rl:
        return "Technical Controller"
    if "technical specialist" in rl:
        return "Technical Specialist"
    if "data operator" in rl or "replay operator" in rl:
        return "Data Operator"
    return r


def _split_role_and_discipline(text: str) -> tuple[str, str]:
    """Split ``Role – Discipline`` labels (en dash, em dash, or ASCII hyphen)."""
    cleaned = str(text or "").strip()
    if cleaned.endswith(" (lower)"):
        cleaned = cleaned[: -len(" (lower)")].strip()
    for sep in (" – ", " — ", " - "):
        if sep in cleaned:
            role_part, disc = cleaned.split(sep, 1)
            return role_part.strip(), disc.strip()
    return cleaned, ""


def _panel_role_families_compatible(role_a: str, role_b: str) -> bool:
    """True when two canonical roles are the same panel family (e.g. Chief Referee ≈ Referee)."""
    if role_a == role_b:
        return True
    if role_a == "Referee" and role_b == "Referee":
        return True
    if role_a == "Judge" and role_b == "Judge":
        return True
    return False


def _normalize_discipline_label(discipline: str) -> str:
    d = str(discipline or "").strip().lower()
    if d == "ice dance":
        return "dance"
    if d in ("singles and pairs", "singles/pairs", "singles & pairs"):
        return "singles/pairs"
    return d


def _disciplines_compatible_for_role(role: str, assign_disc: str, proto_disc: str) -> bool:
    """
    Discipline match for panel role comparison.

    Judges and referees: directory **Singles/Pairs** assignments match protocol
    **Singles** or **Pairs** segment disciplines. Other official types require an
    exact discipline match (after normalization).
    """
    ad = _normalize_discipline_label(assign_disc)
    pd = _normalize_discipline_label(proto_disc)
    if ad == pd:
        return True
    if role in ("Judge", "Referee"):
        if ad == "singles/pairs" and pd in ("singles", "pairs"):
            return True
    return False


def _panel_role_pairs_compatible(
    assign_pair: tuple[str, str],
    proto_pair: tuple[str, str],
) -> bool:
    ar, ad = assign_pair
    pr, pd = proto_pair
    if not _disciplines_compatible_for_role(ar, ad, pd):
        return False
    return _panel_role_families_compatible(ar, pr)


def _proto_rows_satisfied_by_assignment(
    assign_pair: tuple[str, str],
    proto_pairs: Iterable[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    Protocol rows covered by one assignment row.

    Judges/referees with directory **Singles/Pairs** may satisfy multiple protocol
    segment rows (Singles and Pairs). Other roles use one protocol row per assignment.
    """
    compatible = [pp for pp in proto_pairs if _panel_role_pairs_compatible(assign_pair, pp)]
    if not compatible:
        return []
    ar, ad = assign_pair
    if ar in ("Judge", "Referee") and _normalize_discipline_label(ad) == "singles/pairs":
        return compatible
    return compatible


def _symmetric_panel_role_diff(
    assign_pairs: set[tuple[str, str]],
    proto_pairs: set[tuple[str, str]],
) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """
    Match assignment/protocol role sets (chief referee, Singles/Pairs coverage, etc.).

    Exact discipline rows are paired first so a redundant ``Singles/Pairs`` assignment
    row does not consume protocol ``Singles`` before a separate ``Singles`` assignment
    is matched. Remaining ``Singles/Pairs`` judge/referee rows may still cover multiple
    protocol segment disciplines.
    """
    rem_assign = set(assign_pairs)
    rem_proto = set(proto_pairs)

    changed = True
    while changed:
        changed = False
        for ap in list(rem_assign):
            ar, ad = ap
            for pp in list(rem_proto):
                pr, pd = pp
                if not _panel_role_pairs_compatible(ap, pp):
                    continue
                if _normalize_discipline_label(ad) != _normalize_discipline_label(pd):
                    continue
                rem_assign.remove(ap)
                rem_proto.remove(pp)
                changed = True
                break

    changed = True
    while changed:
        changed = False
        for ap in list(rem_assign):
            satisfied = _proto_rows_satisfied_by_assignment(ap, rem_proto)
            if not satisfied:
                continue
            rem_assign.remove(ap)
            for pp in satisfied:
                rem_proto.remove(pp)
            changed = True
            break

    return rem_assign, rem_proto


def _role_discipline_pairs_from_display_label(label: str) -> set[tuple[str, str]]:
    role_part, disc = _split_role_and_discipline(label)
    role = _canonical_panel_role(role_part)
    if not role:
        return set()
    return {(role, _normalize_discipline_label(disc))}


def _role_discipline_pairs_from_labels(labels: set[str]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for label in labels:
        out |= _role_discipline_pairs_from_display_label(label)
    return out


def _format_role_discipline_pairs(pairs: set[tuple[str, str]]) -> str:
    parts: list[str] = []
    for role, disc in sorted(pairs, key=lambda p: (p[0].lower(), p[1])):
        parts.append(f"{role} – {disc}" if disc else role)
    return ", ".join(parts)


def _compare_panel_role_sets(
    assign_labels: set[str],
    proto_labels: set[str],
) -> tuple[str, set[tuple[str, str]], set[tuple[str, str]]]:
    """
    Return (status, roles_only_on_assignments, roles_only_on_protocol).

    ``status`` is ``both`` when role/discipline sets match, else ``role_mismatch``.
    """
    assign_pairs = _role_discipline_pairs_from_labels(assign_labels)
    proto_pairs = _role_discipline_pairs_from_labels(proto_labels)
    only_assign, only_proto = _symmetric_panel_role_diff(assign_pairs, proto_pairs)
    if only_assign or only_proto:
        return "role_mismatch", only_assign, only_proto
    return "both", set(), set()


def match_assignment_competitions_to_public(
    oa_competitions: pd.DataFrame,
    pub_competitions: pd.DataFrame,
) -> pd.DataFrame:
    """Pair assignment competitions with best public competition candidate."""
    if oa_competitions.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    pub = pub_competitions.copy()
    pub["season_code"] = pub["pub_year"].map(pub_year_to_season_code)

    pub_by_key: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in pub.to_dict("records"):
        sc = row.get("season_code")
        ctid = row.get("competition_type_id")
        if sc is None or ctid is None or pd.isna(ctid):
            continue
        key = (int(ctid), int(sc))
        pub_by_key.setdefault(key, []).append(row)

    records: list[dict[str, Any]] = []
    for oa in oa_competitions.to_dict("records"):
        calendar_year = int(oa["oa_calendar_year"])
        season_code = season_code_for_calendar_year(calendar_year)
        ctid = int(oa["competition_type_id"])
        candidates = pub_by_key.get((ctid, season_code), [])
        pub_row, status = _pick_public_competition(oa, candidates)
        records.append(
            {
                "oa_competition_id": int(oa["oa_competition_id"]),
                "oa_name": oa["oa_name"],
                "oa_calendar_year": calendar_year,
                "season_code": season_code,
                "competition_type_id": ctid,
                "competition_type_name": oa.get("competition_type_name"),
                "pub_competition_id": int(pub_row["pub_competition_id"]) if pub_row else None,
                "pub_name": pub_row.get("pub_name") if pub_row else None,
                "pub_year": pub_row.get("pub_year") if pub_row else None,
                "match_status": status,
            }
        )
    return pd.DataFrame(records)


def compare_official_sets(
    assignments: pd.DataFrame,
    protocol: pd.DataFrame,
    *,
    matches: pd.DataFrame,
    identity_ctx: dict[str, dict[int, set[str]]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build per-competition summary counts and per-official detail rows.

    Officials are matched on shared ``official_id``, equivalent display names, and
    linked protocol judge names / directory aliases (same rules as judge analysis).

    When a person appears on both sides, panel **role + discipline** sets are compared;
    extra protocol roles (e.g. Technical Controller without an assignment) yield
    ``role_mismatch``.
    """
    if matches.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS), pd.DataFrame(columns=DETAIL_COLUMNS)

    identity_ctx = identity_ctx or {
        "judge_names_by_official": {},
        "alias_keys_by_official": {},
    }

    assign_by_comp: dict[int, dict[tuple[str, int | str], _OfficialRosterEntry]] = {}
    if not assignments.empty:
        for row in assignments.to_dict("records"):
            cid = int(row["competition_id"])
            key = _official_identity_key(row.get("official_id"), row.get("full_name"))
            label = _assignment_label(
                row.get("appt_type_name"),
                row.get("discipline_name"),
                chief=bool(row.get("chief")),
                lower_levels_only=bool(row.get("lower_levels_only")),
            )
            bucket = assign_by_comp.setdefault(cid, {})
            if key not in bucket:
                bucket[key] = _OfficialRosterEntry(
                    official_id=_int_official_id(row.get("official_id")),
                    official_name=str(row.get("full_name") or "").strip(),
                )
            bucket[key].labels.add(label)

    protocol_by_comp: dict[int, dict[tuple[str, int | str], _OfficialRosterEntry]] = {}
    if not protocol.empty:
        for row in protocol.to_dict("records"):
            cid = int(row["competition_id"])
            directory_name = str(row.get("directory_name") or "").strip()
            protocol_name = str(row.get("official_name") or "").strip()
            display_name = directory_name or protocol_name
            key = _official_identity_key(row.get("official_id"), display_name)
            role = _protocol_role_label(row.get("appointment_name"), row.get("protocol_role"))
            disc = str(row.get("discipline") or "").strip()
            label = f"{role} – {disc}" if role and disc else (role or disc or "Panel")
            bucket = protocol_by_comp.setdefault(cid, {})
            if key not in bucket:
                bucket[key] = _OfficialRosterEntry(
                    official_id=_int_official_id(row.get("official_id")),
                    official_name=display_name,
                )
            if protocol_name:
                bucket[key].all_names.add(protocol_name)
            if directory_name:
                bucket[key].all_names.add(directory_name)
            bucket[key].labels.add(label)

    oa_to_pub = {
        int(r["oa_competition_id"]): int(r["pub_competition_id"])
        for _, r in matches.iterrows()
        if pd.notna(r.get("pub_competition_id"))
    }

    summary_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []

    for _, m in matches.iterrows():
        oa_id = int(m["oa_competition_id"])
        pub_id = oa_to_pub.get(oa_id)
        assign_entries = list(assign_by_comp.get(oa_id, {}).values())
        proto_entries = (
            list(protocol_by_comp.get(pub_id, {}).values()) if pub_id is not None else []
        )

        pairs, assign_only_entries, proto_only_entries = _pair_roster_entries(
            assign_entries,
            proto_entries,
            identity_ctx=identity_ctx,
        )

        roles_matched = 0
        role_mismatch_count = 0

        summary = m.to_dict()
        summary.update(
            {
                "assignment_officials": len(assign_entries),
                "protocol_officials": len(proto_entries),
                "in_both": 0,
                "role_mismatch": 0,
                "assignments_only": len(assign_only_entries),
                "protocol_only": len(proto_only_entries),
            }
        )
        summary_rows.append(summary)

        for assign, proto in sorted(
            pairs,
            key=lambda pair: (
                (pair[0].official_name or pair[1].official_name or "").lower(),
            ),
        ):
            status, only_assign_roles, only_proto_roles = _compare_panel_role_sets(
                assign.labels,
                proto.labels,
            )
            if status == "both":
                roles_matched += 1
            else:
                role_mismatch_count += 1
            oid = assign.official_id or proto.official_id
            name = assign.official_name or proto.official_name or ""
            detail_rows.append(
                {
                    "oa_competition_id": oa_id,
                    "oa_name": m.get("oa_name"),
                    "oa_calendar_year": m.get("oa_calendar_year"),
                    "pub_competition_id": pub_id,
                    "pub_name": m.get("pub_name"),
                    "official_id": oid,
                    "official_name": name,
                    "status": status,
                    "assignments": ", ".join(sorted(assign.labels, key=str.lower)),
                    "protocol_roles": ", ".join(sorted(proto.labels, key=str.lower)),
                    "roles_assign_only": _format_role_discipline_pairs(only_assign_roles),
                    "roles_protocol_only": _format_role_discipline_pairs(only_proto_roles),
                }
            )

        summary_rows[-1]["in_both"] = roles_matched
        summary_rows[-1]["role_mismatch"] = role_mismatch_count

        for entry in sorted(assign_only_entries, key=lambda e: (e.official_name or "").lower()):
            detail_rows.append(
                {
                    "oa_competition_id": oa_id,
                    "oa_name": m.get("oa_name"),
                    "oa_calendar_year": m.get("oa_calendar_year"),
                    "pub_competition_id": pub_id,
                    "pub_name": m.get("pub_name"),
                    "official_id": entry.official_id,
                    "official_name": entry.official_name,
                    "status": "assignments_only",
                    "assignments": ", ".join(sorted(entry.labels, key=str.lower)),
                    "protocol_roles": "",
                    "roles_assign_only": _format_role_discipline_pairs(
                        _role_discipline_pairs_from_labels(entry.labels)
                    ),
                    "roles_protocol_only": "",
                }
            )

        for entry in sorted(proto_only_entries, key=lambda e: (e.official_name or "").lower()):
            detail_rows.append(
                {
                    "oa_competition_id": oa_id,
                    "oa_name": m.get("oa_name"),
                    "oa_calendar_year": m.get("oa_calendar_year"),
                    "pub_competition_id": pub_id,
                    "pub_name": m.get("pub_name"),
                    "official_id": entry.official_id,
                    "official_name": entry.official_name,
                    "status": "protocol_only",
                    "assignments": "",
                    "protocol_roles": ", ".join(sorted(entry.labels, key=str.lower)),
                    "roles_assign_only": "",
                    "roles_protocol_only": _format_role_discipline_pairs(
                        _role_discipline_pairs_from_labels(entry.labels)
                    ),
                }
            )

    summary_df = pd.DataFrame(summary_rows)
    detail_df = pd.DataFrame(detail_rows)
    if summary_df.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS), pd.DataFrame(columns=DETAIL_COLUMNS)
    for frame, default_cols in (
        (summary_df, SUMMARY_COLUMNS),
        (detail_df, DETAIL_COLUMNS),
    ):
        for c in default_cols:
            if c not in frame.columns:
                frame[c] = None
    if detail_df.empty:
        detail_df = pd.DataFrame(columns=DETAIL_COLUMNS)
    return summary_df[SUMMARY_COLUMNS], detail_df[DETAIL_COLUMNS]


def _panel_type_ids_sql_in(*, include_data_operator: bool) -> str:
    """Comma-separated ``appointment_types.id`` literals for panel roles (not user input)."""
    return ", ".join(
        str(int(x)) for x in panel_appointment_type_ids(include_data_operator=include_data_operator)
    )


def _protocol_panel_role_sql_predicate(
    so_alias: str = "so",
    *,
    include_data_operator: bool,
) -> str:
    """Match protocol role text when ``appointment_type_id`` is unset on the row."""
    role = f"LOWER(BTRIM({so_alias}.role))"
    data_op = (
        f" OR {role} LIKE '%data operator%' OR {role} LIKE '%replay operator%'"
        if include_data_operator
        else ""
    )
    return f"""(
      {role} LIKE 'judge%'
      OR {role} LIKE '%referee%'
      OR {role} LIKE '%technical controller%'
      OR {role} LIKE '%technical specialist%'
      {data_op}
    )"""


def _protocol_panel_official_sql_predicate(
    so_alias: str = "so",
    *,
    include_data_operator: bool,
) -> str:
    """Panel officials only: linked appointment type or recognizable protocol role text."""
    panel_ids = _panel_type_ids_sql_in(include_data_operator=include_data_operator)
    return f"""(
      {so_alias}.appointment_type_id IN ({panel_ids})
      OR (
        {so_alias}.appointment_type_id IS NULL
        AND {_protocol_panel_role_sql_predicate(so_alias, include_data_operator=include_data_operator)}
      )
    )"""


def _fetch_oa_competitions_with_assignments(
    session: Session,
    *,
    competition_type_ids: list[int] | None,
    include_data_operator: bool,
) -> pd.DataFrame:
    type_filter = ""
    params: dict[str, Any] = {}
    if competition_type_ids:
        type_filter = "AND c.competition_type_id IN :type_ids"
        params["type_ids"] = [int(x) for x in competition_type_ids]
    stmt = text(
        f"""
        SELECT
            c.id AS oa_competition_id,
            c.name AS oa_name,
            c.year AS oa_calendar_year,
            c.competition_type_id,
            ct.name AS competition_type_name,
            COUNT(DISTINCT a.official_id)::int AS assignment_row_count
        FROM officials_analysis.competition c
        JOIN officials_analysis.competition_type ct
          ON ct.id = c.competition_type_id
        JOIN officials_analysis.assignment a
          ON a.competition_id = c.id
         AND a.appointment_type_id IN ({_panel_type_ids_sql_in(include_data_operator=include_data_operator)})
        WHERE 1=1
        {type_filter}
        GROUP BY c.id, c.name, c.year, c.competition_type_id, ct.name
        ORDER BY c.year DESC, c.competition_type_id, lower(c.name)
        """
    )
    if competition_type_ids:
        stmt = stmt.bindparams(bindparam("type_ids", expanding=True))
    rows = session.execute(stmt, params).mappings().all()
    return pd.DataFrame(rows)


def _fetch_public_competitions_with_protocol(
    session: Session,
    *,
    competition_type_ids: list[int] | None,
    include_data_operator: bool,
) -> pd.DataFrame:
    type_filter = ""
    params: dict[str, Any] = {}
    if competition_type_ids:
        type_filter = "AND c.officials_analysis_competition_type_id IN :type_ids"
        params["type_ids"] = [int(x) for x in competition_type_ids]
    stmt = text(
        f"""
        SELECT
            c.id AS pub_competition_id,
            c.name AS pub_name,
            c.year AS pub_year,
            c.officials_analysis_competition_type_id AS competition_type_id,
            COUNT(DISTINCT so.id)::int AS segment_official_rows
        FROM public.competition c
            JOIN public.segment s ON s.competition_id = c.id
            JOIN public.segment_official so ON so.segment_id = s.id
             AND {_protocol_panel_official_sql_predicate("so", include_data_operator=include_data_operator)}
            WHERE c.officials_analysis_competition_type_id IS NOT NULL
            {type_filter}
            GROUP BY c.id, c.name, c.year, c.officials_analysis_competition_type_id
            ORDER BY c.year DESC, c.officials_analysis_competition_type_id, lower(c.name)
            """
    )
    if competition_type_ids:
        stmt = stmt.bindparams(bindparam("type_ids", expanding=True))
    rows = session.execute(stmt, params).mappings().all()
    return pd.DataFrame(rows)


def _fetch_assignment_rows(
    session: Session,
    oa_competition_ids: list[int],
    *,
    include_data_operator: bool,
) -> pd.DataFrame:
    if not oa_competition_ids:
        return pd.DataFrame(
            columns=[
                "competition_id",
                "official_id",
                "full_name",
                "appt_type_name",
                "discipline_name",
                "chief",
                "lower_levels_only",
            ]
        )
    rows = session.execute(
        text(
            f"""
            SELECT
                a.competition_id,
                o.id AS official_id,
                o.full_name,
                at.name AS appt_type_name,
                d.name AS discipline_name,
                a.chief,
                a.lower_levels_only
            FROM officials_analysis.assignment a
            JOIN officials_analysis.officials o ON o.id = a.official_id
            JOIN officials_analysis.appointment_types at
              ON at.id = a.appointment_type_id
            JOIN officials_analysis.disciplines d ON d.id = a.discipline_id
            WHERE a.competition_id IN :comp_ids
              AND a.appointment_type_id IN ({_panel_type_ids_sql_in(include_data_operator=include_data_operator)})
            ORDER BY a.competition_id, lower(o.full_name)
            """
        ).bindparams(bindparam("comp_ids", expanding=True)),
        {"comp_ids": [int(x) for x in oa_competition_ids]},
    ).mappings().all()
    return pd.DataFrame(rows)


def _fetch_protocol_rows(
    session: Session,
    pub_competition_ids: list[int],
    *,
    include_data_operator: bool,
) -> pd.DataFrame:
    if not pub_competition_ids:
        return pd.DataFrame(
            columns=[
                "competition_id",
                "official_id",
                "official_name",
                "directory_name",
                "appointment_name",
                "protocol_role",
                "discipline",
            ]
        )
    rows = session.execute(
        text(
            f"""
            SELECT
                c.id AS competition_id,
                so.official_id,
                so.official_name,
                o.full_name AS directory_name,
                at.name AS appointment_name,
                so.role AS protocol_role,
                dt.name AS discipline
            FROM public.segment_official so
            JOIN public.segment s ON s.id = so.segment_id
            JOIN public.competition c ON c.id = s.competition_id
            LEFT JOIN public.discipline_type dt ON dt.id = s.discipline_type_id
            LEFT JOIN officials_analysis.officials o ON o.id = so.official_id
            LEFT JOIN officials_analysis.appointment_types at
              ON at.id = so.appointment_type_id
            WHERE c.id IN :comp_ids
              AND {_protocol_panel_official_sql_predicate("so", include_data_operator=include_data_operator)}
            ORDER BY c.id, lower(COALESCE(o.full_name, so.official_name))
            """
        ).bindparams(bindparam("comp_ids", expanding=True)),
        {"comp_ids": [int(x) for x in pub_competition_ids]},
    ).mappings().all()
    return pd.DataFrame(rows)


def load_assignment_protocol_reconciliation(
    *,
    competition_type_ids: list[int] | None = None,
    only_mismatches: bool = False,
    only_with_protocol_match: bool = False,
    include_data_operator: bool = False,
    exclude_retired: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Bulk reconciliation of assignment rosters vs protocol panels.

    Compares Competition Judge, Referee, Technical Controller, and Technical
    Specialist by default. **Data Operator** is optional (``include_data_operator``)
    because assignments often list D/O roles that do not appear on IJS protocols.

    Officials on both sides are compared by panel role + discipline; mismatches
    appear as ``role_mismatch`` in the detail output.

    When ``exclude_retired`` is true (default), names in ``officials_analysis.retired_official``
    (synced from ``Retired_officials.xlsx``) are omitted from both assignment and
    protocol rosters.

    Returns (summary_df, detail_df). Empty when not PostgreSQL or no data.
    """
    empty_summary = pd.DataFrame(columns=SUMMARY_COLUMNS)
    empty_detail = pd.DataFrame(columns=DETAIL_COLUMNS)
    if not activity_database_is_postgresql():
        return empty_summary, empty_detail

    with Session(engine) as session:
        oa_comps = _fetch_oa_competitions_with_assignments(
            session,
            competition_type_ids=competition_type_ids,
            include_data_operator=include_data_operator,
        )
        if oa_comps.empty:
            return empty_summary, empty_detail

        pub_comps = _fetch_public_competitions_with_protocol(
            session,
            competition_type_ids=competition_type_ids,
            include_data_operator=include_data_operator,
        )
        matches = match_assignment_competitions_to_public(oa_comps, pub_comps)

        if only_with_protocol_match:
            matches = matches[matches["pub_competition_id"].notna()].copy()

        oa_ids = [int(x) for x in matches["oa_competition_id"].tolist()]
        pub_ids = [
            int(x)
            for x in matches["pub_competition_id"].dropna().tolist()
        ]
        assignments = _fetch_assignment_rows(
            session,
            oa_ids,
            include_data_operator=include_data_operator,
        )
        protocol = _fetch_protocol_rows(
            session,
            pub_ids,
            include_data_operator=include_data_operator,
        )
        identity_ctx = _fetch_official_identity_context(session)
        retired_name_keys = (
            _fetch_retired_official_name_keys(session) if exclude_retired else set()
        )

    if exclude_retired and retired_name_keys:
        assignments = _filter_retired_official_rows(
            assignments,
            retired_name_keys=retired_name_keys,
            name_fields=("full_name",),
        )
        protocol = _filter_retired_official_rows(
            protocol,
            retired_name_keys=retired_name_keys,
            name_fields=("directory_name", "official_name"),
        )

    summary, detail = compare_official_sets(
        assignments,
        protocol,
        matches=matches,
        identity_ctx=identity_ctx,
    )

    if only_mismatches and not summary.empty:
        mismatch_mask = (
            (summary["match_status"] != "matched")
            | (summary["assignments_only"] > 0)
            | (summary["protocol_only"] > 0)
            | (summary["role_mismatch"] > 0)
        )
        bad_oa_ids = set(summary.loc[mismatch_mask, "oa_competition_id"].astype(int))
        summary = summary.loc[summary["oa_competition_id"].isin(bad_oa_ids)].copy()
        detail = detail.loc[detail["oa_competition_id"].isin(bad_oa_ids)].copy()

    return summary, detail
