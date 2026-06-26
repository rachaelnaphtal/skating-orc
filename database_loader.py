from __future__ import annotations

import datetime
import decimal
import re
import unicodedata
from sqlalchemy import select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import bindparam, text
import pandas as pd
from sqlalchemy.orm import Session
from models import Judge, Competition, Segment, Skater, SkaterSegment, Element, ElementScorePerJudge, PcsScorePerJudge, PcsType, ElementType, DisciplineType, SegmentOfficial
from database import get_db_session, test_connection
from pcs_fall_rule_errors import (
    max_pcs_for_fall_count,
    pcs_score_exceeds_fall_limit,
    program_fall_count_from_elements,
)
from rule_errors_policy import (
    MIN_PCS_FALL_RULE_ERROR_SEASON_YEAR,
    PCS_FALL_RULE_DISCIPLINE_TYPE_IDS,
    segment_supports_pcs_fall_rule_errors,
    should_flag_pcs_fall_rule_errors,
    should_flag_rule_errors,
)

try:
    from judge_official_link_core import (
        normalize_name,
        protocol_person_match_key,
        suggest_matches,
    )
except ImportError:  # pragma: no cover
    normalize_name = None  # type: ignore[misc, assignment]
    protocol_person_match_key = None  # type: ignore[misc, assignment]
    suggest_matches = None  # type: ignore[misc, assignment]


def coerce_competition_date(value: object) -> datetime.date | None:
    """Map scrape/metadata values to ``date`` or ``None`` (never empty string)."""
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def coerce_competition_location(value: object) -> str | None:
    s = str(value or "").strip()
    return s or None


def _normalize_person_name(name: str | None) -> str:
    """Case-insensitive match key; prefers judge_official_link_core.protocol_person_match_key."""
    if protocol_person_match_key is not None:
        return protocol_person_match_key(name)
    if not name:
        return ""
    return " ".join(name.lower().split()).strip()


_SCRAPED_JUDGE_HONORIFIC_RE = re.compile(r"^(?:Mr\.?|Ms\.?)\s*", re.IGNORECASE)


def normalize_scraped_judge_name(name: str | None) -> str:
    """Strip leading ``Mr.`` / ``Ms.`` (optional period, any case) from IJS panel names."""
    if name is None:
        return ""
    s = " ".join(str(name).split())
    s = _SCRAPED_JUDGE_HONORIFIC_RE.sub("", s, count=1)
    return s.strip()


def judge_person_match_key(name: str | None) -> str:
    """Normalized key for case-insensitive judge deduplication (``public.judge.name``)."""
    return _normalize_person_name(normalize_scraped_judge_name(name))


def select_canonical_judge_ids_per_match_key(
    candidates: dict[str, list[int]],
    score_counts: dict[int, int],
) -> dict[str, int]:
    """
    Pick one ``judge.id`` per normalized match key when duplicates exist.

    Prefer the row referenced by the most element/PCS scores; tie-break to lowest id
    (original load row).
    """
    out: dict[str, int] = {}
    for key, ids in candidates.items():
        unique_ids = list(dict.fromkeys(int(i) for i in ids))
        if len(unique_ids) == 1:
            out[key] = unique_ids[0]
            continue
        out[key] = min(
            unique_ids,
            key=lambda jid: (-int(score_counts.get(jid, 0)), jid),
        )
    return out


def _judge_match_key_sql_expr() -> str:
    return "lower(regexp_replace(trim(name), '\\\\s+', ' ', 'g'))"


def _normalize_skater_name_key(name: str) -> str:
    if not name:
        return ""
    t = unicodedata.normalize("NFKC", str(name))
    t = t.strip().lower()
    t = re.sub(r"[-_/.,;:'\"`]+", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def _skater_names_equivalent(parsed_name: str, db_name: str) -> bool:
    """Match protocol skater labels to ``public.skater.name`` across case/order variants."""
    ka = _normalize_skater_name_key(parsed_name)
    kb = _normalize_skater_name_key(db_name)
    if not ka or not kb:
        return ka == kb
    if ka == kb:
        return True
    return sorted(ka.split()) == sorted(kb.split())


def _resolve_skater_dict_key(
    skater_dict: dict[str, int], parsed_name: str
) -> str | None:
    text = str(parsed_name)
    if text in skater_dict:
        return text
    for db_name in skater_dict:
        if _skater_names_equivalent(text, db_name):
            return db_name
    return None


_ELEMENT_MARKING_TOKEN_RE = re.compile(r"^[F*<!>qnscuSCUex,b|]+$", re.IGNORECASE)


def _element_name_candidates_from_rule_error(element_label: str) -> list[str]:
    """
    DB element names omit separate notes; rule-error rows may still carry
    ``\"3Lz+2T F\"`` or ``\"2F+2T+1Lo<<+2Lo<<* *\"`` from legacy formatting.
    """
    text = str(element_label).strip()
    if not text:
        return []
    candidates: list[str] = []
    seen: set[str] = set()

    def add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            candidates.append(name)

    add(text)
    parts = text.split()
    if len(parts) == 1:
        return candidates
    base = parts[0]
    add(base)
    tail = parts[1:]
    if all(_ELEMENT_MARKING_TOKEN_RE.fullmatch(t) for t in tail):
        add(base + "".join(tail))
    return candidates


def _resolve_element_dict_key(
    elem_id_by_pair: dict[tuple[int, str], int],
    skater_segment_id: int,
    element_label: str,
) -> str | None:
    for candidate in _element_name_candidates_from_rule_error(element_label):
        if (skater_segment_id, candidate) in elem_id_by_pair:
            return candidate
    return None


def _competition_flags_from_discipline_type_name(
    name: str | None,
) -> tuple[bool, bool, bool, bool]:
    """
    Map ``discipline_type.name`` (as stored for segments) to competition booleans
    ``(singles, pairs, dance, synchronized)``.
    """
    if not name or not str(name).strip():
        return False, False, False, False
    key = str(name).strip().lower()
    singles = key == "singles"
    pairs = key == "pairs"
    dance = key in ("ice dance", "solo dance")
    synchronized = key == "synchronized"
    return singles, pairs, dance, synchronized

# Initialize connection.
# conn = st.connection("postgresql", type="sql")

# IJS / USFS results panel role → officials_analysis.appointment_types.id
# (values from directory; see user specification.)
APPOINTMENT_TYPE_ID_JUDGE = 1
APPOINTMENT_TYPE_ID_REFEREE = 4
APPOINTMENT_TYPE_ID_TECH_SPECIALIST = 9
APPOINTMENT_TYPE_ID_TECH_CONTROLLER = 11
APPOINTMENT_TYPE_ID_DATA_OR_REPLAY_OPERATOR = 8


def appointment_type_id_for_ijs_role(role_label: str) -> int | None:
    r = (role_label or "").strip()
    r_lower = r.lower()
    if r.startswith("Judge"):
        return APPOINTMENT_TYPE_ID_JUDGE
    if "Referee" in r:
        return APPOINTMENT_TYPE_ID_REFEREE
    if "Technical Controller" in r:
        return APPOINTMENT_TYPE_ID_TECH_CONTROLLER
    if "Assistant Technical Specialist" in r or "Technical Specialist" in r:
        return APPOINTMENT_TYPE_ID_TECH_SPECIALIST
    if r_lower.startswith("data operator") or r_lower.startswith("replay operator"):
        return APPOINTMENT_TYPE_ID_DATA_OR_REPLAY_OPERATOR
    return None


class DatabaseLoader:
    def __init__(self, session: Session, *, defer_commits: bool = False):
        self.session = session
        self.defer_commits = defer_commits
        self._isu_official_schema_cache: bool | None = None

    def commit(self) -> None:
        """Flush pending ORM work and commit (used at end of batch scrapes)."""
        self.session.flush()
        self.session.commit()

    def _maybe_flush(self) -> None:
        """Flush unless commits are deferred (backfill batches flush at ``commit()``)."""
        if not self.defer_commits:
            self.session.flush()

    def _persist(self) -> None:
        """Commit, or only flush when ``defer_commits`` is set (batch load)."""
        if self.defer_commits:
            self.session.flush()
        else:
            self.session.commit()

    def replace_segment_officials(self, segment_id: int, rows: list) -> None:
        """
        Persist IJS segment officials. Replaces prior rows for this segment.
        ``official_id`` is set from (in order): ``judge_official_link`` for the judge
        row from ``insert_judge``; else ``public.official_name_alias`` on the
        normalized scraped name; else exact match on ``officials.full_name``;
        else high-confidence fuzzy match on the US directory.

        When no US match, ``isu_official_id`` uses the same order against the ISU roster
        (``judge_isu_official_link``, ``isu_official_name_alias``, exact, fuzzy).

        ``appointment_type_id`` follows role labels.
        """
        if not rows:
            return
        self.session.query(SegmentOfficial).filter(
            SegmentOfficial.segment_id == segment_id
        ).delete(synchronize_session=False)
        choices_cache: dict[int, str] | None = None
        isu_choices_cache: dict[int, str] | None = None
        for r in rows:
            official_name = normalize_scraped_judge_name(r["name"])
            role = r["role"]
            judge_id = self.insert_judge(official_name)
            appt_type_id = appointment_type_id_for_ijs_role(role)
            oid = self._official_id_from_judge_id(judge_id)
            if oid is None:
                oid = self._official_id_from_name_alias(official_name)
            if oid is None:
                oid = self._official_id_from_exact_directory_name(official_name)
            if oid is None:
                if choices_cache is None:
                    choices_cache = self._load_official_directory_choices()
                oid = self._official_id_from_fuzzy_directory_name(
                    official_name, choices_cache
                )
            isu_oid = None
            if oid is None and self._isu_official_schema_ready():
                isu_oid = self._isu_official_id_from_judge_id(judge_id)
                if isu_oid is None:
                    isu_oid = self._isu_official_id_from_name_alias(official_name)
                if isu_oid is None:
                    isu_oid = self._isu_official_id_from_exact_name(official_name)
                if isu_oid is None:
                    if isu_choices_cache is None:
                        isu_choices_cache = self._load_isu_official_choices()
                    isu_oid = self._isu_official_id_from_fuzzy_name(
                        official_name, isu_choices_cache
                    )
            self.session.add(
                SegmentOfficial(
                    segment_id=segment_id,
                    official_name=official_name,
                    official_id=oid,
                    isu_official_id=isu_oid,
                    role=role,
                    appointment_type_id=appt_type_id,
                )
            )
        self._persist()

    def _flush(self) -> None:
        """Persist pending work to the DB without ending the transaction (fast for bulk loads)."""
        self.session.flush()

    _BULK_CHUNK = 3500

    @staticmethod
    def _to_decimal(v) -> decimal.Decimal:
        if isinstance(v, decimal.Decimal):
            return v
        return decimal.Decimal(str(v))

    @staticmethod
    def _numeric_eq(a, b) -> bool:
        """Compare panel scores; DB Numeric may differ only in trailing zeros."""
        da = DatabaseLoader._to_decimal(a)
        db = DatabaseLoader._to_decimal(b)
        q = decimal.Decimal("0.01")
        return da.quantize(q) == db.quantize(q)

    def _dataframe_element_scores(
        self, all_element_dict: list | dict,
    ) -> pd.DataFrame:
        if isinstance(all_element_dict, dict):
            if not all_element_dict:
                return pd.DataFrame()
            return pd.DataFrame.from_dict(all_element_dict)
        return pd.DataFrame(all_element_dict)

    def _dataframe_pcs(self, all_pcs_dict: list | dict) -> pd.DataFrame:
        if isinstance(all_pcs_dict, dict):
            if not all_pcs_dict:
                return pd.DataFrame()
            return pd.DataFrame.from_dict(all_pcs_dict)
        return pd.DataFrame(all_pcs_dict)

    @staticmethod
    def _merge_judge_names_from_scores(
        panel_judge_names: list[str],
        score_rows: pd.DataFrame,
        *,
        judge_name_col: str = "Judge Name",
    ) -> list[str]:
        """Panel list plus any judge names present in parsed score rows (e.g. inferred placeholders)."""
        merged: list[str] = []
        seen: set[str] = set()
        for raw in list(panel_judge_names or []):
            norm = normalize_scraped_judge_name(str(raw))
            if norm and norm not in seen:
                seen.add(norm)
                merged.append(str(raw))
        if not score_rows.empty and judge_name_col in score_rows.columns:
            for raw in score_rows[judge_name_col].astype(str).unique().tolist():
                norm = normalize_scraped_judge_name(raw)
                if norm and norm not in seen:
                    seen.add(norm)
                    merged.append(raw)
        return merged

    def _judge_score_counts(self, judge_ids: list[int]) -> dict[int, int]:
        unique_ids = list(dict.fromkeys(int(i) for i in judge_ids))
        if not unique_ids:
            return {}
        rows = self.session.execute(
            text("""
                SELECT judge_id, SUM(cnt)::bigint AS total
                FROM (
                    SELECT judge_id, COUNT(*)::bigint AS cnt
                    FROM element_score_per_judge
                    WHERE judge_id IN :ids
                    GROUP BY judge_id
                    UNION ALL
                    SELECT judge_id, COUNT(*)::bigint AS cnt
                    FROM pcs_score_per_judge
                    WHERE judge_id IN :ids
                    GROUP BY judge_id
                ) sub
                GROUP BY judge_id
            """).bindparams(bindparam("ids", expanding=True)),
            {"ids": unique_ids},
        ).all()
        return {int(row.judge_id): int(row.total) for row in rows}

    def _ensure_judges_by_name(self, names: list[str]) -> dict[str, int]:
        displays = [normalize_scraped_judge_name(str(n)) for n in names]
        unique_displays = list(dict.fromkeys(d for d in displays if d))
        if not unique_displays:
            return {}
        keys = {d: judge_person_match_key(d) for d in unique_displays}
        unique_keys = list(dict.fromkeys(keys.values()))
        by_key: dict[str, int] = {}
        if unique_keys:
            match_key_expr = _judge_match_key_sql_expr()
            rows = self.session.execute(
                text(f"""
                    SELECT id, name, {match_key_expr} AS match_key
                    FROM judge
                    WHERE {match_key_expr} IN :keys
                    ORDER BY id
                """).bindparams(bindparam("keys", expanding=True)),
                {"keys": unique_keys},
            ).all()
            candidates: dict[str, list[int]] = {}
            for row in rows:
                mk = str(row.match_key)
                candidates.setdefault(mk, []).append(int(row.id))
            dup_ids = [
                jid for jid_list in candidates.values() if len(jid_list) > 1 for jid in jid_list
            ]
            score_counts = self._judge_score_counts(dup_ids) if dup_ids else {}
            by_key = select_canonical_judge_ids_per_match_key(
                candidates, score_counts
            )
        by_name: dict[str, int] = {}
        for display in unique_displays:
            mk = keys[display]
            if mk in by_key:
                by_name[display] = by_key[mk]
                continue
            self.session.add(Judge(name=display))
            self.session.flush()
            by_name[display] = int(
                self.session.execute(
                    select(Judge.id).where(Judge.name == display)
                ).scalar_one()
            )
            by_key[mk] = by_name[display]
        return by_name

    def _ensure_skaters_by_name(self, names: list[str]) -> dict[str, int]:
        unique = list(dict.fromkeys(names))
        if not unique:
            return {}
        rows = self.session.execute(
            select(Skater.id, Skater.name).where(Skater.name.in_(unique))
        ).all()
        by_name = {str(r.name): int(r.id) for r in rows}
        missing = [n for n in unique if n not in by_name]
        for n in missing:
            self.session.add(Skater(name=n))
        if missing:
            self.session.flush()
            rows = self.session.execute(
                select(Skater.id, Skater.name).where(Skater.name.in_(missing))
            ).all()
            for r in rows:
                by_name[str(r.name)] = int(r.id)
        return by_name

    def _ensure_skater_segments_map(
        self, segment_id: int, skater_ids: list[int],
    ) -> dict[int, int]:
        """Map skater_id -> skater_segment.id for this segment."""
        unique = list(dict.fromkeys(skater_ids))
        if not unique:
            return {}
        rows = self.session.execute(
            select(SkaterSegment.id, SkaterSegment.skater_id).where(
                SkaterSegment.segment_id == segment_id,
                SkaterSegment.skater_id.in_(unique),
            )
        ).all()
        out = {int(r.skater_id): int(r.id) for r in rows}
        missing = [sid for sid in unique if sid not in out]
        for sid in missing:
            self.session.add(
                SkaterSegment(segment_id=segment_id, skater_id=sid)
            )
        if missing:
            self.session.flush()
            rows = self.session.execute(
                select(SkaterSegment.id, SkaterSegment.skater_id).where(
                    SkaterSegment.segment_id == segment_id,
                    SkaterSegment.skater_id.in_(missing),
                )
            ).all()
            for r in rows:
                out[int(r.skater_id)] = int(r.id)
        return out

    def _ensure_element_types_by_name(self, names: list[str]) -> dict[str, int]:
        unique = list(dict.fromkeys(names))
        if not unique:
            return {}
        rows = self.session.execute(
            select(ElementType.id, ElementType.name).where(
                ElementType.name.in_(unique)
            )
        ).all()
        by_name = {str(r.name): int(r.id) for r in rows}
        missing = [n for n in unique if n not in by_name]
        for n in missing:
            self.session.add(ElementType(name=n))
        if missing:
            self.session.flush()
            rows = self.session.execute(
                select(ElementType.id, ElementType.name).where(
                    ElementType.name.in_(missing)
                )
            ).all()
            for r in rows:
                by_name[str(r.name)] = int(r.id)
        return by_name

    def _ensure_pcs_types_by_name(self, names: list[str]) -> dict[str, int]:
        unique = list(dict.fromkeys(names))
        if not unique:
            return {}
        rows = self.session.execute(
            select(PcsType.id, PcsType.name).where(PcsType.name.in_(unique))
        ).all()
        by_name = {str(r.name): int(r.id) for r in rows}
        missing = [n for n in unique if n not in by_name]
        for n in missing:
            self.session.add(PcsType(name=n))
        if missing:
            self.session.flush()
            rows = self.session.execute(
                select(PcsType.id, PcsType.name).where(PcsType.name.in_(missing))
            ).all()
            for r in rows:
                by_name[str(r.name)] = int(r.id)
        return by_name

    def _pg_bulk_insert_ignore(
        self, table, rows: list[dict], constraint: str,
    ) -> None:
        if not rows:
            return
        for i in range(0, len(rows), self._BULK_CHUNK):
            chunk = rows[i : i + self._BULK_CHUNK]
            stmt = pg_insert(table).values(chunk).on_conflict_do_nothing(
                constraint=constraint
            )
            self.session.execute(stmt)

    def _pg_bulk_upsert(
        self,
        table,
        rows: list[dict],
        constraint: str,
        *,
        update_columns: tuple[str, ...],
    ) -> None:
        """Insert score rows; on unique conflict refresh score columns (re-scrape safe)."""
        if not rows:
            return
        excluded = pg_insert(table).excluded
        set_clause = {col: getattr(excluded, col) for col in update_columns}
        for i in range(0, len(rows), self._BULK_CHUNK):
            chunk = rows[i : i + self._BULK_CHUNK]
            stmt = (
                pg_insert(table)
                .values(chunk)
                .on_conflict_do_update(constraint=constraint, set_=set_clause)
            )
            self.session.execute(stmt)

    def ensure_segment_officials_if_empty(
        self, competition_id: int, segment_name: str, rows: list
    ) -> bool:
        """
        If ``public.segment`` already exists for this competition and name but has no
        ``segment_official`` rows, load ``rows``. Used when ``scrape`` skips score
        processing (e.g. ``event_regex``) but panel data is available.
        """
        if not rows or not segment_name:
            return False
        segment = (
            self.session.query(Segment)
            .filter_by(competition_id=competition_id, name=segment_name)
            .first()
        )
        if segment is None:
            return False
        existing = (
            self.session.query(SegmentOfficial)
            .filter(SegmentOfficial.segment_id == segment.id)
            .count()
        )
        if existing > 0:
            return False
        self.replace_segment_officials(segment.id, rows)
        return True

    def _load_official_directory_choices(self) -> dict[int, str]:
        try:
            rows = self.session.execute(
                text("""
                    SELECT id, TRIM(full_name) AS full_name
                    FROM officials_analysis.officials
                    WHERE full_name IS NOT NULL AND TRIM(full_name) <> ''
                """)
            ).mappings().all()
        except Exception:
            return {}
        return {int(r["id"]): str(r["full_name"]) for r in rows}

    def _official_id_from_exact_directory_name(self, official_name: str) -> int | None:
        norm = _normalize_person_name(official_name)
        if not norm:
            return None
        try:
            rows = self.session.execute(
                text("""
                    SELECT id FROM officials_analysis.officials
                    WHERE full_name IS NOT NULL AND TRIM(full_name) <> ''
                      AND lower(regexp_replace(trim(full_name), '\\s+', ' ', 'g')) = :norm
                    LIMIT 2
                """),
                {"norm": norm},
            ).fetchall()
        except Exception:
            return None
        if len(rows) != 1:
            return None
        return int(rows[0][0])

    def _official_id_from_fuzzy_directory_name(
        self, official_name: str, choices: dict[int, str]
    ) -> int | None:
        if not suggest_matches or not choices:
            return None
        matches = suggest_matches(official_name, choices, top=3, min_score=88)
        if not matches:
            return None
        best_id, best_score, _ = matches[0]
        if best_score < 92:
            return None
        if len(matches) > 1 and (best_score - matches[1][1]) < 3:
            return None
        return int(best_id)

    def _official_id_from_judge_id(self, judge_id: int) -> int | None:
        try:
            row = self.session.execute(
                text(
                    "SELECT official_id FROM judge_official_link "
                    "WHERE judge_id = :jid AND status = 'linked' LIMIT 1"
                ),
                {"jid": judge_id},
            ).first()
        except Exception:
            return None
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _official_id_from_name_alias(self, official_name: str) -> int | None:
        norm = _normalize_person_name(official_name)
        if not norm:
            return None
        try:
            row = self.session.execute(
                text(
                    "SELECT official_id FROM public.official_name_alias "
                    "WHERE alias_normalized = :n LIMIT 1"
                ),
                {"n": norm},
            ).first()
        except Exception:
            return None
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _isu_official_schema_ready(self) -> bool:
        """True when migration 013 objects exist (``officials_analysis.isu_official``)."""
        if self._isu_official_schema_cache is not None:
            return self._isu_official_schema_cache
        try:
            row = self.session.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'officials_analysis'
                      AND table_name = 'isu_official'
                    LIMIT 1
                    """
                )
            ).first()
            ready = row is not None
        except Exception:
            ready = False
        self._isu_official_schema_cache = ready
        return ready

    def _load_isu_official_choices(self) -> dict[int, str]:
        try:
            rows = self.session.execute(
                text("""
                    SELECT id, TRIM(full_name) AS full_name
                    FROM officials_analysis.isu_official
                    WHERE full_name IS NOT NULL AND TRIM(full_name) <> ''
                """)
            ).mappings().all()
        except Exception:
            return {}
        return {int(r["id"]): str(r["full_name"]) for r in rows}

    def _isu_official_id_from_judge_id(self, judge_id: int) -> int | None:
        try:
            row = self.session.execute(
                text(
                    "SELECT isu_official_id FROM judge_isu_official_link "
                    "WHERE judge_id = :jid LIMIT 1"
                ),
                {"jid": judge_id},
            ).first()
        except Exception:
            return None
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _isu_official_id_from_name_alias(self, official_name: str) -> int | None:
        norm = _normalize_person_name(official_name)
        if not norm:
            return None
        try:
            row = self.session.execute(
                text(
                    "SELECT isu_official_id FROM public.isu_official_name_alias "
                    "WHERE alias_normalized = :n LIMIT 1"
                ),
                {"n": norm},
            ).first()
        except Exception:
            return None
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _isu_official_id_from_exact_name(self, official_name: str) -> int | None:
        norm = _normalize_person_name(official_name)
        if not norm:
            return None
        try:
            rows = self.session.execute(
                text("""
                    SELECT id FROM officials_analysis.isu_official
                    WHERE name_normalized = :norm
                       OR lower(regexp_replace(trim(full_name), '\\s+', ' ', 'g')) = :norm
                    LIMIT 2
                """),
                {"norm": norm},
            ).fetchall()
        except Exception:
            return None
        if len(rows) != 1:
            return None
        return int(rows[0][0])

    def _isu_official_id_from_fuzzy_name(
        self, official_name: str, choices: dict[int, str]
    ) -> int | None:
        return self._official_id_from_fuzzy_directory_name(official_name, choices)

    def getCompetitionUrlsWithNoLocation(self):
        competitions = (self.session.query(Competition).filter(Competition.location == None).all())
        return [competition.results_url for competition in competitions]
    
    def _competition_by_results_url(self, url: str) -> Competition | None:
        from ijs_results_urls import results_url_for_storage, results_url_lookup_keys

        canonical = results_url_for_storage(url)
        for key in results_url_lookup_keys(url):
            existing = (
                self.session.query(Competition).filter_by(results_url=key).first()
            )
            if existing:
                if existing.results_url != canonical:
                    existing.results_url = canonical
                    self._persist()
                return existing
        return None

    def updateCompetition(
        self,
        url,
        location,
        start_date,
        end_date,
        *,
        name=None,
        qualifying=None,
        nqs=None,
        international=None,
        officials_analysis_competition_type_id=None,
        update_officials_competition_type=False,
    ):
        existing = self._competition_by_results_url(url)
        if not existing:
            raise ValueError(
                f"updateCompetition: no competition with results_url={url!r}"
            )
        if name is not None:
            existing.name = name
        existing.location = coerce_competition_location(location)
        existing.start_date = coerce_competition_date(start_date)
        existing.end_date = coerce_competition_date(end_date)
        if qualifying is not None:
            existing.qualifying = qualifying
        if nqs is not None:
            existing.nqs = nqs
        if international is not None:
            existing.international = international
        if update_officials_competition_type:
            existing.officials_analysis_competition_type_id = (
                officials_analysis_competition_type_id
            )
        if international is not None:
            existing.international = international
        self._persist()

    def refresh_competition_discipline_flags(self, competition_id: int) -> None:
        """
        Set ``competition.singles`` / ``pairs`` / ``dance`` / ``synchronized`` from distinct
        segment discipline types for this competition (no ``commit``).
        """
        rows = (
            self.session.query(DisciplineType.name)
            .join(Segment, Segment.discipline_type_id == DisciplineType.id)
            .filter(
                Segment.competition_id == competition_id,
                Segment.discipline_type_id.isnot(None),
            )
            .distinct()
            .all()
        )
        singles = pairs = dance = synchronized = False
        for (name,) in rows:
            s, p, d, sy = _competition_flags_from_discipline_type_name(name)
            singles |= s
            pairs |= p
            dance |= d
            synchronized |= sy
        comp = self.session.query(Competition).filter_by(id=competition_id).first()
        if comp:
            comp.singles = singles
            comp.pairs = pairs
            comp.dance = dance
            comp.synchronized = synchronized

    def refresh_all_competition_discipline_flags(self) -> int:
        """Recompute discipline flags for every competition; ``commit`` once. Returns row count."""
        ids = [int(r[0]) for r in self.session.query(Competition.id).all()]
        for cid in ids:
            self.refresh_competition_discipline_flags(cid)
        self.session.commit()
        return len(ids)
    
    def getSegmentNamesForCompetition(self, url):
        competition = self._competition_by_results_url(url)
        if not competition:
            return []
        segments = (
            self.session.query(Segment)
            .filter(Segment.competition_id == competition.id)
            .all()
        )
        return [segment.name for segment in segments]

    def insert_competition(
        self,
        name,
        url,
        year,
        qualifying=None,
        nqs=None,
        international=None,
        officials_analysis_competition_type_id=None,
    ):
        from ijs_results_urls import results_url_for_storage

        canonical_url = results_url_for_storage(url)
        existing = self._competition_by_results_url(url)
        if not existing:
            q_flag = False if qualifying is None else qualifying
            n_flag = False if nqs is None else nqs
            i_flag = False if international is None else international
            new = Competition(
                name=name,
                results_url=canonical_url,
                year=year,
                qualifying=q_flag,
                nqs=n_flag,
                international=i_flag,
                officials_analysis_competition_type_id=officials_analysis_competition_type_id,
            )
            self.session.add(new)
            self._persist()
            return new.id
        return existing.id
        
    def insert_discipline_type(self, segment_name):
        type = "Uncategorized"
        if "women" in segment_name.lower() or "girl" in segment_name.lower() or "ladies" in segment_name.lower():
            type="Singles"
        elif "men" in segment_name.lower() or "boy" in segment_name.lower() or "excel" in segment_name.lower():
            type = "Singles"
        elif "singles" in segment_name.lower():
            type = "Singles"
        elif "solo" in segment_name.lower() or "shadow" in segment_name.lower():
            type = "Solo Dance"
        elif "pair" in segment_name.lower():
            type = "Pairs"
        elif "dance" in segment_name.lower():
            type = "Ice Dance"
        elif "showcase" in segment_name.lower():
            type = "Showcase"
        elif "team" in segment_name.lower():
            type="Synchronized"
        elif "synchronized" in segment_name.lower():
            type="Synchronized"
        elif "unified" in segment_name.lower():
            type="Synchronized"
        elif "elite" in segment_name.lower():
            type="Synchronized"
        elif "choreographic" in segment_name.lower():
            type="Theatre On Ice"
        elif "theatre" in segment_name.lower():
            type="Theatre On Ice"
        elif "spin" in segment_name.lower():
            type="Athlete Development"
        elif "jump" in segment_name.lower():
            type="Athlete Development"
        elif "compulsory" in segment_name.lower():
            type="Athlete Development"

        existing = self.session.query(DisciplineType).filter_by(name=type).first()
        if not existing:
            new = DisciplineType(name=type)
            self.session.add(new)
            self._flush()
            return new.id
        return existing.id

    def insert_segment(self, segment_name, competition_id):
        from segment_level import classify_segment_level_for_row

        is_freeskate = "free" in segment_name.lower()
        discipline_type_id = self.insert_discipline_type(segment_name)
        comp = self.session.get(Competition, competition_id)
        level_result = classify_segment_level_for_row(
            segment_name,
            (comp.name if comp else "") or "",
            comp.international if comp is not None else False,
            discipline_type_id=discipline_type_id,
        )
        existing = self.session.query(Segment).filter_by(name=segment_name, competition_id=competition_id).first()
        if not existing:
            new = Segment(
                name=segment_name,
                competition_id=competition_id,
                freeskate=is_freeskate,
                discipline_type_id=discipline_type_id,
                level=level_result.level,
                level_source=level_result.source,
            )
            self.session.add(new)
            self._flush()
            self.refresh_competition_discipline_flags(competition_id)
            self._persist()
            return new.id
        existing.discipline_type_id = discipline_type_id
        existing.freeskate = is_freeskate
        existing.level = level_result.level
        existing.level_source = level_result.source
        self._flush()
        self.refresh_competition_discipline_flags(competition_id)
        self._persist()
        return existing.id

    def get_segment_id(self, segment_name: str, competition_id: int) -> int | None:
        row = (
            self.session.query(Segment)
            .filter_by(name=segment_name, competition_id=competition_id)
            .first()
        )
        return int(row.id) if row else None


    def _judge_id_for_match_key(self, match_key: str) -> int | None:
        if not match_key:
            return None
        row = self.session.execute(
            text(
                """
                SELECT id FROM judge
                WHERE lower(regexp_replace(trim(name), '\\s+', ' ', 'g')) = :k
                ORDER BY id
                LIMIT 1
                """
            ),
            {"k": match_key},
        ).first()
        if not row:
            return None
        return int(row[0])

    def insert_judge(self, judge_name):
        judge_name = normalize_scraped_judge_name(judge_name)
        if not judge_name:
            raise ValueError("insert_judge: empty name after normalization")
        match_key = _normalize_person_name(judge_name)
        existing_id = self._judge_id_for_match_key(match_key)
        if existing_id is not None:
            return existing_id
        new_judge = Judge(name=judge_name)
        self.session.add(new_judge)
        self._flush()
        return int(new_judge.id)

    def insert_skater(self, skater_name):
        existing = self.session.query(Skater).filter_by(name=skater_name).first()
        if not existing:
            new = Skater(name=skater_name)
            self.session.add(new)
            self._flush()
            return new.id
        return existing.id


    def insert_skater_segment(self, segment_id, skater_id):
        existing = self.session.query(SkaterSegment).filter_by(segment_id=segment_id, skater_id=skater_id).first()
        if not existing:
            new = SkaterSegment(segment_id=segment_id, skater_id=skater_id)
            self.session.add(new)
            self._flush()
            return new.id
        return existing.id

    def insert_element_type(self, element_name):
        existing = self.session.query(ElementType).filter_by(name=element_name).first()
        if not existing:
            new = ElementType(name=element_name)
            self.session.add(new)
            self._flush()
            return new.id
        return existing.id
        
    def insert_pcs_type(self, pcs_name):
        existing = self.session.query(PcsType).filter_by(name=pcs_name).first()
        if not existing:
            new = PcsType(name=pcs_name)
            self.session.add(new)
            self._flush()
            return new.id
        return existing.id

    def insert_element(self,element_name, element_type, skater_segment_id):
        element_type_id= self.insert_element_type(element_type)
        existing = self.session.query(Element).filter_by(name=element_name, skater_segment_id=skater_segment_id).first()
        if not existing:
            new = Element(name=element_name, element_type_id = element_type_id, element_type=element_type, skater_segment_id=skater_segment_id)
            self.session.add(new)
            self._flush()
            return new.id
        else:
            existing.element_type_id = element_type_id
            existing.element_type = element_type
            self._flush()
        return existing.id

    def insert_element_score_per_judge(self, element_id, judge_id, score, panel_average, deviation, thrown_out):
        existing = self.session.query(ElementScorePerJudge).filter_by(element_id=element_id, judge_id=judge_id).first()
        if not existing:
            new = ElementScorePerJudge(
                judge_id=judge_id,
                element_id=element_id,
                judge_score=score,
                panel_average=panel_average,
                deviation=deviation,
                thrown_out=thrown_out
            )
            self.session.add(new)
            self._flush()
            return new.id
        return existing.id
        
    def insert_pcs_score_per_judge(self,skater_segment_id, pcs_type_id, judge_id, score, panel_average, deviation, thrown_out):
        existing = self.session.query(PcsScorePerJudge).filter_by(skater_segment_id=skater_segment_id, judge_id=judge_id, pcs_type_id=pcs_type_id).first()
        if not existing:
            new = PcsScorePerJudge(
                judge_id=judge_id,
                pcs_type_id = pcs_type_id,
                skater_segment_id = skater_segment_id,
                judge_score=score,
                panel_average=panel_average,
                deviation=deviation,
                thrown_out=thrown_out
            )
            self.session.add(new)
            self._flush()
            return new.id
        elif not self._numeric_eq(existing.judge_score, score):
            existing.judge_score = score
            existing.panel_average = panel_average
            existing.deviation = deviation
            existing.thrown_out = thrown_out
            self._flush()
        return existing.id

    def insert_element_scores(self, judgesNames, all_element_dict, segment_id, rule_errors):
        df = self._dataframe_element_scores(all_element_dict)
        if df.empty:
            return

        judge_dict = self._ensure_judges_by_name(
            self._merge_judge_names_from_scores(judgesNames, df)
        )
        skater_names = df["Skater"].astype(str).unique().tolist()
        skater_dict = self._ensure_skaters_by_name(skater_names)
        skater_ids = [skater_dict[n] for n in skater_names]
        ss_map = self._ensure_skater_segments_map(segment_id, skater_ids)

        type_names = df["Element Type"].astype(str).unique().tolist()
        element_types_map = self._ensure_element_types_by_name(type_names)

        element_specs: dict[tuple[int, str], tuple[str, str | None, decimal.Decimal | None]] = {}
        has_notes_col = "Notes" in df.columns
        has_max_goe_col = "Max GOE Allowed" in df.columns
        for _, r in df.iterrows():
            sid = skater_dict[str(r["Skater"])]
            ssid = ss_map[sid]
            notes_val = None
            if has_notes_col:
                raw_notes = r.get("Notes")
                if raw_notes is not None and pd.notna(raw_notes):
                    text_notes = str(raw_notes).strip()
                    notes_val = text_notes or None
            max_goe_val = None
            if has_max_goe_col:
                raw_max = r.get("Max GOE Allowed")
                if raw_max is not None and pd.notna(raw_max):
                    max_goe_val = self._to_decimal(raw_max)
            key = (ssid, str(r["Element"]))
            if key not in element_specs:
                element_specs[key] = (str(r["Element Type"]), notes_val, max_goe_val)
            else:
                etype, prev_notes, prev_max = element_specs[key]
                if notes_val and not prev_notes:
                    prev_notes = notes_val
                if max_goe_val is not None and prev_max is None:
                    prev_max = max_goe_val
                element_specs[key] = (etype, prev_notes, prev_max)

        pairs = list(element_specs.keys())
        elem_id_by_pair: dict[tuple[int, str], int] = {}
        step = 500
        for i in range(0, len(pairs), step):
            chunk = pairs[i : i + step]
            elems = self.session.execute(
                select(Element).where(
                    tuple_(Element.skater_segment_id, Element.name).in_(chunk)
                )
            ).scalars().all()
            for el in elems:
                k = (int(el.skater_segment_id), str(el.name))
                elem_id_by_pair[k] = int(el.id)
                etype_name, notes_val, max_goe_val = element_specs[k]
                etid = element_types_map[etype_name]
                cur_etid = el.element_type_id
                if el.element_type != etype_name or (
                    cur_etid is None or int(cur_etid) != etid
                ):
                    el.element_type = etype_name
                    el.element_type_id = etid
                if notes_val is not None and el.notes != notes_val:
                    el.notes = notes_val
                if max_goe_val is not None and not self._numeric_eq(
                    el.max_goe_allowed, max_goe_val
                ):
                    el.max_goe_allowed = max_goe_val

        to_add: list[Element] = []
        for (ssid, ename), (etype_name, notes_val, max_goe_val) in element_specs.items():
            if (ssid, ename) in elem_id_by_pair:
                continue
            etid = element_types_map[etype_name]
            to_add.append(
                Element(
                    name=ename,
                    element_type_id=etid,
                    element_type=etype_name,
                    skater_segment_id=ssid,
                    notes=notes_val,
                    max_goe_allowed=max_goe_val,
                )
            )
        if to_add:
            self.session.add_all(to_add)
            self.session.flush()
            for el in to_add:
                k = (int(el.skater_segment_id), str(el.name))
                elem_id_by_pair[k] = int(el.id)

        score_by_key: dict[tuple[int, int], dict] = {}
        for _, r in df.iterrows():
            sid = skater_dict[str(r["Skater"])]
            ssid = ss_map[sid]
            eid = elem_id_by_pair[(ssid, str(r["Element"]))]
            jid = judge_dict[normalize_scraped_judge_name(str(r["Judge Name"]))]
            key = (eid, jid)
            score_by_key[key] = {
                "element_id": eid,
                "judge_id": jid,
                "judge_score": self._to_decimal(r["Score"]),
                "panel_average": self._to_decimal(r["Panel Average"]),
                "deviation": self._to_decimal(r["Deviation"]),
                "thrown_out": bool(r["Thrown out"]),
                "is_rule_error": False,
            }
        score_rows = list(score_by_key.values())
        self._pg_bulk_upsert(
            ElementScorePerJudge,
            score_rows,
            "element_score_per_judge_unique",
            update_columns=(
                "judge_score",
                "panel_average",
                "deviation",
                "thrown_out",
            ),
        )

        if self._should_apply_rule_errors_for_segment(segment_id):
            self._apply_rule_errors_bulk(
                rule_errors,
                skater_dict,
                ss_map,
                elem_id_by_pair,
                judge_dict,
            )

    def _competition_dates_for_segment(
        self, segment_id: int
    ) -> tuple[datetime.date | None, datetime.date | None]:
        row = self.session.execute(
            select(Competition.start_date, Competition.end_date)
            .join(Segment, Segment.competition_id == Competition.id)
            .where(Segment.id == segment_id)
        ).first()
        if not row:
            return None, None
        return row[0], row[1]

    def _competition_year_for_segment(self, segment_id: int) -> str | None:
        row = self.session.execute(
            select(Competition.year)
            .join(Segment, Segment.competition_id == Competition.id)
            .where(Segment.id == segment_id)
        ).first()
        if not row or row[0] is None:
            return None
        text = str(row[0]).strip()
        return text or None

    def _should_apply_rule_errors_for_segment(self, segment_id: int) -> bool:
        start, end = self._competition_dates_for_segment(segment_id)
        return should_flag_rule_errors(start, end)

    def _should_apply_pcs_fall_rule_errors_for_segment(self, segment_id: int) -> bool:
        start, end = self._competition_dates_for_segment(segment_id)
        year = self._competition_year_for_segment(segment_id)
        return should_flag_pcs_fall_rule_errors(year, start, end)

    def _segment_discipline_supports_pcs_fall_rules(self, segment_id: int) -> bool:
        row = self.session.execute(
            select(Segment.discipline_type_id, Segment.name).where(Segment.id == segment_id)
        ).first()
        if not row:
            return False
        dt_id, segment_name = row
        if dt_id is not None and int(dt_id) in PCS_FALL_RULE_DISCIPLINE_TYPE_IDS:
            return True
        return segment_supports_pcs_fall_rule_errors(str(segment_name or ""))

    def _build_segment_element_score_maps_from_rows(
        self, rows
    ) -> tuple[dict[str, int], dict[int, int], dict[tuple[int, str], int]]:
        skater_dict: dict[str, int] = {}
        ss_map: dict[int, int] = {}
        elem_id_by_pair: dict[tuple[int, str], int] = {}
        for skater_name, skater_id, ss_id, el_name, el_id in rows:
            skater_dict[str(skater_name)] = int(skater_id)
            ss_map[int(skater_id)] = int(ss_id)
            elem_id_by_pair[(int(ss_id), str(el_name))] = int(el_id)
        return skater_dict, ss_map, elem_id_by_pair

    def _segment_element_score_maps(
        self, segment_id: int
    ) -> tuple[dict[str, int], dict[int, int], dict[tuple[int, str], int]]:
        """Skater name → id, skater id → skater_segment id, (ss_id, element name) → element id."""
        rows = self.session.execute(
            select(
                Skater.name,
                Skater.id,
                SkaterSegment.id,
                Element.name,
                Element.id,
            )
            .join(SkaterSegment, SkaterSegment.skater_id == Skater.id)
            .join(Element, Element.skater_segment_id == SkaterSegment.id)
            .where(SkaterSegment.segment_id == segment_id)
        ).all()
        return self._build_segment_element_score_maps_from_rows(rows)

    def segment_element_score_maps_for_competition(
        self, competition_id: int
    ) -> dict[int, tuple[dict[str, int], dict[int, int], dict[tuple[int, str], int]]]:
        """Preload score maps for every segment in a competition (one query)."""
        rows = self.session.execute(
            select(
                SkaterSegment.segment_id,
                Skater.name,
                Skater.id,
                SkaterSegment.id,
                Element.name,
                Element.id,
            )
            .join(SkaterSegment, SkaterSegment.skater_id == Skater.id)
            .join(Element, Element.skater_segment_id == SkaterSegment.id)
            .join(Segment, Segment.id == SkaterSegment.segment_id)
            .where(Segment.competition_id == competition_id)
        ).all()
        by_segment: dict[int, list] = {}
        for segment_id, skater_name, skater_id, ss_id, el_name, el_id in rows:
            by_segment.setdefault(int(segment_id), []).append(
                (skater_name, skater_id, ss_id, el_name, el_id)
            )
        return {
            sid: self._build_segment_element_score_maps_from_rows(seg_rows)
            for sid, seg_rows in by_segment.items()
        }

    def _reset_element_rule_errors_for_segment(self, segment_id: int) -> None:
        element_ids = self.session.execute(
            select(Element.id)
            .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
            .where(SkaterSegment.segment_id == segment_id)
        ).scalars().all()
        if not element_ids:
            return
        step = 500
        for i in range(0, len(element_ids), step):
            chunk = element_ids[i : i + step]
            self.session.execute(
                update(ElementScorePerJudge)
                .where(ElementScorePerJudge.element_id.in_(chunk))
                .values(is_rule_error=False)
            )

    def _bulk_update_element_metadata(self, updates: list[dict]) -> int:
        """Bulk ORM update by element ``id`` (notes / max_goe_allowed)."""
        if not updates:
            return 0
        step = 500
        for i in range(0, len(updates), step):
            chunk = updates[i : i + step]
            self.session.execute(update(Element), chunk)
        return len(updates)

    def update_element_protocol_metadata_for_segment(
        self,
        segment_id: int,
        element_metadata: dict[tuple[str, str], dict],
        *,
        score_maps: (
            tuple[dict[str, int], dict[int, int], dict[tuple[int, str], int]] | None
        ) = None,
    ) -> int:
        """
        Set ``element.notes`` and ``element.max_goe_allowed`` from parsed protocol data.

        ``element_metadata`` values are dicts with optional ``notes`` and
        ``max_goe_allowed`` keys. Returns element rows updated.
        """
        if not element_metadata:
            return 0
        if score_maps is None:
            skater_dict, ss_map, elem_id_by_pair = self._segment_element_score_maps(
                segment_id
            )
        else:
            skater_dict, ss_map, elem_id_by_pair = score_maps
        resolved: list[tuple[int, dict]] = []
        for (skater_name, element_name), meta in element_metadata.items():
            db_skater = _resolve_skater_dict_key(skater_dict, str(skater_name))
            if db_skater is None:
                continue
            skater_id = skater_dict[db_skater]
            ss_id = ss_map.get(skater_id)
            if ss_id is None:
                continue
            eid = elem_id_by_pair.get((ss_id, str(element_name)))
            if eid is None:
                continue
            resolved.append((int(eid), meta))
        if not resolved:
            return 0
        element_ids = [eid for eid, _ in resolved]
        current_rows = self.session.execute(
            select(Element.id, Element.notes, Element.max_goe_allowed).where(
                Element.id.in_(element_ids)
            )
        ).all()
        current = {
            int(row.id): (row.notes, row.max_goe_allowed) for row in current_rows
        }
        updates: list[dict] = []
        for eid, meta in resolved:
            cur_notes, cur_max = current.get(eid, (None, None))
            raw_notes = meta.get("notes")
            normalized = (
                (str(raw_notes).strip() if raw_notes is not None else None) or None
            )
            raw_max = meta.get("max_goe_allowed")
            max_dec = (
                self._to_decimal(raw_max) if raw_max is not None else None
            )
            new_max = cur_max
            if max_dec is not None and (
                cur_max is None or not self._numeric_eq(cur_max, max_dec)
            ):
                new_max = max_dec
            if normalized == cur_notes and new_max == cur_max:
                continue
            updates.append({
                "id": eid,
                "notes": normalized,
                "max_goe_allowed": new_max,
            })
        if not updates:
            return 0
        updated = self._bulk_update_element_metadata(updates)
        self._maybe_flush()
        return updated

    def update_element_notes_for_segment(
        self,
        segment_id: int,
        element_notes: dict[tuple[str, str], str | None],
    ) -> int:
        """Backward-compatible wrapper around ``update_element_protocol_metadata_for_segment``."""
        metadata = {
            key: {"notes": notes_val}
            for key, notes_val in element_notes.items()
        }
        return self.update_element_protocol_metadata_for_segment(segment_id, metadata)

    def _resolve_rule_error_pairs(
        self,
        rule_errors: list,
        skater_dict: dict[str, int],
        ss_map: dict[int, int],
        elem_id_by_pair: dict[tuple[int, str], int],
        judge_dict: dict[str, int],
    ) -> tuple[list[tuple[int, int]], list[dict]]:
        pairs: list[tuple[int, int]] = []
        unresolved: list[dict] = []
        for rule_error in rule_errors:
            skater_label = str(rule_error.get("Skater", ""))
            element_label = str(rule_error.get("Element", ""))
            judge_label = str(rule_error.get("Judge Name", ""))
            base = {
                "skater": skater_label,
                "element": element_label,
                "judge": judge_label,
            }
            db_skater = _resolve_skater_dict_key(skater_dict, skater_label)
            if db_skater is None:
                unresolved.append({**base, "reason": "skater not in segment"})
                continue
            skater_id = skater_dict[db_skater]
            ssid = ss_map.get(skater_id)
            if ssid is None:
                unresolved.append({**base, "reason": "skater not in segment"})
                continue
            db_element_name = _resolve_element_dict_key(
                elem_id_by_pair, ssid, element_label
            )
            if db_element_name is None:
                eid = None
            else:
                eid = elem_id_by_pair.get((ssid, db_element_name))
            if eid is None:
                unresolved.append({**base, "reason": "element not in segment"})
                continue
            jid = judge_dict.get(normalize_scraped_judge_name(judge_label))
            if jid is None:
                unresolved.append({**base, "reason": "judge not in segment"})
                continue
            pairs.append((eid, jid))
        uniq = list(dict.fromkeys(pairs))
        return uniq, unresolved

    def preview_rule_errors_for_segment(
        self,
        segment_id: int,
        rule_errors: list,
        *,
        panel_judge_names: list[str] | None = None,
        score_maps: (
            tuple[dict[str, int], dict[int, int], dict[tuple[int, str], int]] | None
        ) = None,
    ) -> dict:
        """Resolve parsed rule errors without writing; for dry-run reporting."""
        if not rule_errors:
            return {"flagged": 0, "unresolved": []}
        if score_maps is None:
            skater_dict, ss_map, elem_id_by_pair = self._segment_element_score_maps(
                segment_id
            )
        else:
            skater_dict, ss_map, elem_id_by_pair = score_maps
        judge_dict = self._judge_dict_for_rule_errors(
            rule_errors, panel_judge_names or []
        )
        uniq, unresolved = self._resolve_rule_error_pairs(
            rule_errors, skater_dict, ss_map, elem_id_by_pair, judge_dict
        )
        return {"flagged": len(uniq), "unresolved": unresolved}

    def _judge_dict_for_rule_errors(
        self,
        rule_errors: list,
        panel_judge_names: list[str],
    ) -> dict[str, int]:
        judge_names = list(
            dict.fromkeys(
                normalize_scraped_judge_name(str(n))
                for n in panel_judge_names
                if str(n).strip()
            )
        )
        for err in rule_errors:
            jn = normalize_scraped_judge_name(str(err.get("Judge Name", "")))
            if jn and jn not in judge_names:
                judge_names.append(jn)
        return self._ensure_judges_by_name(judge_names)

    def refresh_element_rule_errors(
        self,
        segment_id: int,
        rule_errors: list,
        *,
        panel_judge_names: list[str] | None = None,
        score_maps: (
            tuple[dict[str, int], dict[int, int], dict[tuple[int, str], int]] | None
        ) = None,
        apply_rule_errors: bool | None = None,
    ) -> dict:
        """
        Re-apply element rule errors on an existing segment (backfill / re-parse path).

        Clears ``is_rule_error`` for all element scores in the segment. When the
        competition is before 2018-07-01, leaves all flags cleared. Otherwise flags
        matches from ``rule_errors``.

        Returns ``{"flagged": int, "unresolved": list[dict]}`` where each unresolved
        entry has ``skater``, ``element``, ``judge``, and ``reason``.
        """
        self._reset_element_rule_errors_for_segment(segment_id)
        if apply_rule_errors is None:
            apply_rule_errors = self._should_apply_rule_errors_for_segment(segment_id)
        if not apply_rule_errors:
            self._maybe_flush()
            return {"flagged": 0, "unresolved": []}
        if not rule_errors:
            self._maybe_flush()
            return {"flagged": 0, "unresolved": []}
        if score_maps is None:
            skater_dict, ss_map, elem_id_by_pair = self._segment_element_score_maps(
                segment_id
            )
        else:
            skater_dict, ss_map, elem_id_by_pair = score_maps
        judge_dict = self._judge_dict_for_rule_errors(
            rule_errors, panel_judge_names or []
        )
        flagged = self._apply_rule_errors_bulk(
            rule_errors, skater_dict, ss_map, elem_id_by_pair, judge_dict
        )
        self._maybe_flush()
        _, unresolved = self._resolve_rule_error_pairs(
            rule_errors, skater_dict, ss_map, elem_id_by_pair, judge_dict
        )
        return {"flagged": flagged, "unresolved": unresolved}

    def _apply_rule_errors_bulk(
        self,
        rule_errors: list,
        skater_dict: dict[str, int],
        ss_map: dict[int, int],
        elem_id_by_pair: dict[tuple[int, str], int],
        judge_dict: dict[str, int],
    ) -> int:
        if not rule_errors:
            return 0
        uniq, _unresolved = self._resolve_rule_error_pairs(
            rule_errors, skater_dict, ss_map, elem_id_by_pair, judge_dict
        )
        step = 500
        for i in range(0, len(uniq), step):
            chunk = uniq[i : i + step]
            self.session.execute(
                update(ElementScorePerJudge)
                .where(
                    tuple_(
                        ElementScorePerJudge.element_id,
                        ElementScorePerJudge.judge_id,
                    ).in_(chunk)
                )
                .values(is_rule_error=True)
            )
        return len(uniq)

    def _reset_pcs_rule_errors_for_segment(self, segment_id: int) -> None:
        ss_ids = [
            int(r[0])
            for r in self.session.execute(
                select(SkaterSegment.id).where(SkaterSegment.segment_id == segment_id)
            ).all()
        ]
        if not ss_ids:
            return
        step = 500
        for i in range(0, len(ss_ids), step):
            chunk = ss_ids[i : i + step]
            self.session.execute(
                update(PcsScorePerJudge)
                .where(PcsScorePerJudge.skater_segment_id.in_(chunk))
                .values(is_rule_error=False)
            )

    def _segment_name_for_rule_errors(self, segment_id: int) -> str:
        row = self.session.execute(
            select(Segment.name).where(Segment.id == segment_id)
        ).first()
        return str(row[0]) if row else ""

    def _program_fall_counts_for_segment(
        self, segment_id: int
    ) -> dict[int, int]:
        rows = self.session.execute(
            select(
                SkaterSegment.id,
                Element.notes,
            )
            .join(Element, Element.skater_segment_id == SkaterSegment.id)
            .where(SkaterSegment.segment_id == segment_id)
        ).all()
        by_ss: dict[int, list[dict]] = {}
        for ss_id, notes in rows:
            sid = int(ss_id)
            by_ss.setdefault(sid, []).append({"notes": notes})
        return {
            ss_id: program_fall_count_from_elements(elements)
            for ss_id, elements in by_ss.items()
        }

    def refresh_pcs_fall_rule_errors_from_db(
        self,
        segment_id: int,
        *,
        apply_rule_errors: bool | None = None,
    ) -> dict:
        """
        Re-apply PCS fall caps from ``element.notes`` and judge PCS scores in DB.

        Returns ``{"flagged": int}``.
        """
        if apply_rule_errors is None:
            apply_rule_errors = self._should_apply_pcs_fall_rule_errors_for_segment(
                segment_id
            )
        if not apply_rule_errors or not self._segment_discipline_supports_pcs_fall_rules(
            segment_id
        ):
            self._reset_pcs_rule_errors_for_segment(segment_id)
            self._maybe_flush()
            return {"flagged": 0}

        self._reset_pcs_rule_errors_for_segment(segment_id)

        fall_counts = self._program_fall_counts_for_segment(segment_id)
        ss_to_max: dict[int, decimal.Decimal] = {}
        for ss_id, fc in fall_counts.items():
            max_pcs = max_pcs_for_fall_count(fc)
            if max_pcs is not None:
                ss_to_max[ss_id] = max_pcs

        ss_ids = list(ss_to_max.keys())
        flagged_keys: list[tuple[int, int, int]] = []
        step = 500
        for i in range(0, len(ss_ids), step):
            chunk = ss_ids[i : i + step]
            rows = self.session.execute(
                select(
                    PcsScorePerJudge.skater_segment_id,
                    PcsScorePerJudge.pcs_type_id,
                    PcsScorePerJudge.judge_id,
                    PcsScorePerJudge.judge_score,
                ).where(PcsScorePerJudge.skater_segment_id.in_(chunk))
            ).all()
            for ss_id, pcs_type_id, judge_id, judge_score in rows:
                max_pcs = ss_to_max.get(int(ss_id))
                if max_pcs is None:
                    continue
                if pcs_score_exceeds_fall_limit(judge_score, max_pcs):
                    flagged_keys.append(
                        (int(ss_id), int(pcs_type_id), int(judge_id))
                    )

        for i in range(0, len(flagged_keys), step):
            chunk = flagged_keys[i : i + step]
            self.session.execute(
                update(PcsScorePerJudge)
                .where(
                    tuple_(
                        PcsScorePerJudge.skater_segment_id,
                        PcsScorePerJudge.pcs_type_id,
                        PcsScorePerJudge.judge_id,
                    ).in_(chunk)
                )
                .values(is_rule_error=True)
            )
        self._maybe_flush()
        return {"flagged": len(flagged_keys)}

    def _count_pcs_fall_rule_errors_for_segment(self, segment_id: int) -> int:
        if not self._should_apply_pcs_fall_rule_errors_for_segment(segment_id):
            return 0
        if not self._segment_discipline_supports_pcs_fall_rules(segment_id):
            return 0

        fall_counts = self._program_fall_counts_for_segment(segment_id)
        ss_to_max: dict[int, decimal.Decimal] = {}
        for ss_id, count in fall_counts.items():
            max_pcs = max_pcs_for_fall_count(count)
            if max_pcs is not None:
                ss_to_max[ss_id] = max_pcs
        if not ss_to_max:
            return 0

        ss_ids = list(ss_to_max.keys())
        flagged = 0
        step = 500
        for i in range(0, len(ss_ids), step):
            chunk = ss_ids[i : i + step]
            rows = self.session.execute(
                select(
                    PcsScorePerJudge.skater_segment_id,
                    PcsScorePerJudge.judge_score,
                ).where(PcsScorePerJudge.skater_segment_id.in_(chunk))
            ).all()
            for ss_id, judge_score in rows:
                max_pcs = ss_to_max.get(int(ss_id))
                if max_pcs is None:
                    continue
                if pcs_score_exceeds_fall_limit(judge_score, max_pcs):
                    flagged += 1
        return flagged

    def _pcs_fall_bulk_sql_params(
        self,
        *,
        min_season_year: str | None = None,
        competition_id: int | None = None,
        segment_id: int | None = None,
        season_year: str | int | None = None,
    ) -> dict:
        return {
            "discipline_ids": sorted(PCS_FALL_RULE_DISCIPLINE_TYPE_IDS),
            "min_season_year": min_season_year or MIN_PCS_FALL_RULE_ERROR_SEASON_YEAR,
            "competition_id": competition_id,
            "segment_id": segment_id,
            "season_year": str(season_year) if season_year is not None else None,
        }

    def _pcs_fall_scope_sql(self, *, eligible_only: bool) -> str:
        year_clause = (
            "AND TRIM(c.year) >= :min_season_year"
            if eligible_only
            else "AND TRIM(c.year) < :min_season_year AND TRIM(c.year) <> ''"
        )
        return f"""
            s.discipline_type_id = ANY(:discipline_ids)
            AND (:competition_id IS NULL OR c.id = :competition_id)
            AND (:segment_id IS NULL OR s.id = :segment_id)
            AND (:season_year IS NULL OR c.year = :season_year)
            {year_clause}
        """

    def count_pcs_fall_rule_errors_bulk(
        self,
        *,
        min_season_year: str | None = None,
        competition_id: int | None = None,
        segment_id: int | None = None,
        season_year: str | int | None = None,
    ) -> int:
        """Count PCS fall violations in scope (same logic as examples SQL)."""
        params = self._pcs_fall_bulk_sql_params(
            min_season_year=min_season_year,
            competition_id=competition_id,
            segment_id=segment_id,
            season_year=season_year,
        )
        scope = self._pcs_fall_scope_sql(eligible_only=True)
        row = self.session.execute(
            text(
                f"""
                WITH element_falls AS (
                    SELECT
                        e.skater_segment_id,
                        SUM(
                            CASE
                                WHEN e.notes ~* 'Fx' THEN 2
                                WHEN e.notes LIKE '%F%' THEN 1
                                ELSE 0
                            END
                        ) AS program_fall_count
                    FROM element e
                    GROUP BY e.skater_segment_id
                ),
                fall_caps AS (
                    SELECT
                        skater_segment_id,
                        CASE
                            WHEN program_fall_count = 1 THEN 9.5
                            WHEN program_fall_count >= 2 THEN 8.75
                        END AS max_pcs_allowed
                    FROM element_falls
                    WHERE program_fall_count >= 1
                )
                SELECT COUNT(*)::bigint
                FROM pcs_score_per_judge p
                JOIN fall_caps fc
                    ON fc.skater_segment_id = p.skater_segment_id
                JOIN skater_segment ss ON ss.id = p.skater_segment_id
                JOIN segment s ON s.id = ss.segment_id
                JOIN competition c ON c.id = s.competition_id
                WHERE p.judge_score > fc.max_pcs_allowed
                  AND {scope}
                """
            ),
            params,
        ).scalar()
        return int(row or 0)

    def refresh_pcs_fall_rule_errors_bulk(
        self,
        *,
        min_season_year: str | None = None,
        competition_id: int | None = None,
        segment_id: int | None = None,
        season_year: str | int | None = None,
        clear_pre_season: bool = True,
    ) -> dict:
        """
        Re-apply PCS fall flags in a few set-based SQL statements (fast backfill path).

        Returns ``{"cleared": int, "flagged": int, "pre_season_cleared": int}``.
        """
        params = self._pcs_fall_bulk_sql_params(
            min_season_year=min_season_year,
            competition_id=competition_id,
            segment_id=segment_id,
            season_year=season_year,
        )
        eligible_scope = self._pcs_fall_scope_sql(eligible_only=True)
        pre_season_cleared = 0
        if (
            clear_pre_season
            and competition_id is None
            and segment_id is None
            and season_year is None
        ):
            pre_scope = self._pcs_fall_scope_sql(eligible_only=False)
            pre_season_cleared = int(
                self.session.execute(
                    text(
                        f"""
                        UPDATE pcs_score_per_judge p
                        SET is_rule_error = false
                        FROM skater_segment ss
                        JOIN segment s ON s.id = ss.segment_id
                        JOIN competition c ON c.id = s.competition_id
                        WHERE p.skater_segment_id = ss.id
                          AND p.is_rule_error
                          AND {pre_scope}
                        """
                    ),
                    params,
                ).rowcount
                or 0
            )

        cleared = int(
            self.session.execute(
                text(
                    f"""
                    UPDATE pcs_score_per_judge p
                    SET is_rule_error = false
                    FROM skater_segment ss
                    JOIN segment s ON s.id = ss.segment_id
                    JOIN competition c ON c.id = s.competition_id
                    WHERE p.skater_segment_id = ss.id
                      AND {eligible_scope}
                    """
                ),
                params,
            ).rowcount
            or 0
        )

        flagged = int(
            self.session.execute(
                text(
                    f"""
                    WITH element_falls AS (
                        SELECT
                            e.skater_segment_id,
                            SUM(
                                CASE
                                    WHEN e.notes ~* 'Fx' THEN 2
                                    WHEN e.notes LIKE '%F%' THEN 1
                                    ELSE 0
                                END
                            ) AS program_fall_count
                        FROM element e
                        GROUP BY e.skater_segment_id
                    ),
                    fall_caps AS (
                        SELECT
                            skater_segment_id,
                            CASE
                                WHEN program_fall_count = 1 THEN 9.5
                                WHEN program_fall_count >= 2 THEN 8.75
                            END AS max_pcs_allowed
                        FROM element_falls
                        WHERE program_fall_count >= 1
                    ),
                    violations AS (
                        SELECT
                            p.skater_segment_id,
                            p.pcs_type_id,
                            p.judge_id
                        FROM pcs_score_per_judge p
                        JOIN fall_caps fc
                            ON fc.skater_segment_id = p.skater_segment_id
                        JOIN skater_segment ss ON ss.id = p.skater_segment_id
                        JOIN segment s ON s.id = ss.segment_id
                        JOIN competition c ON c.id = s.competition_id
                        WHERE p.judge_score > fc.max_pcs_allowed
                          AND {eligible_scope}
                    )
                    UPDATE pcs_score_per_judge p
                    SET is_rule_error = true
                    FROM violations v
                    WHERE p.skater_segment_id = v.skater_segment_id
                      AND p.pcs_type_id = v.pcs_type_id
                      AND p.judge_id = v.judge_id
                    """
                ),
                params,
            ).rowcount
            or 0
        )
        self._persist()
        return {
            "cleared": cleared,
            "flagged": flagged,
            "pre_season_cleared": pre_season_cleared,
        }

    def insert_rule_errors(self, rule_errors, segment_id):
        """Legacy per-row path; prefer rule errors applied in insert_element_scores."""
        if not rule_errors or not self._should_apply_rule_errors_for_segment(segment_id):
            return
        _, _ss_map, elem_id_by_pair = self._segment_element_score_maps(segment_id)
        for rule_error in rule_errors:
            skater_id = (
                self.session.query(Skater)
                .filter_by(name=rule_error["Skater"])
                .first()
                .id
            )
            skater_segment_id = (
                self.session.query(SkaterSegment)
                .filter_by(segment_id=segment_id, skater_id=skater_id)
                .first()
                .id
            )
            db_element_name = _resolve_element_dict_key(
                elem_id_by_pair, skater_segment_id, rule_error["Element"]
            )
            if db_element_name is None:
                continue
            element_id = elem_id_by_pair[(skater_segment_id, db_element_name)]
            judge_id = (
                self.session.query(Judge)
                .filter_by(name=normalize_scraped_judge_name(rule_error["Judge Name"]))
                .first()
                .id
            )
            score = (
                self.session.query(ElementScorePerJudge)
                .filter_by(element_id=element_id, judge_id=judge_id)
                .first()
            )
            score.is_rule_error = True
        self._maybe_flush()

    def insert_pcs_scores(self, judgesNames, all_pcs_dict, segment_id):
        all_pcs_df = self._dataframe_pcs(all_pcs_dict)
        judge_dict = self._ensure_judges_by_name(
            self._merge_judge_names_from_scores(judgesNames, all_pcs_df)
        )
        if all_pcs_df.empty:
            self._persist()
            return

        skater_names = all_pcs_df["Skater"].astype(str).unique().tolist()
        skater_dict = self._ensure_skaters_by_name(skater_names)
        skater_ids = [skater_dict[n] for n in skater_names]
        ss_map = self._ensure_skater_segments_map(segment_id, skater_ids)

        pcs_type_names = all_pcs_df["Component"].astype(str).unique().tolist()
        pcs_type_map = self._ensure_pcs_types_by_name(pcs_type_names)

        expected: dict[tuple[int, int, int], decimal.Decimal] = {}
        pcs_row_by_key: dict[tuple[int, int, int], dict] = {}
        for _, r in all_pcs_df.iterrows():
            sid = skater_dict[str(r["Skater"])]
            skater_segment_id = ss_map[sid]
            pcs_type_id = pcs_type_map[str(r["Component"])]
            judge_id = judge_dict[normalize_scraped_judge_name(str(r["Judge Name"]))]
            key = (skater_segment_id, pcs_type_id, judge_id)
            score_dec = self._to_decimal(r["Score"])
            expected[key] = score_dec
            pcs_row_by_key[key] = {
                "skater_segment_id": skater_segment_id,
                "pcs_type_id": pcs_type_id,
                "judge_id": judge_id,
                "judge_score": score_dec,
                "panel_average": self._to_decimal(r["Panel Average"]),
                "deviation": self._to_decimal(r["Deviation"]),
                "thrown_out": bool(r["Thrown out"]),
                "is_rule_error": False,
            }
        pcs_rows = list(pcs_row_by_key.values())
        self._pg_bulk_upsert(
            PcsScorePerJudge,
            pcs_rows,
            "pcs_score_per_judge_unique",
            update_columns=(
                "judge_score",
                "panel_average",
                "deviation",
                "thrown_out",
            ),
        )

        keys = list(expected.keys())
        step = 500
        db_scores: dict[tuple[int, int, int], object] = {}
        for i in range(0, len(keys), step):
            chunk = keys[i : i + step]
            rows = self.session.execute(
                select(
                    PcsScorePerJudge.skater_segment_id,
                    PcsScorePerJudge.pcs_type_id,
                    PcsScorePerJudge.judge_id,
                    PcsScorePerJudge.judge_score,
                ).where(
                    tuple_(
                        PcsScorePerJudge.skater_segment_id,
                        PcsScorePerJudge.pcs_type_id,
                        PcsScorePerJudge.judge_id,
                    ).in_(chunk)
                )
            ).all()
            for r in rows:
                db_scores[(int(r[0]), int(r[1]), int(r[2]))] = r[3]

        try:
            from ijs_scrape_log import note_warning
        except ImportError:
            note_warning = None  # type: ignore[assignment]

        for k, exp in expected.items():
            if k not in db_scores:
                msg = f"PCS row missing after upsert (segment={k[0]}, pcs_type={k[1]}, judge={k[2]})"
                if note_warning:
                    note_warning(msg)
                else:
                    raise NameError(msg)
                continue
            if not self._numeric_eq(db_scores[k], exp):
                self.session.execute(
                    update(PcsScorePerJudge)
                    .where(
                        PcsScorePerJudge.skater_segment_id == k[0],
                        PcsScorePerJudge.pcs_type_id == k[1],
                        PcsScorePerJudge.judge_id == k[2],
                    )
                    .values(judge_score=exp)
                )
                msg = (
                    f"PCS score corrected after mismatch (segment={k[0]}, "
                    f"pcs_type={k[1]}, judge={k[2]}): expected {exp!s}, was {db_scores[k]!s}"
                )
                if note_warning:
                    note_warning(msg)
                else:
                    raise NameError("Scores do not align")

        if self._should_apply_pcs_fall_rule_errors_for_segment(segment_id):
            self.refresh_pcs_fall_rule_errors_from_db(segment_id, apply_rule_errors=True)

        self._persist()


def fetch_duplicate_judge_groups(session: Session) -> list[dict]:
    """
    Return judge rows grouped by normalized name key where more than one row exists.

    Each group dict has ``match_key``, ``canonical_id``, and ``members`` (list of
    ``{id, name, element_scores, pcs_scores, total_scores}``).
    """
    match_key_expr = _judge_match_key_sql_expr()
    rows = session.execute(
        text(f"""
            SELECT id, name, {match_key_expr} AS match_key
            FROM judge
            ORDER BY match_key, id
        """)
    ).all()
    grouped: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        grouped.setdefault(str(row.match_key), []).append((int(row.id), str(row.name)))
    duplicate_groups = {k: v for k, v in grouped.items() if len(v) > 1}
    if not duplicate_groups:
        return []

    all_ids = [jid for members in duplicate_groups.values() for jid, _ in members]
    elem_counts: dict[int, int] = {}
    pcs_counts: dict[int, int] = {}
    step = 500
    for i in range(0, len(all_ids), step):
        chunk = all_ids[i : i + step]
        for row in session.execute(
            text("""
                SELECT judge_id, COUNT(*)::bigint AS cnt
                FROM element_score_per_judge
                WHERE judge_id IN :ids
                GROUP BY judge_id
            """).bindparams(bindparam("ids", expanding=True)),
            {"ids": chunk},
        ).all():
            elem_counts[int(row.judge_id)] = int(row.cnt)
        for row in session.execute(
            text("""
                SELECT judge_id, COUNT(*)::bigint AS cnt
                FROM pcs_score_per_judge
                WHERE judge_id IN :ids
                GROUP BY judge_id
            """).bindparams(bindparam("ids", expanding=True)),
            {"ids": chunk},
        ).all():
            pcs_counts[int(row.judge_id)] = int(row.cnt)

    total_counts = {
        jid: elem_counts.get(jid, 0) + pcs_counts.get(jid, 0) for jid in all_ids
    }
    out: list[dict] = []
    for match_key, members in sorted(duplicate_groups.items()):
        ids = [jid for jid, _ in members]
        canonical_id = select_canonical_judge_ids_per_match_key(
            {match_key: ids}, total_counts
        )[match_key]
        out.append({
            "match_key": match_key,
            "canonical_id": canonical_id,
            "members": [
                {
                    "id": jid,
                    "name": name,
                    "element_scores": elem_counts.get(jid, 0),
                    "pcs_scores": pcs_counts.get(jid, 0),
                    "total_scores": total_counts.get(jid, 0),
                    "is_canonical": jid == canonical_id,
                }
                for jid, name in members
            ],
        })
    return out


# session = get_db_session()
# database_obj = DatabaseLoader(session)
# print(database_obj.getSegmentNamesForCompetition("https://ijs.usfigureskating.org/leaderboard/results/2025/35645"))
# print(insert_competition("test", "https://ijs.usfigureskating.org/leaderboard/results/2024/33458",""))


