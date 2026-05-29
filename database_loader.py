from __future__ import annotations

import datetime
import decimal
import re
from sqlalchemy import select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import bindparam, text
import pandas as pd
from sqlalchemy.orm import Session
from models import Judge, Competition, Segment, Skater, SkaterSegment, Element, ElementScorePerJudge, PcsScorePerJudge, PcsType, ElementType, DisciplineType, SegmentOfficial
from database import get_db_session, test_connection
from rule_errors_policy import should_flag_rule_errors

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
        self.session.commit()

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

    def _ensure_judges_by_name(self, names: list[str]) -> dict[str, int]:
        displays = [normalize_scraped_judge_name(str(n)) for n in names]
        unique_displays = list(dict.fromkeys(d for d in displays if d))
        if not unique_displays:
            return {}
        keys = {d: _normalize_person_name(d) for d in unique_displays}
        unique_keys = list(dict.fromkeys(keys.values()))
        by_key: dict[str, int] = {}
        if unique_keys:
            rows = self.session.execute(
                text("""
                    SELECT id,
                           lower(regexp_replace(trim(name), '\\s+', ' ', 'g')) AS match_key
                    FROM judge
                    WHERE lower(regexp_replace(trim(name), '\\s+', ' ', 'g')) IN :keys
                """).bindparams(bindparam("keys", expanding=True)),
                {"keys": unique_keys},
            ).all()
            for row in rows:
                by_key[str(row.match_key)] = int(row.id)
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
        existing = self.session.query(Competition).filter_by(results_url=url).first()
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
        segments = (self.session.query(Segment).join(Competition, Segment.competition_id == Competition.id).filter(Competition.results_url == url).all())
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
        existing = self.session.query(Competition).filter_by(results_url=url).first()
        if not existing:
            q_flag = False if qualifying is None else qualifying
            n_flag = False if nqs is None else nqs
            i_flag = False if international is None else international
            new = Competition(
                name=name,
                results_url=url,
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
        is_freeskate = "free" in segment_name.lower()
        discipline_type_id = self.insert_discipline_type(segment_name)
        existing = self.session.query(Segment).filter_by(name=segment_name, competition_id=competition_id).first()
        if not existing:
            new = Segment(name=segment_name, competition_id=competition_id, freeskate=is_freeskate, discipline_type_id=discipline_type_id)
            self.session.add(new)
            self._flush()
            self.refresh_competition_discipline_flags(competition_id)
            self._persist()
            return new.id
        existing.discipline_type_id = discipline_type_id
        existing.freeskate = is_freeskate
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

        judge_dict = self._ensure_judges_by_name(list(judgesNames))
        skater_names = df["Skater"].astype(str).unique().tolist()
        skater_dict = self._ensure_skaters_by_name(skater_names)
        skater_ids = [skater_dict[n] for n in skater_names]
        ss_map = self._ensure_skater_segments_map(segment_id, skater_ids)

        type_names = df["Element Type"].astype(str).unique().tolist()
        element_types_map = self._ensure_element_types_by_name(type_names)

        element_specs: dict[tuple[int, str], str] = {}
        for _, r in df.iterrows():
            sid = skater_dict[str(r["Skater"])]
            ssid = ss_map[sid]
            element_specs[(ssid, str(r["Element"]))] = str(r["Element Type"])

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
                etype_name = element_specs[k]
                etid = element_types_map[etype_name]
                cur_etid = el.element_type_id
                if el.element_type != etype_name or (
                    cur_etid is None or int(cur_etid) != etid
                ):
                    el.element_type = etype_name
                    el.element_type_id = etid

        to_add: list[Element] = []
        for (ssid, ename), etype_name in element_specs.items():
            if (ssid, ename) in elem_id_by_pair:
                continue
            etid = element_types_map[etype_name]
            to_add.append(
                Element(
                    name=ename,
                    element_type_id=etid,
                    element_type=etype_name,
                    skater_segment_id=ssid,
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

    def _should_apply_rule_errors_for_segment(self, segment_id: int) -> bool:
        start, end = self._competition_dates_for_segment(segment_id)
        return should_flag_rule_errors(start, end)

    def _apply_rule_errors_bulk(
        self,
        rule_errors: list,
        skater_dict: dict[str, int],
        ss_map: dict[int, int],
        elem_id_by_pair: dict[tuple[int, str], int],
        judge_dict: dict[str, int],
    ) -> None:
        if not rule_errors:
            return
        pairs: list[tuple[int, int]] = []
        for rule_error in rule_errors:
            skater_id = skater_dict[str(rule_error["Skater"])]
            ssid = ss_map[skater_id]
            converted_name = str(rule_error["Element"]).split(" ")[0]
            eid = elem_id_by_pair[(ssid, converted_name)]
            jid = judge_dict[normalize_scraped_judge_name(str(rule_error["Judge Name"]))]
            pairs.append((eid, jid))
        uniq = list(dict.fromkeys(pairs))
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

    def insert_rule_errors(self, rule_errors, segment_id):
        """Legacy per-row path; prefer rule errors applied in insert_element_scores."""
        if not rule_errors or not self._should_apply_rule_errors_for_segment(segment_id):
            return
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
            converted_name = rule_error["Element"].split(" ")[0]
            element_id = (
                self.session.query(Element)
                .filter_by(
                    name=converted_name, skater_segment_id=skater_segment_id
                )
                .first()
                .id
            )
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
        self._flush()

    def insert_pcs_scores(self, judgesNames, all_pcs_dict, segment_id):
        judge_dict = self._ensure_judges_by_name(list(judgesNames))
        all_pcs_df = self._dataframe_pcs(all_pcs_dict)
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

        self._persist()

# session = get_db_session()
# database_obj = DatabaseLoader(session)
# print(database_obj.getSegmentNamesForCompetition("https://ijs.usfigureskating.org/leaderboard/results/2025/35645"))
# print(insert_competition("test", "https://ijs.usfigureskating.org/leaderboard/results/2024/33458",""))


