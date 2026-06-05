from datetime import date
from typing import Optional

import pandas as pd
import numpy as np
import re
import unicodedata
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, text, select, case
from scipy import stats
from scipy.stats import linregress
from collections import defaultdict
from models import (
    Judge,
    Competition,
    Segment,
    DisciplineType,
    ElementType,
    PcsScorePerJudge,
    ElementScorePerJudge,
    Element,
    SkaterSegment,
    Skater,
    PcsType,
    JudgeOfficialLink,
    JudgeIsuOfficialLink,
    IsuOfficial,
    SegmentOfficial,
    AppointmentTypes,
    Officials,
)

from judge_excess_cache import (
    aggregate_excess_from_cache,
    allowed_errors_for_skater_count,
    ensure_judge_excess_cache,
)

from officials_competition_types import (
    COMPETITION_SCOPE_ALL,
    COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY,
    COMPETITION_SCOPE_INTERNATIONAL,
    COMPETITION_SCOPE_NQS,
    COMPETITION_SCOPE_QUALIFYING,
    COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS,
    OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING,
    OFFICIALS_COMPETITION_TYPE_ID_NQS,
    OFFICIALS_COMPETITION_TYPE_IDS_CHAMPIONSHIPS_ONLY,
    OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL,
    OFFICIALS_COMPETITION_TYPE_IDS_SECTIONALS_AND_CHAMPIONSHIPS,
)


def _normalize_person_name_key(s: str) -> str:
    """Lowercase compare-key for person labels (hyphens, punctuation, Unicode quirks)."""
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", str(s))
    t = t.strip().lower()
    t = re.sub(r"[-_/.,;:'\"`]+", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def _given_name_parts_equivalent(a: str, b: str) -> bool:
    """True when given-name tokens match aside from case, initials, or common shortenings."""
    if a == b:
        return True
    if not a or not b:
        return a == b
    if len(a) == 1 or len(b) == 1:
        return a[0] == b[0]
    if a.startswith(b) or b.startswith(a):
        return True
    return False


def _person_name_tokens_equivalent(ta: list[str], tb: list[str]) -> bool:
    """Same name tokens in any order (e.g. ``Christian Baumann`` vs ``BAUMANN Christian``)."""
    if not ta or not tb:
        return ta == tb
    if sorted(ta) == sorted(tb):
        return True
    if len(ta) < 2 or len(tb) < 2:
        return False
    if ta[-1] != tb[-1]:
        return False
    ga, gb = " ".join(ta[:-1]), " ".join(tb[:-1])
    if not ga or not gb:
        return True
    return _given_name_parts_equivalent(ga, gb)


def _person_names_equivalent_for_display(a: str, b: str) -> bool:
    """
    Whether two display names refer to the same person for identity labels.

    Matches exact normalized forms, the same tokens in either order (protocol
    ``FAMILY Given`` vs directory ``Given Family``), or western-order family name
    with compatible given names (e.g. ``Christopher Buchanan`` vs ``Chris BUCHANAN``).
    """
    ka, kb = _normalize_person_name_key(a), _normalize_person_name_key(b)
    if not ka or not kb:
        return ka == kb
    if ka == kb:
        return True
    return _person_name_tokens_equivalent(ka.split(), kb.split())


def _union_find_parent(parent: dict[int, int], x: int) -> int:
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


def _union_find_union(parent: dict[int, int], a: int, b: int) -> None:
    ra, rb = _union_find_parent(parent, a), _union_find_parent(parent, b)
    if ra != rb:
        parent[rb] = ra


def _union_find_clusters(parent: dict[int, int], judge_ids: set[int]) -> list[list[int]]:
    """Connected components of size 2+ from union-find ``parent`` map."""
    buckets: dict[int, list[int]] = defaultdict(list)
    for jid in judge_ids:
        buckets[_union_find_parent(parent, jid)].append(jid)
    return [sorted(v) for v in buckets.values() if len(v) >= 2]


def _identity_label_for_merged_jids(
    jids: list[int],
    judge_map: dict[int, tuple[str, str]],
    *,
    directory_name: str,
    fallback_suffix: str,
) -> str:
    names = sorted({judge_map[j][0] for j in jids}, key=str.lower)
    by_norm: dict[str, str] = {}
    for n in names:
        if directory_name and _person_names_equivalent_for_display(n, directory_name):
            continue
        nk = _normalize_person_name_key(n)
        merged = False
        for ek in list(by_norm.keys()):
            if _person_names_equivalent_for_display(by_norm[ek], n):
                prev = by_norm.pop(ek)
                keep = n if len(n.strip()) > len(prev.strip()) else prev
                by_norm[_normalize_person_name_key(keep)] = keep
                merged = True
                break
        if merged:
            continue
        prev = by_norm.get(nk)
        if prev is None or len(n.strip()) > len(prev.strip()):
            by_norm[nk] = n
    alias_names = sorted(by_norm.values(), key=str.lower)
    if directory_name:
        if alias_names:
            return f"{directory_name} · " + " · ".join(alias_names)
        return directory_name
    if alias_names:
        return " · ".join(alias_names) + fallback_suffix
    return " · ".join(names) + fallback_suffix


def _panel_role_summary_label(role: str) -> str:
    """Map ``segment_official.role`` / appointment label to a short IJS-style tag (activity tracker)."""
    r = (role or "").strip()
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


def _panel_role_base_label(
    appointment_name: Optional[str], protocol_role: Optional[str]
) -> str:
    """
    Role text without discipline: directory appointment name when linked, else protocol role
    (with IJS-style shortening when there is no appointment name).
    """
    appt = (appointment_name or "").strip()
    proto = (protocol_role or "").strip()
    if appt:
        return appt
    if proto:
        return _panel_role_summary_label(proto) or proto
    return ""


class JudgeAnalytics:
    """Analytics over judge scores; qualifying/NQS scope uses linked officials competition types."""

    @staticmethod
    def normalize_judge_ids(judge_ids):
        """Accept a single id or iterable of ids; return a deduped ordered list."""
        if judge_ids is None:
            raise TypeError("judge_ids is required")
        if isinstance(judge_ids, int):
            return [judge_ids]
        out = []
        for x in judge_ids:
            xi = int(x)
            if xi not in out:
                out.append(xi)
        return out

    def _competition_scope_clause(self, competition_scope: str):
        """Restrict competitions by linked ``officials_analysis.competition_type`` id."""
        if not competition_scope or competition_scope == COMPETITION_SCOPE_ALL:
            return None
        if competition_scope == COMPETITION_SCOPE_QUALIFYING:
            return and_(
                Competition.officials_analysis_competition_type_id.isnot(None),
                Competition.officials_analysis_competition_type_id
                != OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING,
            )
        if competition_scope == COMPETITION_SCOPE_NQS:
            return (
                Competition.officials_analysis_competition_type_id
                == OFFICIALS_COMPETITION_TYPE_ID_NQS
            )
        if competition_scope == COMPETITION_SCOPE_SECTIONALS_AND_CHAMPIONSHIPS:
            return Competition.officials_analysis_competition_type_id.in_(
                OFFICIALS_COMPETITION_TYPE_IDS_SECTIONALS_AND_CHAMPIONSHIPS
            )
        if competition_scope == COMPETITION_SCOPE_CHAMPIONSHIPS_ONLY:
            return Competition.officials_analysis_competition_type_id.in_(
                OFFICIALS_COMPETITION_TYPE_IDS_CHAMPIONSHIPS_ONLY
            )
        if competition_scope == COMPETITION_SCOPE_INTERNATIONAL:
            return Competition.officials_analysis_competition_type_id.in_(
                OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL
            )
        return None

    def _filter_orm_competition_scope(self, query, competition_scope: str):
        clause = self._competition_scope_clause(competition_scope)
        if clause is not None:
            query = query.filter(clause)
        return query

    def _filter_select_competition_scope(self, sel, competition_scope: str):
        clause = self._competition_scope_clause(competition_scope)
        if clause is not None:
            sel = sel.filter(clause)
        return sel

    @staticmethod
    def _qualifying_core_disciplines_active(competition_scope: str) -> bool:
        """Narrow to core disciplines for every scope except *all*."""
        return bool(competition_scope and competition_scope != COMPETITION_SCOPE_ALL)

    def get_officials_analysis_competition_types(self) -> list[tuple[int, str]]:
        """Rows from ``officials_analysis.competition_type`` for linking ``public.competition``."""
        try:
            rows = self.session.execute(
                text(
                    "SELECT id, name FROM officials_analysis.competition_type "
                    "ORDER BY lower(name)"
                )
            ).fetchall()
            return [(int(r[0]), str(r[1])) for r in rows]
        except Exception:
            return []

    def get_public_competition_officials_type_breakdown(self) -> pd.DataFrame:
        """Counts of ``public.competition`` rows per linked officials competition type."""
        try:
            rows = self.session.execute(
                text(
                    """
                    SELECT ct.id AS officials_competition_type_id,
                           ct.name AS officials_competition_type_name,
                           COUNT(pc.id)::bigint AS public_competition_count
                    FROM officials_analysis.competition_type ct
                    LEFT JOIN public.competition pc
                      ON pc.officials_analysis_competition_type_id = ct.id
                    GROUP BY ct.id, ct.name
                    ORDER BY lower(ct.name)
                    """
                )
            ).fetchall()
            unlinked = self.session.execute(
                text(
                    """
                    SELECT COUNT(*)::bigint FROM public.competition
                    WHERE officials_analysis_competition_type_id IS NULL
                    """
                )
            ).scalar_one()
            cols = [
                "officials_competition_type_id",
                "officials_competition_type_name",
                "public_competition_count",
            ]
            df = pd.DataFrame(
                [
                    {
                        "officials_competition_type_id": int(r[0]),
                        "officials_competition_type_name": str(r[1]),
                        "public_competition_count": int(r[2]),
                    }
                    for r in rows
                ],
                columns=cols,
            )
            if int(unlinked or 0) > 0:
                df = pd.concat(
                    [
                        df,
                        pd.DataFrame(
                            [
                                {
                                    "officials_competition_type_id": pd.NA,
                                    "officials_competition_type_name": "(Not linked)",
                                    "public_competition_count": int(unlinked),
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
            return df
        except Exception:
            return pd.DataFrame(
                columns=[
                    "officials_competition_type_id",
                    "officials_competition_type_name",
                    "public_competition_count",
                ]
            )

    def get_judge_analysis_identity_groups(self) -> list:
        """
        Select-box rows for UI: merge protocol ``judge`` rows that share a US directory
        link (``judge_official_link``) and/or an ISU roster link (``judge_isu_official_link``).

        Each group has ``label``, ``judge_ids``, and optional ``official_id`` /
        ``isu_official_id``.
        """
        judges = (
            self.session.query(Judge.id, Judge.name, Judge.location)
            .order_by(Judge.name)
            .all()
        )
        judge_map = {r.id: (r.name, r.location or "") for r in judges}
        if not judge_map:
            return []

        parent: dict[int, int] = {jid: jid for jid in judge_map}
        us_jid_to_oid: dict[int, int] = {}
        isu_jid_to_ioid: dict[int, int] = {}

        linked_rows = (
            self.session.query(JudgeOfficialLink.judge_id, JudgeOfficialLink.official_id)
            .filter(JudgeOfficialLink.status == "linked")
            .filter(JudgeOfficialLink.official_id.isnot(None))
            .all()
        )
        by_official: dict[int, list[int]] = defaultdict(list)
        for jid, oid in linked_rows:
            jid = int(jid)
            if jid in judge_map:
                oid = int(oid)
                by_official[oid].append(jid)
                us_jid_to_oid[jid] = oid
        for jids in by_official.values():
            uniq = sorted(set(jids))
            if len(uniq) < 2:
                continue
            for other in uniq[1:]:
                _union_find_union(parent, uniq[0], other)

        try:
            isu_rows = self.session.query(
                JudgeIsuOfficialLink.judge_id, JudgeIsuOfficialLink.isu_official_id
            ).all()
        except Exception:
            isu_rows = []
        by_isu: dict[int, list[int]] = defaultdict(list)
        for jid, ioid in isu_rows:
            jid = int(jid)
            if jid in judge_map:
                ioid = int(ioid)
                by_isu[ioid].append(jid)
                isu_jid_to_ioid[jid] = ioid
        for jids in by_isu.values():
            uniq = sorted(set(jids))
            if len(uniq) < 2:
                continue
            for other in uniq[1:]:
                _union_find_union(parent, uniq[0], other)

        multi_assigned: set[int] = set()
        merged_groups: list[dict] = []
        for jids in sorted(
            _union_find_clusters(parent, set(judge_map.keys())),
            key=lambda ids: judge_map[ids[0]][0].lower(),
        ):
            multi_assigned.update(jids)
            us_oids = {us_jid_to_oid[j] for j in jids if j in us_jid_to_oid}
            isu_oids = {isu_jid_to_ioid[j] for j in jids if j in isu_jid_to_ioid}
            official_id = None
            isu_official_id = None
            directory_name = ""
            fallback_suffix = " (same linked identity)"
            if len(us_oids) == 1:
                official_id = next(iter(us_oids))
                off = self.session.get(Officials, official_id)
                directory_name = (off.full_name or "").strip() if off else ""
                fallback_suffix = " (same directory official)"
            if not directory_name and len(isu_oids) == 1:
                isu_official_id = next(iter(isu_oids))
                isu = self.session.get(IsuOfficial, isu_official_id)
                directory_name = (isu.full_name or "").strip() if isu else ""
                fallback_suffix = " (same ISU roster official)"
            label = _identity_label_for_merged_jids(
                jids,
                judge_map,
                directory_name=directory_name,
                fallback_suffix=fallback_suffix,
            )
            merged_groups.append(
                {
                    "label": label,
                    "judge_ids": jids,
                    "official_id": official_id,
                    "isu_official_id": isu_official_id,
                }
            )

        singleton_labels_seen = set()
        singleton_groups = []
        for jid in sorted(judge_map.keys(), key=lambda x: judge_map[x][0].lower()):
            if jid in multi_assigned:
                continue
            nm, loc = judge_map[jid]
            base = nm if not loc else f"{nm} ({loc})"
            if base in singleton_labels_seen:
                label = f"{base} [judge id {jid}]"
            else:
                singleton_labels_seen.add(base)
                label = base
            singleton_groups.append(
                {
                    "label": label,
                    "judge_ids": [jid],
                    "official_id": None,
                    "isu_official_id": isu_jid_to_ioid.get(jid),
                }
            )

        all_groups = merged_groups + singleton_groups
        all_groups.sort(key=lambda g: g["label"].lower())
        return all_groups

    def get_judge_id_to_identity_label(self) -> dict[int, str]:
        """Map each scoring ``judge.id`` to a display label (US / ISU linked aliases merged)."""
        out: dict[int, str] = {}
        for group in self.get_judge_analysis_identity_groups():
            label = group["label"]
            for jid in group["judge_ids"]:
                out[int(jid)] = label
        return out

    @staticmethod
    def _merge_per_judge_stat_dicts_by_identity(
        per_judge: dict[int, dict],
        judge_id_to_label: dict[int, str],
    ) -> dict[str, dict]:
        """Sum count fields and weight-average ``avg_dev`` per identity label."""
        merged: dict[str, dict] = {}
        for judge_id, stats in per_judge.items():
            label = judge_id_to_label.get(judge_id)
            if not label:
                continue
            bucket = merged.setdefault(
                label,
                {
                    "total": 0,
                    "throwouts": 0,
                    "anomalies": 0,
                    "rule_errors": 0,
                    "_avg_dev_sum": 0.0,
                    "_avg_dev_n": 0,
                },
            )
            for key in ("total", "throwouts", "anomalies", "rule_errors"):
                bucket[key] += int(stats.get(key) or 0)
            n = int(stats.get("total") or 0)
            if n and stats.get("avg_dev") is not None:
                bucket["_avg_dev_sum"] += float(stats["avg_dev"]) * n
                bucket["_avg_dev_n"] += n
        for bucket in merged.values():
            n = bucket.pop("_avg_dev_n", 0)
            dev_sum = bucket.pop("_avg_dev_sum", 0.0)
            bucket["avg_dev"] = (dev_sum / n) if n else 0.0
        return merged

    @staticmethod
    def _merge_competition_judge_stat_dicts_by_identity(
        per_comp_judge: dict[tuple[int, int], dict],
        judge_id_to_label: dict[int, str],
    ) -> dict[tuple[int, str], dict]:
        """Like ``_merge_per_judge_stat_dicts_by_identity`` but keyed by (competition_id, label)."""
        merged: dict[tuple[int, str], dict] = {}
        for (comp_id, judge_id), stats in per_comp_judge.items():
            label = judge_id_to_label.get(judge_id)
            if not label:
                continue
            key = (int(comp_id), label)
            bucket = merged.setdefault(
                key,
                {
                    "total": 0,
                    "throwouts": 0,
                    "anomalies": 0,
                    "rule_errors": 0,
                    "_avg_dev_sum": 0.0,
                    "_avg_dev_n": 0,
                },
            )
            for field in ("total", "throwouts", "anomalies", "rule_errors"):
                bucket[field] += int(stats.get(field) or 0)
            n = int(stats.get("total") or 0)
            if n and stats.get("avg_dev") is not None:
                bucket["_avg_dev_sum"] += float(stats["avg_dev"]) * n
                bucket["_avg_dev_n"] += n
        for bucket in merged.values():
            n = bucket.pop("_avg_dev_n", 0)
            dev_sum = bucket.pop("_avg_dev_sum", 0.0)
            bucket["avg_dev"] = (dev_sum / n) if n else 0.0
        return merged

    @staticmethod
    def _merge_excess_map_by_identity(
        excess_map: dict,
        judge_id_to_label: dict[int, str],
        *,
        by_competition: bool = False,
    ) -> dict:
        if by_competition:
            merged: dict[tuple[str, int], int] = defaultdict(int)
            for (judge_id, comp_id), val in excess_map.items():
                label = judge_id_to_label.get(int(judge_id))
                if label:
                    merged[(label, int(comp_id))] += int(val or 0)
            return merged
        merged_judge: dict[str, int] = defaultdict(int)
        for judge_id, val in excess_map.items():
            label = judge_id_to_label.get(int(judge_id))
            if label:
                merged_judge[label] += int(val or 0)
        return merged_judge

    def _event_dates_for_competition_ids(self, competition_ids):
        """Map competition id → event date for sorting (start_date, else end_date)."""
        if not competition_ids:
            return {}
        uniq = list({int(c) for c in competition_ids})
        rows = (
            self.session.query(
                Competition.id,
                Competition.start_date,
                Competition.end_date,
            )
            .filter(Competition.id.in_(uniq))
            .all()
        )
        out = {}
        for cid, sd, ed in rows:
            out[int(cid)] = sd or ed
        return out

    def _competition_event_date_expr(self):
        """Calendar date used for event ordering and date-range filters."""
        return func.coalesce(Competition.start_date, Competition.end_date)

    def _apply_competition_event_date_range(
        self,
        query,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """Restrict an ORM query that already joins ``Competition``."""
        if event_start_date is None and event_end_date is None:
            return query
        ev = self._competition_event_date_expr()
        query = query.filter(ev.isnot(None))
        if event_start_date is not None:
            query = query.filter(ev >= event_start_date)
        if event_end_date is not None:
            query = query.filter(ev <= event_end_date)
        return query

    def _filter_competition_rows_by_event_dates(
        self,
        rows: list,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ) -> list:
        """``rows`` are ``(competition_id, name, year)`` tuples."""
        if event_start_date is None and event_end_date is None:
            return rows
        date_map = self._event_dates_for_competition_ids([r[0] for r in rows])
        kept = []
        for row in rows:
            ev = date_map.get(int(row[0]))
            if ev is None:
                continue
            if event_start_date is not None and ev < event_start_date:
                continue
            if event_end_date is not None and ev > event_end_date:
                continue
            kept.append(row)
        return kept

    def get_competition_event_date_bounds(
        self,
        judge_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        discipline_type_ids=None,
    ) -> tuple[date, date]:
        """
        Min/max event dates (start_date, else end_date) for date pickers.

        When ``judge_ids`` is set, only competitions that judge appears in
        (scores or protocol) are considered.
        """
        if judge_ids:
            rows = self.get_judge_competitions(
                judge_ids,
                competition_scope=competition_scope,
                discipline_type_ids=discipline_type_ids,
            )
            cids = [int(r[0]) for r in rows]
        else:
            cids = [
                int(r[0])
                for r in self.session.query(Competition.id).all()
            ]
        date_map = self._event_dates_for_competition_ids(cids)
        dates = [d for d in date_map.values() if d is not None]
        if not dates:
            today = date.today()
            return today, today
        return min(dates), max(dates)

    def get_judge_years(
        self,
        judge_ids,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        discipline_type_ids=None,
    ) -> list[str]:
        """Distinct ``Competition.year`` values for this judge's competitions (newest first)."""
        rows = self.get_judge_competitions(
            judge_ids,
            competition_scope=competition_scope,
            discipline_type_ids=discipline_type_ids,
        )
        years = sorted({str(y) for _, _, y in rows}, reverse=True)
        return years

    _QUALIFYING_SCOPE_DISCIPLINE_NAMES = frozenset(
        {"singles", "pairs", "ice dance", "synchronized"}
    )

    def __init__(self, session: Session):
        self.session = session

    def _qualifying_core_discipline_type_ids(self):
        """Discipline_type ids for Singles, Pairs, Ice Dance, Synchronized (name match)."""
        out = []
        for dt_id, name in self.session.query(DisciplineType.id, DisciplineType.name).all():
            if name is None:
                continue
            key = name.strip().lower()
            if key in self._QUALIFYING_SCOPE_DISCIPLINE_NAMES:
                out.append(dt_id)
        return out

    def qualifying_event_segment_discipline_types(self):
        """(id, name) rows for disciplines counted in qualifying-only benchmarking."""
        ids = self._qualifying_core_discipline_type_ids()
        if not ids:
            return []
        rows = (
            self.session.query(DisciplineType.id, DisciplineType.name)
            .filter(DisciplineType.id.in_(ids))
            .order_by(DisciplineType.name)
            .all()
        )
        return [(r[0], r[1]) for r in rows]

    def _merged_segment_discipline_ids(self, narrow_core_disciplines, discipline_type_ids):
        """
        Segment discipline filter for queries.
        Returns None if no restriction; otherwise a list (possibly empty = no matching segments).
        """
        if narrow_core_disciplines:
            allowed = self._qualifying_core_discipline_type_ids()
            if discipline_type_ids:
                return [i for i in discipline_type_ids if i in allowed]
            return allowed
        if discipline_type_ids:
            return discipline_type_ids
        return None

    def get_judges(self):
        """Get all judges in alphabetical order"""
        judges = self.session.query(Judge).order_by(Judge.name).all()
        return [(judge.id, judge.name, judge.location) for judge in judges]

    def get_competitions(
        self,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """Competitions optionally scoped by linked ``officials_analysis`` competition type."""
        q = self.session.query(Competition).order_by(
            Competition.year.desc(), Competition.name
        )
        q = self._filter_orm_competition_scope(q, competition_scope)
        competitions = q.all()
        rows = [(comp.id, comp.name, comp.year) for comp in competitions]
        return self._filter_competition_rows_by_event_dates(
            rows, event_start_date, event_end_date
        )

    def get_judge_competitions(
        self,
        judge_ids,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        discipline_type_ids=None,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """Competitions for these judge record(s): scored segments plus protocol panel rows.

        Includes competitions where the judge has PCS/element scores. When a judge is
        ``linked`` in ``judge_official_link`` to a directory official, also includes any
        competition where that official appears on ``segment_official`` (any role), even
        if there are no scores under this judge id for that event.

        Returned rows are ordered by competition event date (``start_date``, else ``end_date``)
        descending, then season year and name; competitions missing both dates sort last.

        When ``competition_scope`` is not *all*, only competitions whose linked officials
        type matches that scope are included (same semantics as other analytics filters).

        When ``discipline_type_ids`` is set (or implied by scoped core disciplines), only
        segments in those discipline types count toward scores and protocol appearances.
        """
        ids = self.normalize_judge_ids(judge_ids)
        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(
            core_disc, discipline_type_ids
        )

        def _segment_discipline_clause(query):
            if seg_discipline_ids is None:
                return query
            return query.filter(Segment.discipline_type_id.in_(seg_discipline_ids))

        pcs_competitions = _segment_discipline_clause(
            self.session.query(Competition).join(
            Segment, Segment.competition_id == Competition.id
        ).join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            PcsScorePerJudge, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).filter(
            PcsScorePerJudge.judge_id.in_(ids)
        )).distinct()

        element_competitions = _segment_discipline_clause(
            self.session.query(Competition).join(
            Segment, Segment.competition_id == Competition.id
        ).join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            Element, Element.skater_segment_id == SkaterSegment.id
        ).join(
            ElementScorePerJudge, ElementScorePerJudge.element_id == Element.id
        ).filter(
            ElementScorePerJudge.judge_id.in_(ids)
        )).distinct()

        all_competitions = set()
        for comp in pcs_competitions:
            all_competitions.add((comp.id, comp.name, comp.year))
        for comp in element_competitions:
            all_competitions.add((comp.id, comp.name, comp.year))

        linked_official_ids = [
            oid
            for (oid,) in self.session.query(JudgeOfficialLink.official_id)
            .filter(JudgeOfficialLink.judge_id.in_(ids))
            .filter(JudgeOfficialLink.status == "linked")
            .filter(JudgeOfficialLink.official_id.isnot(None))
            .distinct()
            .all()
            if oid is not None
        ]
        if linked_official_ids:
            protocol_competitions = _segment_discipline_clause(
                self.session.query(Competition)
                .join(Segment, Segment.competition_id == Competition.id)
                .join(SegmentOfficial, SegmentOfficial.segment_id == Segment.id)
                .filter(SegmentOfficial.official_id.in_(linked_official_ids))
            ).distinct()
            for comp in protocol_competitions:
                all_competitions.add((comp.id, comp.name, comp.year))

        rows = list(all_competitions)
        if not rows:
            return []
        date_map = self._event_dates_for_competition_ids([r[0] for r in rows])

        def _sort_key(row):
            cid, name, year = row
            ev = date_map.get(int(cid))
            missing = ev is None
            neg_ord = -ev.toordinal() if ev else 0
            return (missing, neg_ord, str(year), str(name).lower())

        rows = sorted(rows, key=_sort_key)
        if competition_scope and competition_scope != COMPETITION_SCOPE_ALL:
            cids = [r[0] for r in rows]
            if not cids:
                return []
            scoped = self._filter_orm_competition_scope(
                self.session.query(Competition.id).filter(Competition.id.in_(cids)),
                competition_scope,
            ).all()
            allowed = {int(r[0]) for r in scoped}
            rows = [r for r in rows if int(r[0]) in allowed]
        return self._filter_competition_rows_by_event_dates(
            rows, event_start_date, event_end_date
        )

    def get_judge_segment_stats(
        self,
        judge_ids,
        year_filter=None,
        competition_ids=None,
        discipline_type_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """Segment statistics for one judge or merged identities (multiple judge ids)."""
        ids = self.normalize_judge_ids(judge_ids)
        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(
            core_disc, discipline_type_ids
        )
        # Base query for segments this judge scored
        segment_query = self.session.query(Segment).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        )

        # Apply filters
        if year_filter:
            segment_query = segment_query.filter(Competition.year == year_filter)
        if competition_ids:
            segment_query = segment_query.filter(Segment.competition_id.in_(competition_ids))
        if seg_discipline_ids is not None:
            segment_query = segment_query.filter(
                Segment.discipline_type_id.in_(seg_discipline_ids)
            )
        segment_query = self._filter_orm_competition_scope(
            segment_query, competition_scope
        )
        segment_query = self._apply_competition_event_date_range(
            segment_query, event_start_date, event_end_date
        )

        # Get segments where this judge has PCS scores
        pcs_segments = segment_query.join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            PcsScorePerJudge, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).filter(
            PcsScorePerJudge.judge_id.in_(ids)
        ).distinct()

        # Get segments where these judges have element scores
        element_segments = segment_query.join(
            SkaterSegment, SkaterSegment.segment_id == Segment.id
        ).join(
            Element, Element.skater_segment_id == SkaterSegment.id
        ).join(
            ElementScorePerJudge, ElementScorePerJudge.element_id == Element.id
        ).filter(
            ElementScorePerJudge.judge_id.in_(ids)
        ).distinct()

        segment_stats = []
        all_segments = set()

        # Collect all segments
        for segment in pcs_segments:
            all_segments.add(segment)
        for segment in element_segments:
            all_segments.add(segment)

        all_segment_ids = [segment.id for segment in all_segments]

        # --- Pre-calculate skater counts ---
        segment_skater_counts = dict(
            self.session.execute(
                select(SkaterSegment.segment_id, func.count())
                .group_by(SkaterSegment.segment_id)
                .filter(SkaterSegment.segment_id.in_(all_segment_ids))
            ).all()
        )

        # --- PCS anomaly counts for this judge ---
        pcs_counts = self.session.execute(
            select(
                SkaterSegment.segment_id,
                func.count().label("pcs_anomalies"),
                func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label("pcs_rule_errors")
            )
            .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
            .filter(SkaterSegment.segment_id.in_(all_segment_ids))
            .filter(PcsScorePerJudge.judge_id.in_(ids))
            .filter(or_(func.abs(PcsScorePerJudge.deviation) >= 1.5, PcsScorePerJudge.is_rule_error))
            .group_by(SkaterSegment.segment_id)
        ).all()

        pcs_counts_dict = {seg_id: (pcs_anom, pcs_rule)
                        for seg_id, pcs_anom, pcs_rule in pcs_counts}

        # --- Element anomaly counts for this judge ---
        element_counts = self.session.execute(
            select(
                SkaterSegment.segment_id,
                func.count().label("element_anomalies"),
                func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label("element_rule_errors")
            )
            .join(Element, ElementScorePerJudge.element_id == Element.id)
            .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
            .filter(SkaterSegment.segment_id.in_(all_segment_ids))
            .filter(ElementScorePerJudge.judge_id.in_(ids))
            .filter(or_(func.abs(ElementScorePerJudge.deviation) >= 2, ElementScorePerJudge.is_rule_error))
            .group_by(SkaterSegment.segment_id)
        ).all()

        element_counts_dict = {seg_id: (elem_anom, elem_rule)
                            for seg_id, elem_anom, elem_rule in element_counts}

        # --- Assemble statistics ---
        segment_stats = []
        for segment in all_segments:
            if segment.id not in segment_skater_counts:
                continue

            skater_count = segment_skater_counts[segment.id]

            pcs_anom, pcs_rule = pcs_counts_dict.get(segment.id, (0, 0))
            elem_anom, elem_rule = element_counts_dict.get(segment.id, (0, 0))

            segment_stats.append({
                'segment_id': segment.id,
                'competition_name': segment.competition.name,
                'competition_year': segment.competition.year,
                'discipline': segment.discipline_type.name,
                'segment_name': segment.name,
                'skater_count': skater_count,
                'total_anomalies': pcs_anom + elem_anom,
                'pcs_anomalies': pcs_anom,
                'element_anomalies': elem_anom,
                'total_rule_errors': pcs_rule + elem_rule,
                'pcs_rule_errors': pcs_rule,
                'element_rule_errors': elem_rule
            })

        return pd.DataFrame(segment_stats)

    def get_competition_segment_statistics(self, competition_id):
        """Get segment statistics for all judges in a specific competition"""
        # Get all segments in this competition
        segments_raw = self.session.execute(select(Segment).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        ).filter(
            Competition.id == competition_id
        )).all()

        segments = [segment[0] for segment in segments_raw]

        return pd.DataFrame(self.get_segment_statistics(segments))

    def get_competition_segment_officials_display(self, competition_id: int) -> pd.DataFrame:
        """
        One row per official **per discipline** at this competition from ``segment_official``.

        Columns: official name, member number, discipline (segment category), and **panel_roles**
        (comma-separated distinct roles in that discipline only). Directory appointment names are
        preferred when linked; otherwise protocol roles use the short IJS-style mapping.
        """
        stmt = (
            select(
                SegmentOfficial.official_id.label("official_id"),
                SegmentOfficial.official_name.label("protocol_name"),
                Officials.full_name.label("directory_name"),
                Officials.mbr_number.label("mbr_number"),
                AppointmentTypes.name.label("appointment_name"),
                SegmentOfficial.role.label("protocol_role"),
                DisciplineType.name.label("discipline"),
            )
            .select_from(SegmentOfficial)
            .join(Segment, SegmentOfficial.segment_id == Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
            .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)
            .outerjoin(Officials, SegmentOfficial.official_id == Officials.id)
            .outerjoin(
                AppointmentTypes,
                SegmentOfficial.appointment_type_id == AppointmentTypes.id,
            )
            .where(Competition.id == int(competition_id))
        )
        rows = self.session.execute(stmt).all()
        if not rows:
            return pd.DataFrame()

        states: dict[tuple, dict] = {}

        for r in rows:
            oid, proto, dir_name, mbr, appt_name, proto_role, discipline = r
            proto_s = (proto or "").strip()
            dir_s = (dir_name or "").strip()
            display_name = dir_s or proto_s or "—"
            mbr_s = (mbr or "").strip() if mbr else ""
            disc_label = (discipline or "").strip() or "Unknown"

            if oid is not None:
                dedup_k = ("id", int(oid))
            else:
                name_src = proto_s if proto_s else dir_s if dir_s else display_name
                dedup_k = ("name", _normalize_person_name_key(name_src))

            if dedup_k not in states:
                states[dedup_k] = {
                    "official": display_name,
                    "mbr_number": mbr_s,
                    "has_dir": bool(dir_s),
                    "roles_by_discipline": defaultdict(set),
                }
            st = states[dedup_k]
            base = _panel_role_base_label(appt_name, proto_role)
            if base:
                st["roles_by_discipline"][disc_label].add(base)
            else:
                # Keep discipline row even if protocol/appointment role text is empty
                _ = st["roles_by_discipline"][disc_label]
            if dir_s:
                if not st["has_dir"]:
                    st["official"] = dir_s
                    st["has_dir"] = True
                st["mbr_number"] = mbr_s or st["mbr_number"]
            else:
                st["mbr_number"] = st["mbr_number"] or mbr_s
                if st["official"] in ("", "—") and display_name not in ("", "—"):
                    st["official"] = display_name

        records = []
        for st in states.values():
            by_d = st["roles_by_discipline"]
            for disc, role_set in sorted(by_d.items(), key=lambda x: x[0].lower()):
                roles_str = ", ".join(sorted(role_set, key=str.lower))
                records.append(
                    {
                        "official": st["official"],
                        "mbr_number": st["mbr_number"],
                        "discipline": disc,
                        "panel_roles": roles_str,
                    }
                )

        records.sort(
            key=lambda x: (x["official"].lower(), x["discipline"].lower())
        )
        return pd.DataFrame(records)


    def get_segment_statistics(self, segments):
        """Get segment statistics for all judges in a set of segments"""
        segment_ids = [seg.id for seg in segments]

        # --- Pre-calculate skater counts ---
        segment_skater_counts = dict(
            self.session.execute(
                select(SkaterSegment.segment_id, func.count())
                .group_by(SkaterSegment.segment_id)
                .filter(SkaterSegment.segment_id.in_(segment_ids))
            ).all()
        )

        # --- PCS anomaly counts per (segment_id, judge_id) ---
        pcs_counts = self.session.execute(
            select(
                SkaterSegment.segment_id,
                PcsScorePerJudge.judge_id,
                func.count().label("pcs_anomalies"),
                func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label("pcs_rule_errors")
            )
            .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
            .filter(SkaterSegment.segment_id.in_(segment_ids))
            .filter(or_(func.abs(PcsScorePerJudge.deviation) >= 1.5, PcsScorePerJudge.is_rule_error))
            .group_by(SkaterSegment.segment_id, PcsScorePerJudge.judge_id)
        ).all()

        pcs_counts_dict = {(seg_id, judge_id): (pcs_anom, pcs_rule) 
                        for seg_id, judge_id, pcs_anom, pcs_rule in pcs_counts}

        # --- Element anomaly counts per (segment_id, judge_id) ---
        element_counts = self.session.execute(
            select(
                SkaterSegment.segment_id,
                ElementScorePerJudge.judge_id,
                func.count().label("element_anomalies"),
                func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label("element_rule_errors")
            )
            .join(Element, ElementScorePerJudge.element_id == Element.id)
            .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
            .filter(SkaterSegment.segment_id.in_(segment_ids))
            .filter(or_(func.abs(ElementScorePerJudge.deviation) >= 2, ElementScorePerJudge.is_rule_error))
            .group_by(SkaterSegment.segment_id, ElementScorePerJudge.judge_id)
        ).all()

        element_counts_dict = {(seg_id, judge_id): (elem_anom, elem_rule) 
                            for seg_id, judge_id, elem_anom, elem_rule in element_counts}

        # --- Collect judges involved in these segments ---
        judge_ids = set(j for (_, j) in pcs_counts_dict.keys()) | set(j for (_, j) in element_counts_dict.keys())
        judges = self.session.execute(
            select(Judge).filter(Judge.id.in_(judge_ids))
        ).scalars().all()
        judge_map = {j.id: j for j in judges}

        # --- Assemble statistics ---
        segment_stats = []
        for segment in segments:
            if segment.id not in segment_skater_counts:
                continue

            skater_count = segment_skater_counts[segment.id]

            # Find all judges that had scores in this segment
            segment_judge_ids = {j for (seg_id, j) in pcs_counts_dict if seg_id == segment.id}
            segment_judge_ids |= {j for (seg_id, j) in element_counts_dict if seg_id == segment.id}

            for judge_id in segment_judge_ids:
                pcs_anom, pcs_rule = pcs_counts_dict.get((segment.id, judge_id), (0, 0))
                elem_anom, elem_rule = element_counts_dict.get((segment.id, judge_id), (0, 0))

                judge = judge_map[judge_id]
                segment_stats.append({
                    'judge_id': judge.id,
                    'judge_name': judge.name,
                    'segment_id': segment.id,
                    'segment_name': segment.name,
                    'competition_name': segment.competition.name,
                    'competition_year': segment.competition.year,
                    'discipline': segment.discipline_type.name,
                    'skater_count': skater_count,
                    'total_anomalies': pcs_anom + elem_anom,
                    'pcs_anomalies': pcs_anom,
                    'element_anomalies': elem_anom,
                    'total_rule_errors': pcs_rule + elem_rule,
                    'pcs_rule_errors': pcs_rule,
                    'element_rule_errors': elem_rule
                })

        return segment_stats

    def get_all_rule_errors(
        self,
        year_filter=None,
        competition_ids=None,
        judge_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
    ):
        """Get all rule errors with optional filters."""
        # PCS rule errors
        pcs_query = self.session.query(
            PcsScorePerJudge.judge_id,
            Judge.name.label('judge_name'),
            Competition.name.label('competition_name'),
            Competition.year.label('competition_year'),
            Competition.results_url.label('competition_url'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name'),
            PcsType.name.label('score_type'),
            PcsScorePerJudge.judge_score,
            PcsScorePerJudge.panel_average,
            PcsScorePerJudge.deviation,

        ).join(
            Judge, PcsScorePerJudge.judge_id == Judge.id
        ).join(
            SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).join(
            Segment, SkaterSegment.segment_id == Segment.id
        ).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        ).join(
            Skater, SkaterSegment.skater_id == Skater.id
        ).join(
            PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id
        ).filter(
            PcsScorePerJudge.is_rule_error == True
        )

        # Element rule errors
        element_query = self.session.query(
            ElementScorePerJudge.judge_id,
            Judge.name.label('judge_name'),
            Competition.name.label('competition_name'),
            Competition.year.label('competition_year'),
            Competition.results_url.label('competition_url'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name'),
            Element.name.label('element_name'),
            ElementType.name.label('element_type'),
            Element.notes.label('element_notes'),
            Element.max_goe_allowed.label('max_goe_allowed'),
            ElementScorePerJudge.judge_score,
            ElementScorePerJudge.panel_average,
            ElementScorePerJudge.deviation
        ).join(
            Judge, ElementScorePerJudge.judge_id == Judge.id
        ).join(
            Element, ElementScorePerJudge.element_id == Element.id
        ).join(
            SkaterSegment, Element.skater_segment_id == SkaterSegment.id
        ).join(
            Segment, SkaterSegment.segment_id == Segment.id
        ).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        ).join(
            Skater, SkaterSegment.skater_id == Skater.id
        ).join(
            ElementType, Element.element_type_id == ElementType.id
        ).filter(
            ElementScorePerJudge.is_rule_error == True
        )

        # Apply filters to both queries
        if year_filter:
            pcs_query = pcs_query.filter(Competition.year == year_filter)
            element_query = element_query.filter(Competition.year == year_filter)
        if competition_ids:
            pcs_query = pcs_query.filter(Competition.id.in_(competition_ids))
            element_query = element_query.filter(Competition.id.in_(competition_ids))
        if judge_ids:
            pcs_query = pcs_query.filter(Judge.id.in_(judge_ids))
            element_query = element_query.filter(Judge.id.in_(judge_ids))
        pcs_query = self._filter_orm_competition_scope(pcs_query, competition_scope)
        element_query = self._filter_orm_competition_scope(
            element_query, competition_scope
        )
        pcs_results = pcs_query.all()
        element_results = element_query.all()

        # Convert to DataFrames and add category
        pcs_data = []
        for result in pcs_results:
            pcs_data.append({
                'judge_id': result.judge_id,
                'judge_name': result.judge_name,
                'competition_name': result.competition_name,
                'competition_year': result.competition_year,
                'competition_url': result.competition_url,
                'segment_name': result.segment_name,
                'discipline_name': result.discipline_name,
                'skater_name': result.skater_name,
                'element_name': '',  # PCS doesn't have element name
                'element_type': result.score_type,
                'element_notes': None,
                'max_goe_allowed': None,
                'judge_score': result.judge_score,
                'panel_average': result.panel_average,
                'deviation': result.deviation
            })

        element_data = []
        for result in element_results:
            element_data.append({
                'judge_id': result.judge_id,
                'judge_name': result.judge_name,
                'competition_name': result.competition_name,
                'competition_year': result.competition_year,
                'competition_url': result.competition_url,
                'segment_name': result.segment_name,
                'discipline_name': result.discipline_name,
                'skater_name': result.skater_name,
                'element_name': result.element_name,
                'element_type': result.element_type,
                'element_notes': result.element_notes,
                'max_goe_allowed': result.max_goe_allowed,
                'judge_score': result.judge_score,
                'panel_average': result.panel_average,
                'deviation': result.deviation
            })

        # Combine results
        all_data = pcs_data + element_data
        return pd.DataFrame(all_data)

    def get_years(self):
        """Get all unique years"""
        years = self.session.query(Competition.year).distinct().order_by(Competition.year.desc()).all()
        return [year[0] for year in years]

    def get_discipline_types(self):
        """Get all discipline types"""
        discipline_types = self.session.query(DisciplineType).all()
        return [(dt.id, dt.name) for dt in discipline_types]

    def _discipline_bucket_case_for_segment(self):
        """
        Map ``segment.discipline_type_id`` to Singles / Pairs / Dance / Synchronized
        (Dance = Ice Dance + Solo Dance). Returns a SQLAlchemy ``case`` or None if no ids.
        """
        rows = (
            self.session.query(DisciplineType.id, DisciplineType.name)
            .filter(DisciplineType.name.isnot(None))
            .all()
        )
        whens: list[tuple] = []
        for did, name in rows:
            key = (name or "").strip().lower()
            if key == "singles":
                lbl = "Singles"
            elif key == "pairs":
                lbl = "Pairs"
            elif key in ("ice dance", "solo dance"):
                lbl = "Dance"
            elif key == "synchronized":
                lbl = "Synchronized"
            else:
                continue
            whens.append((Segment.discipline_type_id == int(did), lbl))
        if not whens:
            return None
        return case(*whens, else_=None)

    def _apply_benchmark_competition_filter(
        self,
        stmt,
        *,
        competition_filter_mode: str,
        competition_scope: str,
    ):
        """
        ``db_qualifying``: ``competition.qualifying`` is true.
        ``officials_scope``: use ``_filter_select_competition_scope`` (linked officials competition type).
        """
        if competition_filter_mode == "db_qualifying":
            return stmt.where(Competition.qualifying.is_(True))
        if competition_filter_mode == "officials_scope":
            return self._filter_select_competition_scope(stmt, competition_scope)
        raise ValueError(
            f"competition_filter_mode must be 'db_qualifying' or 'officials_scope', "
            f"not {competition_filter_mode!r}"
        )

    def get_panel_score_benchmarks(
        self,
        *,
        metrics: tuple[str, ...] = ("throwout", "anomaly"),
        competition_filter_mode: str = "db_qualifying",
        competition_scope: str = COMPETITION_SCOPE_QUALIFYING,
        year_filters: list[str] | None = None,
        min_panel_size: int = 3,
        max_panel_size: int = 12,
    ) -> pd.DataFrame:
        """
        Aggregate throw-out and/or **anomaly** rates by discipline bucket and panel size.

        **Competition filter** (``competition_filter_mode``):

        - ``db_qualifying``: only rows with ``competition.qualifying`` true.
        - ``officials_scope``: filter by linked ``officials_analysis.competition_type``
          (same as other analytics); ``competition_scope`` is one of the
          ``COMPETITION_SCOPE_*`` constants.

        **Discipline buckets** (from segment ``discipline_type``): Singles, Pairs,
        Dance (Ice Dance + Solo Dance), Synchronized.

        **Panel size**: judge count per element row (elements) or per
        (skater_segment × PCS component) (PCS).

        **Metrics**:

        - ``throwout``: stored ``thrown_out`` flag (IJS trim at ingest).
        - ``anomaly``: PCS if ``|deviation| >= 1.5`` or rule error; element if
          ``|deviation| >= 2.0`` or rule error (same as judge summary / HTML report).

        Returns columns:
        ``discipline``, ``panel_size``, ``score_type`` (Element / PCS),
        ``benchmark`` (``throwout`` / ``anomaly``), ``total_scores``, ``hits``, ``rate_pct``.
        """
        allowed_m = {"throwout", "anomaly"}
        _metrics: list[str] = []
        for m in metrics:
            if m in allowed_m and m not in _metrics:
                _metrics.append(m)
        metrics_set = tuple(_metrics)
        if not metrics_set:
            return pd.DataFrame(
                columns=[
                    "discipline",
                    "panel_size",
                    "score_type",
                    "benchmark",
                    "total_scores",
                    "hits",
                    "rate_pct",
                ]
            )
        disc_bucket = self._discipline_bucket_case_for_segment()
        if disc_bucket is None:
            return pd.DataFrame(
                columns=[
                    "discipline",
                    "panel_size",
                    "score_type",
                    "benchmark",
                    "total_scores",
                    "hits",
                    "rate_pct",
                ]
            )

        elem_panel = (
            select(
                ElementScorePerJudge.element_id.label("element_id"),
                func.count().label("panel_size"),
            )
            .select_from(ElementScorePerJudge)
            .group_by(ElementScorePerJudge.element_id)
        ).subquery()

        pcs_panel = (
            select(
                PcsScorePerJudge.skater_segment_id.label("skater_segment_id"),
                PcsScorePerJudge.pcs_type_id.label("pcs_type_id"),
                func.count().label("panel_size"),
            )
            .select_from(PcsScorePerJudge)
            .group_by(
                PcsScorePerJudge.skater_segment_id,
                PcsScorePerJudge.pcs_type_id,
            )
        ).subquery()

        out_rows: list[dict] = []

        for metric in metrics_set:
            if metric == "throwout":
                el_hits = func.sum(
                    case((ElementScorePerJudge.thrown_out, 1), else_=0)
                ).label("hits")
                pcs_hits = func.sum(
                    case((PcsScorePerJudge.thrown_out, 1), else_=0)
                ).label("hits")
            else:
                el_hits = func.sum(
                    case(
                        (
                            or_(
                                func.abs(ElementScorePerJudge.deviation) >= 2.0,
                                ElementScorePerJudge.is_rule_error.is_(True),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("hits")
                pcs_hits = func.sum(
                    case(
                        (
                            or_(
                                func.abs(PcsScorePerJudge.deviation) >= 1.5,
                                PcsScorePerJudge.is_rule_error.is_(True),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("hits")

            q_el = (
                select(
                    disc_bucket.label("discipline"),
                    elem_panel.c.panel_size.label("panel_size"),
                    func.count().label("total_scores"),
                    el_hits,
                )
                .select_from(ElementScorePerJudge)
                .join(Element, ElementScorePerJudge.element_id == Element.id)
                .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
                .join(Segment, SkaterSegment.segment_id == Segment.id)
                .join(Competition, Segment.competition_id == Competition.id)
                .join(
                    elem_panel,
                    ElementScorePerJudge.element_id == elem_panel.c.element_id,
                )
                .where(disc_bucket.isnot(None))
                .where(elem_panel.c.panel_size >= min_panel_size)
                .where(elem_panel.c.panel_size <= max_panel_size)
            )
            q_el = self._apply_benchmark_competition_filter(
                q_el,
                competition_filter_mode=competition_filter_mode,
                competition_scope=competition_scope,
            )
            if year_filters:
                q_el = q_el.where(Competition.year.in_(year_filters))
            q_el = q_el.group_by(disc_bucket, elem_panel.c.panel_size)

            for r in self.session.execute(q_el).all():
                tot = int(r.total_scores or 0)
                h = int(r.hits or 0)
                out_rows.append(
                    {
                        "discipline": r.discipline,
                        "panel_size": int(r.panel_size),
                        "score_type": "Element",
                        "benchmark": metric,
                        "total_scores": tot,
                        "hits": h,
                        "rate_pct": round((h / tot * 100) if tot else 0.0, 3),
                    }
                )

            q_pcs = (
                select(
                    disc_bucket.label("discipline"),
                    pcs_panel.c.panel_size.label("panel_size"),
                    func.count().label("total_scores"),
                    pcs_hits,
                )
                .select_from(PcsScorePerJudge)
                .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
                .join(Segment, SkaterSegment.segment_id == Segment.id)
                .join(Competition, Segment.competition_id == Competition.id)
                .join(
                    pcs_panel,
                    and_(
                        PcsScorePerJudge.skater_segment_id
                        == pcs_panel.c.skater_segment_id,
                        PcsScorePerJudge.pcs_type_id == pcs_panel.c.pcs_type_id,
                    ),
                )
                .where(disc_bucket.isnot(None))
                .where(pcs_panel.c.panel_size >= min_panel_size)
                .where(pcs_panel.c.panel_size <= max_panel_size)
            )
            q_pcs = self._apply_benchmark_competition_filter(
                q_pcs,
                competition_filter_mode=competition_filter_mode,
                competition_scope=competition_scope,
            )
            if year_filters:
                q_pcs = q_pcs.where(Competition.year.in_(year_filters))
            q_pcs = q_pcs.group_by(disc_bucket, pcs_panel.c.panel_size)

            for r in self.session.execute(q_pcs).all():
                tot = int(r.total_scores or 0)
                h = int(r.hits or 0)
                out_rows.append(
                    {
                        "discipline": r.discipline,
                        "panel_size": int(r.panel_size),
                        "score_type": "PCS",
                        "benchmark": metric,
                        "total_scores": tot,
                        "hits": h,
                        "rate_pct": round((h / tot * 100) if tot else 0.0, 3),
                    }
                )

        if not out_rows:
            return pd.DataFrame(
                columns=[
                    "discipline",
                    "panel_size",
                    "score_type",
                    "benchmark",
                    "total_scores",
                    "hits",
                    "rate_pct",
                ]
            )
        df = pd.DataFrame(out_rows)
        return df.sort_values(
            ["benchmark", "score_type", "discipline", "panel_size"],
            kind="mergesort",
        ).reset_index(drop=True)

    def get_element_types(self):
        """Get all element types"""
        element_types = self.session.query(ElementType).all()
        return [(et.id, et.name) for et in element_types]

    def get_judge_pcs_stats(
        self,
        judge_ids,
        year_filter=None,
        competition_ids=None,
        discipline_type_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """PCS statistics for one judge id or merged identities (multiple ids)."""
        ids = self.normalize_judge_ids(judge_ids)
        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(
            core_disc, discipline_type_ids
        )
        query = self.session.query(
            PcsScorePerJudge.thrown_out,
            PcsScorePerJudge.deviation,
            PcsScorePerJudge.judge_score,
            PcsScorePerJudge.panel_average,
            PcsScorePerJudge.is_rule_error,
            PcsType.name.label('pcs_type_name'),
            Competition.name.label('competition_name'),
            Competition.results_url.label('competition_url'),
            Competition.year,
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name')
        ).join(Judge, PcsScorePerJudge.judge_id == Judge.id)\
         .join(PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id)\
         .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .join(Skater, SkaterSegment.skater_id == Skater.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id.in_(ids))

        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if seg_discipline_ids is not None:
            query = query.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
        query = self._filter_orm_competition_scope(query, competition_scope)
        query = self._apply_competition_event_date_range(
            query, event_start_date, event_end_date
        )

        results = query.all()

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame([{
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'judge_score': float(r.judge_score),
            'panel_average': float(r.panel_average),
            'is_rule_error': r.is_rule_error,
            'pcs_type_name': r.pcs_type_name,
            'competition_name': r.competition_name,
            'competition_url': r.competition_url,
            'year': r.year,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'skater_name': r.skater_name,
            'anomaly': abs(float(r.deviation)) >= 1.5 or r.is_rule_error
        } for r in results])

        return df

    def get_judge_element_stats(
        self,
        judge_ids,
        year_filter=None,
        competition_ids=None,
        discipline_type_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """Element statistics for one judge id or merged identities."""
        ids = self.normalize_judge_ids(judge_ids)
        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(
            core_disc, discipline_type_ids
        )
        query = self.session.query(
            ElementScorePerJudge.thrown_out,
            ElementScorePerJudge.deviation,
            ElementScorePerJudge.judge_score,
            ElementScorePerJudge.panel_average,
            ElementScorePerJudge.is_rule_error,
            Element.name.label('element_name'),
            Element.element_type,
            ElementType.name.label('element_type_name'),
            Competition.name.label('competition_name'),
            Competition.results_url.label('competition_url'),
            Competition.year,
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name'),
            Skater.name.label('skater_name')
        ).join(Judge, ElementScorePerJudge.judge_id == Judge.id)\
         .join(Element, ElementScorePerJudge.element_id == Element.id)\
         .outerjoin(ElementType, Element.element_type_id == ElementType.id)\
         .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .join(Skater, SkaterSegment.skater_id == Skater.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id.in_(ids))

        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if seg_discipline_ids is not None:
            query = query.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
        query = self._filter_orm_competition_scope(query, competition_scope)
        query = self._apply_competition_event_date_range(
            query, event_start_date, event_end_date
        )

        results = query.all()

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame([{
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'judge_score': float(r.judge_score),
            'panel_average': float(r.panel_average),
            'is_rule_error': r.is_rule_error,
            'element_name': r.element_name,
            'element_type': r.element_type,
            'element_type_name': r.element_type_name or r.element_type,
            'competition_name': r.competition_name,
            'competition_url': r.competition_url,
            'year': r.year,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'skater_name': r.skater_name,
            'anomaly': abs(float(r.deviation)) >= 2.0 or r.is_rule_error
        } for r in results])

        return df

    def get_multi_judge_pcs_comparison(self, judge_ids, year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get PCS comparison data for multiple judges"""
        query = self.session.query(
            Judge.id.label('judge_id'),
            Judge.name.label('judge_name'),
            PcsScorePerJudge.thrown_out,
            PcsScorePerJudge.deviation,
            PcsScorePerJudge.is_rule_error,
            PcsType.name.label('pcs_type_name'),
            Competition.year,
            Competition.name.label('competition_name'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name')
        ).join(PcsScorePerJudge, Judge.id == PcsScorePerJudge.judge_id)\
         .join(PcsType, PcsScorePerJudge.pcs_type_id == PcsType.id)\
         .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id.in_(judge_ids))

        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if discipline_type_ids:
            query = query.filter(Segment.discipline_type_id.in_(discipline_type_ids))

        results = query.all()

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame([{
            'judge_id': r.judge_id,
            'judge_name': r.judge_name,
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'is_rule_error': r.is_rule_error,
            'pcs_type_name': r.pcs_type_name,
            'year': r.year,
            'competition_name': r.competition_name,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'anomaly': abs(float(r.deviation)) >= 1.5 or r.is_rule_error
        } for r in results])

        return df

    def get_multi_judge_element_comparison(self, judge_ids, year_filter=None, competition_ids=None, discipline_type_ids=None):
        """Get element comparison data for multiple judges"""
        query = self.session.query(
            Judge.id.label('judge_id'),
            Judge.name.label('judge_name'),
            ElementScorePerJudge.thrown_out,
            ElementScorePerJudge.deviation,
            ElementScorePerJudge.is_rule_error,
            Element.element_type,
            ElementType.name.label('element_type_name'),
            Competition.year,
            Competition.name.label('competition_name'),
            Segment.name.label('segment_name'),
            DisciplineType.name.label('discipline_name')
        ).join(ElementScorePerJudge, Judge.id == ElementScorePerJudge.judge_id)\
         .join(Element, ElementScorePerJudge.element_id == Element.id)\
         .outerjoin(ElementType, Element.element_type_id == ElementType.id)\
         .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)\
         .join(Segment, SkaterSegment.segment_id == Segment.id)\
         .join(Competition, Segment.competition_id == Competition.id)\
         .outerjoin(DisciplineType, Segment.discipline_type_id == DisciplineType.id)\
         .filter(Judge.id.in_(judge_ids))

        # Apply filters
        if year_filter:
            query = query.filter(Competition.year == year_filter)
        if competition_ids:
            query = query.filter(Competition.id.in_(competition_ids))
        if discipline_type_ids:
            query = query.filter(Segment.discipline_type_id.in_(discipline_type_ids))

        results = query.all()

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame([{
            'judge_id': r.judge_id,
            'judge_name': r.judge_name,
            'thrown_out': r.thrown_out,
            'deviation': float(r.deviation),
            'is_rule_error': r.is_rule_error,
            'element_type': r.element_type,
            'element_type_name': r.element_type_name or r.element_type,
            'year': r.year,
            'competition_name': r.competition_name,
            'segment_name': r.segment_name,
            'discipline_name': r.discipline_name or 'Unknown',
            'anomaly': abs(float(r.deviation)) >= 2.0 or r.is_rule_error
        } for r in results])

        return df

    def calculate_judge_summary_stats(self, pcs_df, element_df):
        """Calculate summary statistics for a judge"""
        stats = {}

        # PCS Statistics
        if not pcs_df.empty:
            stats['pcs_total_scores'] = len(pcs_df)
            stats['pcs_throwout_rate'] = (pcs_df['thrown_out'].sum() / len(pcs_df)) * 100
            stats['pcs_anomaly_rate'] = (pcs_df['anomaly'].sum() / len(pcs_df)) * 100
            stats['pcs_rule_error_rate'] = (pcs_df['is_rule_error'].sum() / len(pcs_df)) * 100
            stats['pcs_avg_deviation'] = pcs_df['deviation'].mean()
        else:
            stats['pcs_total_scores'] = 0
            stats['pcs_throwout_rate'] = 0
            stats['pcs_anomaly_rate'] = 0
            stats['pcs_rule_error_rate'] = 0
            stats['pcs_avg_deviation'] = 0

        # Element Statistics
        if not element_df.empty:
            stats['element_total_scores'] = len(element_df)
            stats['element_throwout_rate'] = (element_df['thrown_out'].sum() / len(element_df)) * 100
            stats['element_anomaly_rate'] = (element_df['anomaly'].sum() / len(element_df)) * 100
            stats['element_rule_error_rate'] = (element_df['is_rule_error'].sum() / len(element_df)) * 100
            stats['element_avg_deviation'] = element_df['deviation'].mean()
        else:
            stats['element_total_scores'] = 0
            stats['element_throwout_rate'] = 0
            stats['element_anomaly_rate'] = 0
            stats['element_rule_error_rate'] = 0
            stats['element_avg_deviation'] = 0

        return stats

    def get_judge_performance_heatmap_data(
        self,
        metric='throwout_rate',
        score_type='both',
        year_filter=None,
        competition_ids=None,
        discipline_type_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """Get data for judge performance heatmap"""
        from cross_judge_cache import (
            assemble_judge_overview_heatmap,
            shard_cache_populated,
        )

        if shard_cache_populated(self.session):
            cached = assemble_judge_overview_heatmap(
                self,
                metric=metric,
                score_type=score_type,
                year_filter=year_filter,
                competition_ids=competition_ids,
                discipline_type_ids=discipline_type_ids,
                competition_scope=competition_scope,
                event_start_date=event_start_date,
                event_end_date=event_end_date,
            )
            if cached is not None:
                return cached

        judge_id_to_label = self.get_judge_id_to_identity_label()
        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(
            core_disc, discipline_type_ids
        )

        # --- Precompute PCS stats grouped by judge ---
        pcs_query = select(
            PcsScorePerJudge.judge_id,
            func.count().label("pcs_total_scores"),
            func.sum(case((PcsScorePerJudge.thrown_out, 1), else_=0)).label("pcs_throwouts"),
            func.sum(case((or_(func.abs(PcsScorePerJudge.deviation) >= 1.5,
                            PcsScorePerJudge.is_rule_error), 1), else_=0)).label("pcs_anomalies"),
            func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label("pcs_rule_errors"),
            func.avg(PcsScorePerJudge.deviation).label("pcs_avg_deviation"),
        ).join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
        ).join(Segment, SkaterSegment.segment_id == Segment.id
        ).join(Competition, Segment.competition_id == Competition.id)

        # Apply filters
        if year_filter:
            pcs_query = pcs_query.filter(Competition.year == year_filter)
        if competition_ids:
            pcs_query = pcs_query.filter(Competition.id.in_(competition_ids))
        if seg_discipline_ids is not None:
            pcs_query = pcs_query.filter(
                Segment.discipline_type_id.in_(seg_discipline_ids)
            )
        pcs_query = self._filter_select_competition_scope(
            pcs_query, competition_scope
        )
        pcs_query = self._apply_competition_event_date_range(
            pcs_query, event_start_date, event_end_date
        )

        pcs_stats = self.session.execute(pcs_query.group_by(PcsScorePerJudge.judge_id)).all()
        pcs_dict = {
            judge_id: dict(
                total=pcs_total,
                throwouts=pcs_thr,
                anomalies=pcs_anom,
                rule_errors=pcs_rules,
                avg_dev=pcs_avg_dev,
            )
            for judge_id, pcs_total, pcs_thr, pcs_anom, pcs_rules, pcs_avg_dev in pcs_stats
        }

        # --- Precompute Element stats grouped by judge ---
        elem_query = select(
            ElementScorePerJudge.judge_id,
            func.count().label("elem_total_scores"),
            func.sum(case((ElementScorePerJudge.thrown_out, 1), else_=0)).label("elem_throwouts"),
            func.sum(case((or_(func.abs(ElementScorePerJudge.deviation) >= 2,
                            ElementScorePerJudge.is_rule_error), 1), else_=0)).label("elem_anomalies"),
            func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label("elem_rule_errors"),
            func.avg(ElementScorePerJudge.deviation).label("elem_avg_deviation"),
        ).join(Element, ElementScorePerJudge.element_id == Element.id
        ).join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id
        ).join(Segment, SkaterSegment.segment_id == Segment.id
        ).join(Competition, Segment.competition_id == Competition.id)

        # Apply filters
        if year_filter:
            elem_query = elem_query.filter(Competition.year == year_filter)
        if competition_ids:
            elem_query = elem_query.filter(Competition.id.in_(competition_ids))
        if seg_discipline_ids is not None:
            elem_query = elem_query.filter(
                Segment.discipline_type_id.in_(seg_discipline_ids)
            )
        elem_query = self._filter_select_competition_scope(
            elem_query, competition_scope
        )
        elem_query = self._apply_competition_event_date_range(
            elem_query, event_start_date, event_end_date
        )

        elem_stats = self.session.execute(elem_query.group_by(ElementScorePerJudge.judge_id)).all()
        elem_dict_raw = {
            judge_id: dict(
                total=elem_total,
                throwouts=elem_thr,
                anomalies=elem_anom,
                rule_errors=elem_rules,
                avg_dev=elem_avg_dev,
            )
            for judge_id, elem_total, elem_thr, elem_anom, elem_rules, elem_avg_dev in elem_stats
        }
        pcs_dict = self._merge_per_judge_stat_dicts_by_identity(
            pcs_dict, judge_id_to_label
        )
        elem_dict = self._merge_per_judge_stat_dicts_by_identity(
            elem_dict_raw, judge_id_to_label
        )

        # --- Precompute excess anomalies once for all judges ---
        excess_anomalies = None
        if metric == 'excess_anomalies':
            excess_raw = self._calculate_all_judge_excess_anomalies(
                year_filter=year_filter,
                competition_ids=competition_ids,
                discipline_ids=seg_discipline_ids,
                score_type=score_type,
                by_competition=False,
                competition_scope=competition_scope,
                event_start_date=event_start_date,
                event_end_date=event_end_date,
            )
            excess_anomalies = self._merge_excess_map_by_identity(
                excess_raw, judge_id_to_label, by_competition=False
            )

        # --- Assemble heatmap data ---
        heatmap_data = []
        for judge_name in sorted(pcs_dict.keys() | elem_dict.keys(), key=str.lower):
            pcs = pcs_dict.get(judge_name, {})
            elem = elem_dict.get(judge_name, {})

            if not pcs and not elem:
                continue

            # Apply score_type filtering
            if score_type == 'pcs':
                total_scores = pcs.get("total", 0)
                throwouts = pcs.get("throwouts", 0)
                anomalies = pcs.get("anomalies", 0)
                rule_errors = pcs.get("rule_errors", 0)
                pcs_scores = pcs.get("total", 0)
                element_scores = 0
            elif score_type == 'element':
                total_scores = elem.get("total", 0)
                throwouts = elem.get("throwouts", 0)
                anomalies = elem.get("anomalies", 0)
                rule_errors = elem.get("rule_errors", 0)
                pcs_scores = 0
                element_scores = elem.get("total", 0)
            else:  # 'both'
                total_scores = pcs.get("total", 0) + elem.get("total", 0)
                throwouts = pcs.get("throwouts", 0) + elem.get("throwouts", 0)
                anomalies = pcs.get("anomalies", 0) + elem.get("anomalies", 0)
                rule_errors = pcs.get("rule_errors", 0) + elem.get("rule_errors", 0)
                pcs_scores = pcs.get("total", 0)
                element_scores = elem.get("total", 0)

            # Skip if no scores for selected type
            if total_scores == 0:
                continue

            if metric == 'throwout_rate':
                value = (throwouts / total_scores * 100) if total_scores else 0
            elif metric == 'anomaly_rate':
                value = (anomalies / total_scores * 100) if total_scores else 0
            elif metric == 'rule_error_rate':
                value = (rule_errors / total_scores * 100) if total_scores else 0
            elif metric == 'rule_errors':
                value = rule_errors
                if value == 0:
                    continue
            elif metric == 'excess_anomalies':
                value = excess_anomalies.get(judge_name, 0)
                if value == 0:
                    continue
            elif metric == 'avg_deviation':
                pcs_mean = float(pcs.get("avg_dev") or 0)
                elem_mean = float(elem.get("avg_dev") or 0)
                if score_type == 'pcs':
                    value = abs(pcs_mean) if pcs_scores else 0
                elif score_type == 'element':
                    value = abs(elem_mean) if element_scores else 0
                else:
                    if total_scores <= 0:
                        continue
                    value = (
                        abs(pcs_mean) * pcs_scores + abs(elem_mean) * element_scores
                    ) / total_scores
                value = round(value, 4)
            else:
                continue

            heatmap_data.append({
                'judge_name': judge_name,
                'metric_value': value if metric == 'avg_deviation' else round(value, 2),
                'total_scores': total_scores,
                'pcs_scores': pcs_scores,
                'element_scores': element_scores
            })

        return pd.DataFrame(heatmap_data)

    def get_pooled_cross_judge_metrics(
        self,
        score_type="both",
        year_filter=None,
        competition_ids=None,
        discipline_type_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        include_excess: bool = True,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """
        Aggregate all judge scores matching the same filters as cross-judge benchmarking.
        Rates are global counts / total scores (each score weighted equally, not each judge).
        """
        from cross_judge_cache import (
            assemble_pooled_cross_judge_metrics,
            shard_cache_populated,
        )

        if shard_cache_populated(self.session):
            cached = assemble_pooled_cross_judge_metrics(
                self,
                score_type=score_type,
                year_filter=year_filter,
                competition_ids=competition_ids,
                discipline_type_ids=discipline_type_ids,
                competition_scope=competition_scope,
                include_excess=include_excess,
                event_start_date=event_start_date,
                event_end_date=event_end_date,
            )
            if cached is not None:
                return cached

        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(
            core_disc, discipline_type_ids
        )

        def _filter_segment_scope(q):
            if year_filter:
                q = q.filter(Competition.year == year_filter)
            if competition_ids:
                q = q.filter(Competition.id.in_(competition_ids))
            if seg_discipline_ids is not None:
                q = q.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
            q = self._filter_select_competition_scope(q, competition_scope)
            q = self._apply_competition_event_date_range(
                q, event_start_date, event_end_date
            )
            return q

        pcs_sel = (
            select(
                func.count().label("n"),
                func.sum(case((PcsScorePerJudge.thrown_out, 1), else_=0)).label("thr"),
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
                ).label("anom"),
                func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label(
                    "re"
                ),
                func.avg(func.abs(PcsScorePerJudge.deviation)).label("avg_abs_dev"),
            )
            .select_from(PcsScorePerJudge)
            .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
            .join(Segment, SkaterSegment.segment_id == Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
        )
        pcs_sel = _filter_segment_scope(pcs_sel)
        pcs_row = self.session.execute(pcs_sel).one()

        elem_sel = (
            select(
                func.count().label("n"),
                func.sum(case((ElementScorePerJudge.thrown_out, 1), else_=0)).label(
                    "thr"
                ),
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
                ).label("anom"),
                func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label(
                    "re"
                ),
                func.avg(func.abs(ElementScorePerJudge.deviation)).label("avg_abs_dev"),
            )
            .select_from(ElementScorePerJudge)
            .join(Element, ElementScorePerJudge.element_id == Element.id)
            .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
            .join(Segment, SkaterSegment.segment_id == Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
        )
        elem_sel = _filter_segment_scope(elem_sel)
        elem_row = self.session.execute(elem_sel).one()

        def _unpack(row):
            n = int(row.n or 0)
            thr = int(row.thr or 0)
            anom = int(row.anom or 0)
            re = int(row.re or 0)
            avg_abs = float(row.avg_abs_dev or 0.0) if n else 0.0
            return n, thr, anom, re, avg_abs

        pn, pthr, panom, pre, pavg_abs = _unpack(pcs_row)
        en, ethr, eanom, ere, eavg_abs = _unpack(elem_row)

        if score_type == "pcs":
            total_scores = pn
            throwouts = pthr
            anomalies = panom
            rule_errors = pre
            avg_abs_pool = pavg_abs if pn else 0.0
        elif score_type == "element":
            total_scores = en
            throwouts = ethr
            anomalies = eanom
            rule_errors = ere
            avg_abs_pool = eavg_abs if en else 0.0
        else:
            total_scores = pn + en
            throwouts = pthr + ethr
            anomalies = panom + eanom
            rule_errors = pre + ere
            if total_scores <= 0:
                avg_abs_pool = 0.0
            else:
                avg_abs_pool = (pavg_abs * pn + eavg_abs * en) / total_scores

        if include_excess:
            excess_raw = self._calculate_all_judge_excess_anomalies(
                year_filter=year_filter,
                competition_ids=competition_ids,
                discipline_ids=seg_discipline_ids,
                score_type=score_type,
                by_competition=False,
                competition_scope=competition_scope,
                event_start_date=event_start_date,
                event_end_date=event_end_date,
            )
            id_to_label = self.get_judge_id_to_identity_label()
            excess_map = self._merge_excess_map_by_identity(
                excess_raw, id_to_label, by_competition=False
            )
            total_excess = int(sum(excess_map.values()))
        else:
            total_excess = 0

        if total_scores <= 0:
            return {
                "total_scores": 0,
                "throwouts": 0,
                "throwout_rate_pct": 0.0,
                "anomalies": 0,
                "anomaly_rate_pct": 0.0,
                "rule_errors": 0,
                "rule_error_rate_pct": 0.0,
                "avg_abs_deviation": 0.0,
                "total_excess_anomalies": total_excess,
                "pcs_scores": pn,
                "element_scores": en,
            }

        return {
            "total_scores": total_scores,
            "throwouts": throwouts,
            "throwout_rate_pct": (throwouts / total_scores) * 100,
            "anomalies": anomalies,
            "anomaly_rate_pct": (anomalies / total_scores) * 100,
            "rule_errors": rule_errors,
            "rule_error_rate_pct": (rule_errors / total_scores) * 100,
            "avg_abs_deviation": round(avg_abs_pool, 6),
            "total_excess_anomalies": total_excess,
            "pcs_scores": pn,
            "element_scores": en,
        }

    def get_judge_competition_protocol_roles_rows(
        self,
        judge_ids,
        competition_pairs: list,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        discipline_type_ids=None,
    ):
        """
        For competitions in ``competition_pairs``, list distinct protocol roles from
        segment_official when any of these judge records shares a linked directory official,
        or—if not linked—when ``segment_official.official_name`` matches the judge's name
        (same normalization as ``_normalize_person_name_key``).

        judge_ids: one id or merged identities (same as elsewhere in this module).

        competition_pairs: [(competition_name, year_str), ...]; row order is re-sorted by
        competition event date (``start_date``, else ``end_date``) descending, then label.

        ``competition_scope`` and ``discipline_type_ids`` should match the filters used to
        build ``competition_pairs`` so label→competition id resolution and roles agree with
        the individual-judge pickers.

        Returns (DataFrame, optional caption when roles unavailable).
        """
        caption = ""
        roles_by_comp_id = defaultdict(set)
        ids = self.normalize_judge_ids(judge_ids)
        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(
            core_disc, discipline_type_ids
        )

        id_by_label = {
            (str(nm), str(yr)): int(cid)
            for cid, nm, yr in self.get_judge_competitions(
                ids, competition_scope, discipline_type_ids=discipline_type_ids
            )
        }
        relevant_cids = set(id_by_label.values())

        name_keys: set[str] = set()
        for jid in ids:
            jrow = self.session.get(Judge, jid)
            if jrow is not None and jrow.name:
                name_keys.add(_normalize_person_name_key(jrow.name))

        official_id = None
        for jid in ids:
            link = self.session.get(JudgeOfficialLink, jid)
            if link and link.official_id:
                official_id = int(link.official_id)
                break

        used_name_fallback = False
        if relevant_cids:
            base = (
                select(
                    Competition.id.label("cid"),
                    func.coalesce(AppointmentTypes.name, SegmentOfficial.role).label(
                        "lbl"
                    ),
                )
                .select_from(SegmentOfficial)
                .join(Segment, SegmentOfficial.segment_id == Segment.id)
                .join(Competition, Segment.competition_id == Competition.id)
                .outerjoin(
                    AppointmentTypes,
                    SegmentOfficial.appointment_type_id == AppointmentTypes.id,
                )
                .where(Competition.id.in_(relevant_cids))
            )
            if seg_discipline_ids is not None:
                base = base.where(Segment.discipline_type_id.in_(seg_discipline_ids))
            if official_id is not None:
                stmt = base.where(SegmentOfficial.official_id == official_id)
                for cid, lbl in self.session.execute(stmt):
                    if lbl is not None and str(lbl).strip():
                        roles_by_comp_id[int(cid)].add(str(lbl).strip())
            elif name_keys:
                stmt = (
                    select(
                        Competition.id.label("cid"),
                        SegmentOfficial.official_name.label("oname"),
                        func.coalesce(
                            AppointmentTypes.name, SegmentOfficial.role
                        ).label("lbl"),
                    )
                    .select_from(SegmentOfficial)
                    .join(Segment, SegmentOfficial.segment_id == Segment.id)
                    .join(Competition, Segment.competition_id == Competition.id)
                    .outerjoin(
                        AppointmentTypes,
                        SegmentOfficial.appointment_type_id == AppointmentTypes.id,
                    )
                    .where(
                        Competition.id.in_(relevant_cids),
                        SegmentOfficial.official_name.isnot(None),
                    )
                )
                if seg_discipline_ids is not None:
                    stmt = stmt.where(
                        Segment.discipline_type_id.in_(seg_discipline_ids)
                    )
                for cid, oname, lbl in self.session.execute(stmt):
                    if not oname or not str(oname).strip():
                        continue
                    if _normalize_person_name_key(str(oname)) not in name_keys:
                        continue
                    used_name_fallback = True
                    if lbl is not None and str(lbl).strip():
                        roles_by_comp_id[int(cid)].add(str(lbl).strip())

        if official_id is None:
            if used_name_fallback:
                caption = (
                    "Protocol roles matched where **segment_official.official_name** matches "
                    "this judge's name (normalized); judge is not linked to a directory official. "
                    "Link under Admin Tools to use official_id and reduce ambiguity."
                )
            elif not name_keys:
                caption = (
                    "No judge name on file; link to a directory official under Admin Tools to "
                    "populate protocol roles from segment_official."
                )
            else:
                caption = (
                    "No segment_official row matched this judge's name in these competitions, "
                    "and the judge is not linked to a directory official. Link under Admin Tools "
                    "to resolve roles by official_id."
                )

        rows = []
        for name, year in competition_pairs:
            yr = str(year)
            cid = id_by_label.get((str(name), yr))
            labels = roles_by_comp_id.get(cid, set()) if cid is not None else set()
            if labels:
                role_cell = ", ".join(sorted(labels, key=str.lower))
            else:
                role_cell = "—"
            rows.append(
                {
                    "Competition": f"{name} ({yr})",
                    "Distinct protocol roles": role_cell,
                    "_sort_cid": cid,
                }
            )

        cids_for_dates = [r["_sort_cid"] for r in rows if r["_sort_cid"] is not None]
        date_map = self._event_dates_for_competition_ids(cids_for_dates)

        def _row_sort_key(r):
            cid = r["_sort_cid"]
            ev = date_map.get(int(cid)) if cid is not None else None
            missing = ev is None
            neg_ord = -ev.toordinal() if ev else 0
            return (missing, neg_ord, r["Competition"].lower())

        rows.sort(key=_row_sort_key)
        for r in rows:
            del r["_sort_cid"]

        return pd.DataFrame(rows), caption

    def get_judge_competition_heatmap_data(
        self,
        metric='throwout_rate',
        score_type='both',
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """Fast version: judge vs competition heatmap with batch queries"""
        from cross_judge_cache import (
            assemble_judge_competition_heatmap,
            shard_cache_populated,
        )

        if shard_cache_populated(self.session):
            cached = assemble_judge_competition_heatmap(
                self,
                metric=metric,
                score_type=score_type,
                competition_scope=competition_scope,
                event_start_date=event_start_date,
                event_end_date=event_end_date,
            )
            if cached is not None:
                return cached

        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(core_disc, None)

        comp_q = self.session.query(Competition.id, Competition.name, Competition.year)
        comp_q = self._filter_orm_competition_scope(comp_q, competition_scope)
        competitions = self._filter_competition_rows_by_event_dates(
            [(c.id, c.name, c.year) for c in comp_q.all()],
            event_start_date,
            event_end_date,
        )
        judge_id_to_label = self.get_judge_id_to_identity_label()
        identity_labels = sorted(set(judge_id_to_label.values()), key=str.lower)

        # --- Precompute PCS stats grouped by (competition, judge) ---
        pcs_q = (
            select(
                Competition.id.label("competition_id"),
                PcsScorePerJudge.judge_id,
                func.count().label("pcs_total_scores"),
                func.sum(case((PcsScorePerJudge.thrown_out, 1), else_=0)).label("pcs_throwouts"),
                func.sum(case((or_(func.abs(PcsScorePerJudge.deviation) >= 1.5,
                                PcsScorePerJudge.is_rule_error), 1), else_=0)).label("pcs_anomalies"),
                func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label("pcs_rule_errors"),
                func.avg(PcsScorePerJudge.deviation).label("pcs_avg_deviation"),
            )
            .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
            .join(Segment, SkaterSegment.segment_id == Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
        )
        pcs_q = self._filter_select_competition_scope(pcs_q, competition_scope)
        if seg_discipline_ids is not None:
            pcs_q = pcs_q.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
        pcs_q = self._apply_competition_event_date_range(
            pcs_q, event_start_date, event_end_date
        )
        pcs_stats = self.session.execute(
            pcs_q.group_by(Competition.id, PcsScorePerJudge.judge_id)
        ).all()

        pcs_dict_raw = {
            (comp_id, judge_id): dict(
                total=pcs_total,
                throwouts=pcs_thr,
                anomalies=pcs_anom,
                rule_errors=pcs_rules,
                avg_dev=pcs_avg_dev,
            )
            for comp_id, judge_id, pcs_total, pcs_thr, pcs_anom, pcs_rules, pcs_avg_dev in pcs_stats
        }

        # --- Precompute Element stats grouped by (competition, judge) ---
        elem_q = (
            select(
                Competition.id.label("competition_id"),
                ElementScorePerJudge.judge_id,
                func.count().label("elem_total_scores"),
                func.sum(case((ElementScorePerJudge.thrown_out, 1), else_=0)).label("elem_throwouts"),
                func.sum(case((or_(func.abs(ElementScorePerJudge.deviation) >= 2,
                                ElementScorePerJudge.is_rule_error), 1), else_=0)).label("elem_anomalies"),
                func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label("elem_rule_errors"),
                func.avg(ElementScorePerJudge.deviation).label("elem_avg_deviation"),
            )
            .join(Element, ElementScorePerJudge.element_id == Element.id)
            .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
            .join(Segment, SkaterSegment.segment_id == Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
        )
        elem_q = self._filter_select_competition_scope(elem_q, competition_scope)
        if seg_discipline_ids is not None:
            elem_q = elem_q.filter(Segment.discipline_type_id.in_(seg_discipline_ids))
        elem_q = self._apply_competition_event_date_range(
            elem_q, event_start_date, event_end_date
        )
        elem_stats = self.session.execute(
            elem_q.group_by(Competition.id, ElementScorePerJudge.judge_id)
        ).all()

        elem_dict_raw = {
            (comp_id, judge_id): dict(
                total=elem_total,
                throwouts=elem_thr,
                anomalies=elem_anom,
                rule_errors=elem_rules,
                avg_dev=elem_avg_dev,
            )
            for comp_id, judge_id, elem_total, elem_thr, elem_anom, elem_rules, elem_avg_dev in elem_stats
        }
        pcs_dict = self._merge_competition_judge_stat_dicts_by_identity(
            pcs_dict_raw, judge_id_to_label
        )
        elem_dict = self._merge_competition_judge_stat_dicts_by_identity(
            elem_dict_raw, judge_id_to_label
        )

        # --- Precompute excess anomalies for all competitions ---
        excess_anomalies = None
        if metric == 'excess_anomalies':
            excess_raw = self._calculate_all_judge_excess_anomalies(
                year_filter=None,
                competition_ids=None,
                discipline_ids=seg_discipline_ids,
                score_type=score_type,
                by_competition=True,
                competition_scope=competition_scope,
                event_start_date=event_start_date,
                event_end_date=event_end_date,
            )
            excess_anomalies = self._merge_excess_map_by_identity(
                excess_raw, judge_id_to_label, by_competition=True
            )

        # --- Build heatmap data ---
        heatmap_data = []
        for comp_id, comp_name, comp_year in competitions:
            for judge_name in identity_labels:
                pcs = pcs_dict.get((comp_id, judge_name), {})
                elem = elem_dict.get((comp_id, judge_name), {})

                if not pcs and not elem:
                    continue

                # Apply score_type filtering
                if score_type == 'pcs':
                    total_scores = pcs.get("total", 0)
                    throwouts = pcs.get("throwouts", 0)
                    anomalies = pcs.get("anomalies", 0)
                    rule_errors = pcs.get("rule_errors", 0)
                elif score_type == 'element':
                    total_scores = elem.get("total", 0)
                    throwouts = elem.get("throwouts", 0)
                    anomalies = elem.get("anomalies", 0)
                    rule_errors = elem.get("rule_errors", 0)
                else:  # 'both'
                    total_scores = pcs.get("total", 0) + elem.get("total", 0)
                    throwouts = pcs.get("throwouts", 0) + elem.get("throwouts", 0)
                    anomalies = pcs.get("anomalies", 0) + elem.get("anomalies", 0)
                    rule_errors = pcs.get("rule_errors", 0) + elem.get("rule_errors", 0)

                # Skip if no scores for selected type
                if total_scores == 0:
                    continue

                if metric == 'throwout_rate':
                    value = (throwouts / total_scores * 100) if total_scores else 0
                elif metric == 'anomaly_rate':
                    value = (anomalies / total_scores * 100) if total_scores else 0
                elif metric == 'rule_error_rate':
                    value = (rule_errors / total_scores * 100) if total_scores else 0
                elif metric == 'rule_errors':
                    value = rule_errors
                    if value == 0:
                        continue
                elif metric == 'excess_anomalies':
                    value = excess_anomalies.get((judge_name, comp_id), 0)
                    if value == 0:
                        continue
                elif metric == 'avg_deviation':
                    pcs_mean = float(pcs.get("avg_dev") or 0)
                    elem_mean = float(elem.get("avg_dev") or 0)
                    pcs_n = pcs.get("total", 0)
                    elem_n = elem.get("total", 0)
                    if score_type == 'pcs':
                        value = abs(pcs_mean) if pcs_n else 0
                    elif score_type == 'element':
                        value = abs(elem_mean) if elem_n else 0
                    else:
                        if total_scores <= 0:
                            continue
                        value = (
                            abs(pcs_mean) * pcs_n + abs(elem_mean) * elem_n
                        ) / total_scores
                    value = round(value, 4)
                else:
                    continue

                heatmap_data.append({
                    'judge_name': judge_name,
                    'competition': f"{comp_name} ({comp_year})",
                    'metric_value': value if metric == 'avg_deviation' else round(value, 2),
                    'total_scores': total_scores
                })

        return pd.DataFrame(heatmap_data)



    def calculate_allowed_errors(self, skater_count):
        return allowed_errors_for_skater_count(skater_count)

    def _segment_ids_for_excess_scope(
        self,
        year_filter=None,
        competition_ids=None,
        discipline_ids=None,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ) -> list[int]:
        """Segment ids matching cross-judge excess filters (ids only, no ORM load)."""
        q = (
            select(Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
            .join(DisciplineType, Segment.discipline_type_id == DisciplineType.id)
        )
        if year_filter:
            q = q.filter(Competition.year == year_filter)
        if competition_ids:
            q = q.filter(Competition.id.in_(competition_ids))
        if discipline_ids is not None:
            q = q.filter(Segment.discipline_type_id.in_(discipline_ids))
        clause = self._competition_scope_clause(competition_scope)
        if clause is not None:
            q = q.filter(clause)
        q = self._apply_competition_event_date_range(
            q, event_start_date, event_end_date
        )
        return [int(r) for r in self.session.execute(q).scalars().all()]

    def _calculate_all_judge_excess_anomalies(
        self,
        year_filter=None,
        competition_ids=None,
        discipline_ids=None,
        score_type='both',
        by_competition=False,
        competition_scope: str = COMPETITION_SCOPE_ALL,
        event_start_date: date | None = None,
        event_end_date: date | None = None,
    ):
        """
        Calculate excess anomalies for judges via ``judge_excess_anomalies_cache``.
        If by_competition=True, returns {(judge_id, competition_id): excess}.
        Otherwise returns {judge_id: excess}.
        """
        segment_ids = self._segment_ids_for_excess_scope(
            year_filter=year_filter,
            competition_ids=competition_ids,
            discipline_ids=discipline_ids,
            competition_scope=competition_scope,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
        )
        if not segment_ids:
            return {}

        ensure_judge_excess_cache(self.session, segment_ids, score_type)
        return aggregate_excess_from_cache(
            self.session,
            segment_ids,
            score_type,
            by_competition=by_competition,
        )

    def _calculate_all_judge_rule_errors(self, year_filter=None, competition_ids=None, discipline_ids=None, score_type='both', by_competition=False):
        """Calculate total rule errors for all judges using optimized batch queries"""

        # Single query to get all segment data with skater counts that match filters
        base_query = select(Segment).join(
            Competition, Segment.competition_id == Competition.id
        ).join(
            DisciplineType, Segment.discipline_type_id == DisciplineType.id
        )

        # Apply filters to base query
        if year_filter:
            base_query = base_query.filter(Competition.year == year_filter)
        if competition_ids:
            base_query = base_query.filter(Competition.id.in_(competition_ids))
        if discipline_ids:
            base_query = base_query.filter(Segment.discipline_type_id.in_(discipline_ids))

        segments_raw = self.session.execute(base_query).all()
        segments = [segment[0] for segment in segments_raw]

        rule_errors_per_judge = defaultdict(int)

        for segment in segments:
            # Get PCS rule errors for this segment
            if score_type in ['pcs', 'both']:
                pcs_rule_errors = self.session.query(
                    PcsScorePerJudge.judge_id,
                    func.count(PcsScorePerJudge.id).label('rule_error_count')
                ).join(
                    SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id
                ).filter(
                    SkaterSegment.segment_id == segment.id,
                    PcsScorePerJudge.is_rule_error == True
                ).group_by(PcsScorePerJudge.judge_id).all()

                for judge_id, count in pcs_rule_errors:
                    rule_errors_per_judge[judge_id] += count

            # Get element rule errors for this segment
            if score_type in ['element', 'both']:
                element_rule_errors = self.session.query(
                    ElementScorePerJudge.judge_id,
                    func.count(ElementScorePerJudge.id).label('rule_error_count')
                ).join(
                    Element, ElementScorePerJudge.element_id == Element.id
                ).join(
                    SkaterSegment, Element.skater_segment_id == SkaterSegment.id
                ).filter(
                    SkaterSegment.segment_id == segment.id,
                    ElementScorePerJudge.is_rule_error == True
                ).group_by(ElementScorePerJudge.judge_id).all()

                for judge_id, count in element_rule_errors:
                    rule_errors_per_judge[judge_id] += count

        return rule_errors_per_judge

    @staticmethod
    def _summary_stats_from_aggregate_tuples(
        pcs_t: Optional[tuple],
        elem_t: Optional[tuple],
    ) -> dict:
        """
        Build the same shape as calculate_judge_summary_stats from SQL aggregates.

        Each tuple is (count, throwouts, anomalies, rule_errors, avg_deviation).
        """
        stats: dict = {}
        if pcs_t is not None and pcs_t[0]:
            n = int(pcs_t[0])
            thr, anom, re, avg_dev = pcs_t[1], pcs_t[2], pcs_t[3], pcs_t[4]
            stats["pcs_total_scores"] = n
            stats["pcs_throwout_rate"] = float(thr) / n * 100
            stats["pcs_anomaly_rate"] = float(anom) / n * 100
            stats["pcs_rule_error_rate"] = float(re) / n * 100
            stats["pcs_avg_deviation"] = (
                float(avg_dev) if avg_dev is not None else 0.0
            )
        else:
            stats["pcs_total_scores"] = 0
            stats["pcs_throwout_rate"] = 0
            stats["pcs_anomaly_rate"] = 0
            stats["pcs_rule_error_rate"] = 0
            stats["pcs_avg_deviation"] = 0

        if elem_t is not None and elem_t[0]:
            n = int(elem_t[0])
            thr, anom, re, avg_dev = elem_t[1], elem_t[2], elem_t[3], elem_t[4]
            stats["element_total_scores"] = n
            stats["element_throwout_rate"] = float(thr) / n * 100
            stats["element_anomaly_rate"] = float(anom) / n * 100
            stats["element_rule_error_rate"] = float(re) / n * 100
            stats["element_avg_deviation"] = (
                float(avg_dev) if avg_dev is not None else 0.0
            )
        else:
            stats["element_total_scores"] = 0
            stats["element_throwout_rate"] = 0
            stats["element_anomaly_rate"] = 0
            stats["element_rule_error_rate"] = 0
            stats["element_avg_deviation"] = 0

        return stats

    @staticmethod
    def _metric_value_for_temporal_summary(
        stats: dict, metric: str, score_type: str
    ) -> Optional[float]:
        """Single-judge metric for temporal plots; None means skip (no usable scores)."""
        if metric == "throwout_rate":
            if score_type == "pcs":
                return stats["pcs_throwout_rate"]
            if score_type == "element":
                return stats["element_throwout_rate"]
            total_scores = stats["pcs_total_scores"] + stats["element_total_scores"]
            if total_scores <= 0:
                return None
            total_throwouts = (
                stats["pcs_throwout_rate"] * stats["pcs_total_scores"] / 100
                + stats["element_throwout_rate"]
                * stats["element_total_scores"]
                / 100
            )
            return (total_throwouts / total_scores) * 100

        if metric == "anomaly_rate":
            if score_type == "pcs":
                return stats["pcs_anomaly_rate"]
            if score_type == "element":
                return stats["element_anomaly_rate"]
            total_scores = stats["pcs_total_scores"] + stats["element_total_scores"]
            if total_scores <= 0:
                return None
            total_anomalies = (
                stats["pcs_anomaly_rate"] * stats["pcs_total_scores"] / 100
                + stats["element_anomaly_rate"]
                * stats["element_total_scores"]
                / 100
            )
            return (total_anomalies / total_scores) * 100

        if metric == "rule_error_rate":
            if score_type == "pcs":
                return stats["pcs_rule_error_rate"]
            if score_type == "element":
                return stats["element_rule_error_rate"]
            total_scores = stats["pcs_total_scores"] + stats["element_total_scores"]
            if total_scores <= 0:
                return None
            total_rule_errors = (
                stats["pcs_rule_error_rate"] * stats["pcs_total_scores"] / 100
                + stats["element_rule_error_rate"]
                * stats["element_total_scores"]
                / 100
            )
            return (total_rule_errors / total_scores) * 100

        # avg_deviation
        if score_type == "pcs":
            return (
                abs(stats["pcs_avg_deviation"])
                if stats["pcs_total_scores"] > 0
                else 0
            )
        if score_type == "element":
            return (
                abs(stats["element_avg_deviation"])
                if stats["element_total_scores"] > 0
                else 0
            )
        total_scores = stats["pcs_total_scores"] + stats["element_total_scores"]
        if total_scores <= 0:
            return None
        weighted_avg = (
            abs(stats["pcs_avg_deviation"]) * stats["pcs_total_scores"]
            + abs(stats["element_avg_deviation"]) * stats["element_total_scores"]
        ) / total_scores
        return weighted_avg

    @staticmethod
    def _merge_aggregate_tuple_list(parts: list[tuple]) -> Optional[tuple]:
        """Merge SQL PCS/element aggregate rows into one tuple."""
        if not parts:
            return None
        n_total = sum(int(p[0]) for p in parts if p and p[0])
        if n_total <= 0:
            return None
        thr = sum(int(p[1]) for p in parts)
        anom = sum(int(p[2]) for p in parts)
        re = sum(int(p[3]) for p in parts)
        w_avg = (
            sum(float(p[4] or 0) * int(p[0]) for p in parts) / n_total
        )
        return (n_total, thr, anom, re, w_avg)

    def _yearly_pcs_elem_combined_map(
        self,
        competition_scope: str,
        judge_ids_filter: Optional[set[int]] = None,
    ) -> dict:
        """Map (judge_id, season_year) -> {\"pcs\": tuple | None, \"elem\": tuple | None}."""
        if judge_ids_filter is not None and not judge_ids_filter:
            return {}

        core_disc = self._qualifying_core_disciplines_active(competition_scope)
        seg_discipline_ids = self._merged_segment_discipline_ids(core_disc, None)

        pcs_q = (
            select(
                PcsScorePerJudge.judge_id,
                Competition.year,
                func.count().label("pcs_total"),
                func.sum(
                    case((PcsScorePerJudge.thrown_out, 1), else_=0)
                ).label("pcs_throwouts"),
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
                ).label("pcs_anomalies"),
                func.sum(
                    case((PcsScorePerJudge.is_rule_error, 1), else_=0)
                ).label("pcs_rule_errors"),
                func.avg(PcsScorePerJudge.deviation).label("pcs_avg_deviation"),
            )
            .select_from(PcsScorePerJudge)
            .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
            .join(Segment, SkaterSegment.segment_id == Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
        )
        if judge_ids_filter is not None:
            pcs_q = pcs_q.where(PcsScorePerJudge.judge_id.in_(list(judge_ids_filter)))
        if seg_discipline_ids is not None:
            pcs_q = pcs_q.where(Segment.discipline_type_id.in_(seg_discipline_ids))
        pcs_q = self._filter_select_competition_scope(pcs_q, competition_scope)
        pcs_q = pcs_q.group_by(PcsScorePerJudge.judge_id, Competition.year)

        elem_q = (
            select(
                ElementScorePerJudge.judge_id,
                Competition.year,
                func.count().label("elem_total"),
                func.sum(
                    case((ElementScorePerJudge.thrown_out, 1), else_=0)
                ).label("elem_throwouts"),
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
                ).label("elem_anomalies"),
                func.sum(
                    case((ElementScorePerJudge.is_rule_error, 1), else_=0)
                ).label("elem_rule_errors"),
                func.avg(ElementScorePerJudge.deviation).label("elem_avg_deviation"),
            )
            .select_from(ElementScorePerJudge)
            .join(Element, ElementScorePerJudge.element_id == Element.id)
            .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
            .join(Segment, SkaterSegment.segment_id == Segment.id)
            .join(Competition, Segment.competition_id == Competition.id)
        )
        if judge_ids_filter is not None:
            elem_q = elem_q.where(
                ElementScorePerJudge.judge_id.in_(list(judge_ids_filter))
            )
        if seg_discipline_ids is not None:
            elem_q = elem_q.where(Segment.discipline_type_id.in_(seg_discipline_ids))
        elem_q = self._filter_select_competition_scope(elem_q, competition_scope)
        elem_q = elem_q.group_by(ElementScorePerJudge.judge_id, Competition.year)

        pcs_rows = self.session.execute(pcs_q).all()
        elem_rows = self.session.execute(elem_q).all()

        combined: dict = {}
        for row in pcs_rows:
            jid, year = row[0], row[1]
            combined[(jid, year)] = {
                "pcs": tuple(row[2:7]),
                "elem": None,
            }
        for row in elem_rows:
            jid, year = row[0], row[1]
            key = (jid, year)
            tup = tuple(row[2:7])
            if key not in combined:
                combined[key] = {"pcs": None, "elem": tup}
            else:
                combined[key]["elem"] = tup
        return combined

    def _temporal_rows_for_judge_id_set(
        self,
        jid_set: set[int],
        combined: dict,
        metric: str,
        score_type: str,
        judge_row_id: int,
        judge_combined_name: str,
    ) -> list:
        years = sorted({y for (jid, y) in combined if jid in jid_set})
        out = []
        for year in years:
            pcs_parts = []
            elem_parts = []
            for jid in jid_set:
                key = (jid, year)
                if key not in combined:
                    continue
                pr = combined[key]
                if pr["pcs"]:
                    pcs_parts.append(pr["pcs"])
                if pr["elem"]:
                    elem_parts.append(pr["elem"])
            pcs_m = self._merge_aggregate_tuple_list(pcs_parts)
            elem_m = self._merge_aggregate_tuple_list(elem_parts)
            stats = self._summary_stats_from_aggregate_tuples(pcs_m, elem_m)
            value = self._metric_value_for_temporal_summary(
                stats, metric, score_type
            )
            if value is None:
                continue
            out.append({
                "judge_id": judge_row_id,
                "judge_name": judge_combined_name,
                "time_period": year,
                "metric_value": round(value, 2),
                "total_scores": stats["pcs_total_scores"]
                + stats["element_total_scores"],
                "pcs_scores": stats["pcs_total_scores"],
                "element_scores": stats["element_total_scores"],
            })
        return out

    @staticmethod
    def consistency_metrics_from_trends_df(trends_df: pd.DataFrame) -> dict:
        """Trend/variance metrics from a judge temporal trends dataframe."""
        if trends_df.empty or len(trends_df) < 2:
            return {
                "trend_direction": "insufficient_data",
                "trend_strength": 0,
                "consistency_score": 0,
                "variance": 0,
                "coefficient_variation": 0,
                "slope": 0,
                "p_value": 1.0,
            }

        values = trends_df["metric_value"].values
        time_periods = range(len(values))

        try:
            slope, _intercept, r_value, p_value, _std_err = linregress(
                time_periods, values
            )
        except Exception:
            slope, r_value, p_value = 0.0, 0.0, 1.0

        if abs(slope) < 0.1:
            trend_direction = "stable"
        elif slope > 0:
            trend_direction = "increasing"
        else:
            trend_direction = "decreasing"

        variance = np.var(values)
        mean_value = np.mean(values)
        coefficient_variation = (
            (np.std(values) / mean_value * 100) if mean_value > 0 else 0
        )

        max_possible_variance = (np.max(values) - np.min(values)) ** 2 / 4
        consistency_score = (
            max(0, 100 - (variance / max_possible_variance * 100))
            if max_possible_variance > 0
            else 100
        )

        return {
            "trend_direction": trend_direction,
            "trend_strength": abs(r_value),
            "consistency_score": round(consistency_score, 2),
            "variance": round(variance, 2),
            "coefficient_variation": round(coefficient_variation, 2),
            "slope": round(slope, 4),
            "p_value": round(p_value, 4),
        }

    def get_identity_group_consistency_ranking(
        self,
        label_to_judge_ids: dict,
        metric: str = "throwout_rate",
        score_type: str = "both",
        competition_scope: str = COMPETITION_SCOPE_ALL,
    ) -> pd.DataFrame:
        """One row per identity group; uses two SQL aggregates total + O(groups) Python."""
        all_ids: set[int] = set()
        for jids in label_to_judge_ids.values():
            all_ids.update(jids)
        if not all_ids:
            return pd.DataFrame()

        combined = self._yearly_pcs_elem_combined_map(competition_scope, all_ids)

        first_ids = [jids[0] for jids in label_to_judge_ids.values() if jids]
        loc_map: dict[int, Optional[str]] = {}
        if first_ids:
            for jid, loc in (
                self.session.query(Judge.id, Judge.location)
                .filter(Judge.id.in_(first_ids))
                .all()
            ):
                loc_map[int(jid)] = loc

        rows_out = []
        for judge_name, jids in label_to_judge_ids.items():
            if not jids:
                continue
            jid_set = set(jids)
            trends_data = self._temporal_rows_for_judge_id_set(
                jid_set,
                combined,
                metric,
                score_type,
                jids[0],
                judge_name,
            )
            trends_df = pd.DataFrame(trends_data)
            if trends_df.empty:
                continue
            cm = self.consistency_metrics_from_trends_df(trends_df)
            j0 = int(jids[0])
            location = (loc_map.get(j0) if loc_map else None) or "Unknown"
            rows_out.append(
                {
                    "judge_name": judge_name,
                    "location": location,
                    "consistency_score": cm["consistency_score"],
                    "trend_direction": cm["trend_direction"],
                    "trend_strength": cm["trend_strength"],
                    "coefficient_variation": cm["coefficient_variation"],
                    "total_scores": int(trends_df["total_scores"].sum()),
                    "years_active": len(trends_df),
                }
            )
        return pd.DataFrame(rows_out)

    def get_temporal_trends_data(
        self,
        judge_id=None,
        period="year",
        metric="throwout_rate",
        score_type="both",
        competition_scope: str = COMPETITION_SCOPE_ALL,
    ):
        """Get temporal trends data for judge consistency over time"""

        if period == "year":
            pass  # only period implemented for SQL aggregates
        elif period == "quarter":
            pass
        else:
            pass

        trends_data = []

        if judge_id:
            ids = self.normalize_judge_ids(judge_id)
            name_parts = []
            for i in ids:
                nm = self.session.query(Judge.name).filter(Judge.id == i).scalar()
                if nm:
                    name_parts.append(nm)
            judge_combined = " · ".join(sorted(set(name_parts), key=str.lower))

            combined = self._yearly_pcs_elem_combined_map(
                competition_scope, set(ids)
            )
            trends_data = self._temporal_rows_for_judge_id_set(
                set(ids),
                combined,
                metric,
                score_type,
                ids[0],
                judge_combined,
            )
        else:
            combined = self._yearly_pcs_elem_combined_map(
                competition_scope, None
            )

            year_metrics_map = defaultdict(list)
            for (_jid, year), parts in combined.items():
                stats = self._summary_stats_from_aggregate_tuples(
                    parts["pcs"], parts["elem"]
                )
                value = self._metric_value_for_temporal_summary(
                    stats, metric, score_type
                )
                if value is None:
                    continue
                year_metrics_map[year].append(
                    {
                        "value": value,
                        "total_scores": stats["pcs_total_scores"]
                        + stats["element_total_scores"],
                    }
                )

            for year in sorted(year_metrics_map.keys()):
                year_metrics = year_metrics_map[year]
                values = [m["value"] for m in year_metrics]
                total_judges = len(year_metrics)
                avg_value = float(np.mean(values))
                median_value = float(np.median(values))
                std_value = float(np.std(values))

                trends_data.append({
                    "time_period": year,
                    "avg_metric_value": round(avg_value, 2),
                    "median_metric_value": round(median_value, 2),
                    "std_metric_value": round(std_value, 2),
                    "total_judges": total_judges,
                    "total_scores": sum(m["total_scores"] for m in year_metrics),
                })

        return pd.DataFrame(trends_data)

    def get_judge_consistency_metrics(
        self,
        judge_ids,
        metric="throwout_rate",
        score_type="both",
        competition_scope: str = COMPETITION_SCOPE_ALL,
    ):
        """Consistency metrics over time for one or merged judge identities."""
        ids = self.normalize_judge_ids(judge_ids)
        trends_df = self.get_temporal_trends_data(
            ids,
            "year",
            metric,
            score_type,
            competition_scope,
        )
        return self.consistency_metrics_from_trends_df(trends_df)

    def calculate_statistical_significance(self, judge_ids, competition_ids=None, discipline_type_ids=None, year_filter=None):
        """Statistical significance tests; judge_ids may be one id or merged identities."""
        ids = self.normalize_judge_ids(judge_ids)

        # Get judge data
        pcs_df = self.get_judge_pcs_stats(ids, year_filter, competition_ids, discipline_type_ids)
        element_df = self.get_judge_element_stats(ids, year_filter, competition_ids, discipline_type_ids)

        if pcs_df.empty and element_df.empty:
            return {
                'pcs_tests': {},
                'element_tests': {},
                'overall_significance': False,
                'bias_detected': False
            }

        results = {
            'pcs_tests': {},
            'element_tests': {},
            'overall_significance': False,
            'bias_detected': False
        }

        # PCS Statistical Tests
        if not pcs_df.empty:
            # Test 1: One-sample t-test for deviation from zero
            deviations = pcs_df['deviation'].values
            t_stat_pcs, p_val_pcs = stats.ttest_1samp(deviations, 0)

            # Test 2: Chi-square test for throwout rate
            throwouts = pcs_df['thrown_out'].sum()
            total_pcs = len(pcs_df)
            expected_throwout_rate = 0.05  # Expected 5% throwout rate
            expected_throwouts = total_pcs * expected_throwout_rate

            if expected_throwouts > 0:
                chi2_pcs, p_chi2_pcs = stats.chisquare([throwouts, total_pcs - throwouts], 
                                                      [expected_throwouts, total_pcs - expected_throwouts])
            else:
                chi2_pcs, p_chi2_pcs = 0, 1.0

            # Test 3: Normality test for deviations (Shapiro-Wilk)
            if len(deviations) >= 3:
                shapiro_stat_pcs, shapiro_p_pcs = stats.shapiro(deviations)
            else:
                shapiro_stat_pcs, shapiro_p_pcs = 1.0, 1.0

            # Test 4: Outlier detection using z-score
            if len(deviations) > 1:
                z_scores_pcs = np.abs(stats.zscore(deviations))
                outliers_pcs = np.sum(z_scores_pcs > 2.58)  # 99% confidence level
            else:
                outliers_pcs = 0
            outlier_rate_pcs = outliers_pcs / len(deviations) if len(deviations) > 0 else 0

            results['pcs_tests'] = {
                'deviation_ttest': {
                    'statistic': round(t_stat_pcs, 4),
                    'p_value': round(p_val_pcs, 4),
                    'significant': p_val_pcs < 0.05,
                    'interpretation': 'Systematic bias detected' if p_val_pcs < 0.05 else 'No systematic bias'
                },
                'throwout_chi2': {
                    'statistic': round(chi2_pcs, 4),
                    'p_value': round(p_chi2_pcs, 4),
                    'significant': p_chi2_pcs < 0.05,
                    'actual_rate': round(throwouts / total_pcs * 100, 2),
                    'expected_rate': 5.0,
                    'interpretation': 'Unusual throwout pattern' if p_chi2_pcs < 0.05 else 'Normal throwout pattern'
                },
                'normality_test': {
                    'statistic': round(shapiro_stat_pcs, 4),
                    'p_value': round(shapiro_p_pcs, 4),
                    'normal_distribution': shapiro_p_pcs > 0.05,
                    'interpretation': 'Normal scoring pattern' if shapiro_p_pcs > 0.05 else 'Non-normal scoring pattern'
                },
                'outlier_analysis': {
                    'outlier_count': int(outliers_pcs),
                    'outlier_rate': round(outlier_rate_pcs * 100, 2),
                    'excessive_outliers': outlier_rate_pcs > 0.05,
                    'interpretation': 'Excessive outliers detected' if outlier_rate_pcs > 0.05 else 'Normal outlier rate'
                }
            }

        # Element Statistical Tests
        if not element_df.empty:
            # Test 1: One-sample t-test for deviation from zero
            deviations = element_df['deviation'].values
            t_stat_elem, p_val_elem = stats.ttest_1samp(deviations, 0)

            # Test 2: Chi-square test for throwout rate
            throwouts = element_df['thrown_out'].sum()
            total_elem = len(element_df)
            expected_throwout_rate = 0.05  # Expected 5% throwout rate
            expected_throwouts = total_elem * expected_throwout_rate

            if expected_throwouts > 0:
                chi2_elem, p_chi2_elem = stats.chisquare([throwouts, total_elem - throwouts], 
                                                        [expected_throwouts, total_elem - expected_throwouts])
            else:
                chi2_elem, p_chi2_elem = 0, 1.0

            # Test 3: Normality test for deviations (Shapiro-Wilk)
            if len(deviations) >= 3:
                shapiro_stat_elem, shapiro_p_elem = stats.shapiro(deviations)
            else:
                shapiro_stat_elem, shapiro_p_elem = 1.0, 1.0

            # Test 4: Outlier detection using z-score
            if len(deviations) > 1:
                z_scores_elem = np.abs(stats.zscore(deviations))
                outliers_elem = np.sum(z_scores_elem > 2.58)  # 99% confidence level
            else:
                outliers_elem = 0
            outlier_rate_elem = outliers_elem / len(deviations) if len(deviations) > 0 else 0

            results['element_tests'] = {
                'deviation_ttest': {
                    'statistic': round(t_stat_elem, 4),
                    'p_value': round(p_val_elem, 4),
                    'significant': p_val_elem < 0.05,
                    'interpretation': 'Systematic bias detected' if p_val_elem < 0.05 else 'No systematic bias'
                },
                'throwout_chi2': {
                    'statistic': round(chi2_elem, 4),
                    'p_value': round(p_chi2_elem, 4),
                    'significant': p_chi2_elem < 0.05,
                    'actual_rate': round(throwouts / total_elem * 100, 2),
                    'expected_rate': 5.0,
                    'interpretation': 'Unusual throwout pattern' if p_chi2_elem < 0.05 else 'Normal throwout pattern'
                },
                'normality_test': {
                    'statistic': round(shapiro_stat_elem, 4),
                    'p_value': round(shapiro_p_elem, 4),
                    'normal_distribution': shapiro_p_elem > 0.05,
                    'interpretation': 'Normal scoring pattern' if shapiro_p_elem > 0.05 else 'Non-normal scoring pattern'
                },
                'outlier_analysis': {
                    'outlier_count': int(outliers_elem),
                    'outlier_rate': round(outlier_rate_elem * 100, 2),
                    'excessive_outliers': outlier_rate_elem > 0.05,
                    'interpretation': 'Excessive outliers detected' if outlier_rate_elem > 0.05 else 'Normal outlier rate'
                }
            }

        # Overall significance assessment
        significant_tests = 0
        total_tests = 0

        for test_category in [results['pcs_tests'], results['element_tests']]:
            if test_category:
                for test_name, test_result in test_category.items():
                    if 'significant' in test_result:
                        total_tests += 1
                        if test_result['significant']:
                            significant_tests += 1
                    elif 'excessive_outliers' in test_result:
                        total_tests += 1
                        if test_result['excessive_outliers']:
                            significant_tests += 1

        results['overall_significance'] = significant_tests > 0
        results['bias_detected'] = significant_tests >= 2  # Require at least 2 significant tests
        results['significance_ratio'] = round(significant_tests / total_tests, 2) if total_tests > 0 else 0

        return results

    def get_bias_detection_summary(self, competition_ids=None, discipline_type_ids=None, year_filter=None):
        """Get a summary of bias detection across all judges"""

        judges = self.session.query(Judge.id, Judge.name, Judge.location).all()
        bias_summary = []

        for judge_id, judge_name, location in judges:
            # Get statistical significance results
            significance_results = self.calculate_statistical_significance(
                judge_id, competition_ids, discipline_type_ids, year_filter
            )

            if significance_results['pcs_tests'] or significance_results['element_tests']:
                # Get basic stats
                pcs_df = self.get_judge_pcs_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
                element_df = self.get_judge_element_stats(judge_id, year_filter, competition_ids, discipline_type_ids)
                stats_summary = self.calculate_judge_summary_stats(pcs_df, element_df)

                bias_summary.append({
                    'judge_id': judge_id,
                    'judge_name': judge_name,
                    'location': location or 'Unknown',
                    'bias_detected': significance_results['bias_detected'],
                    'overall_significance': significance_results['overall_significance'],
                    'significance_ratio': significance_results['significance_ratio'],
                    'total_scores': stats_summary['pcs_total_scores'] + stats_summary['element_total_scores'],
                    'pcs_throwout_rate': stats_summary['pcs_throwout_rate'],
                    'element_throwout_rate': stats_summary['element_throwout_rate'],
                    'pcs_anomaly_rate': stats_summary['pcs_anomaly_rate'],
                    'element_anomaly_rate': stats_summary['element_anomaly_rate']
                })

        return pd.DataFrame(bias_summary)

    def compare_judge_distributions(self, judge_id_1, judge_id_2, score_type='both'):
        """Compare two judges' (or merged identity groups') scoring distributions."""
        from scipy import stats

        ids_1 = self.normalize_judge_ids(judge_id_1)
        ids_2 = self.normalize_judge_ids(judge_id_2)
        if not ids_1 or not ids_2:
            return {}

        # Initialize dataframes
        pcs_df_1 = pd.DataFrame()
        pcs_df_2 = pd.DataFrame()
        element_df_1 = pd.DataFrame()
        element_df_2 = pd.DataFrame()

        # Get data for both judges
        if score_type in ['pcs', 'both']:
            pcs_df_1 = self.get_judge_pcs_stats(ids_1)
            pcs_df_2 = self.get_judge_pcs_stats(ids_2)

        if score_type in ['element', 'both']:
            element_df_1 = self.get_judge_element_stats(ids_1)
            element_df_2 = self.get_judge_element_stats(ids_2)

        comparison_results = {}

        # PCS comparison
        if score_type in ['pcs', 'both'] and not pcs_df_1.empty and not pcs_df_2.empty:
            deviations_1 = pcs_df_1['deviation'].values
            deviations_2 = pcs_df_2['deviation'].values

            # Mann-Whitney U test (non-parametric)
            u_stat, u_p = stats.mannwhitneyu(deviations_1, deviations_2, alternative='two-sided')

            # Kolmogorov-Smirnov test
            ks_stat, ks_p = stats.ks_2samp(deviations_1, deviations_2)

            # T-test for means
            t_stat, t_p = stats.ttest_ind(deviations_1, deviations_2)

            comparison_results['pcs'] = {
                'mannwhitney_u': {
                    'statistic': round(u_stat, 4),
                    'p_value': round(u_p, 4),
                    'significant': u_p < 0.05,
                    'interpretation': 'Different distributions' if u_p < 0.05 else 'Similar distributions'
                },
                'kolmogorov_smirnov': {
                    'statistic': round(ks_stat, 4),
                    'p_value': round(ks_p, 4),
                    'significant': ks_p < 0.05,
                    'interpretation': 'Different distributions' if ks_p < 0.05 else 'Similar distributions'
                },
                'ttest': {
                    'statistic': round(t_stat, 4),
                    'p_value': round(t_p, 4),
                    'significant': t_p < 0.05,
                    'interpretation': 'Different means' if t_p < 0.05 else 'Similar means'
                }
            }

        # Element comparison
        if score_type in ['element', 'both'] and not element_df_1.empty and not element_df_2.empty:
            deviations_1 = element_df_1['deviation'].values
            deviations_2 = element_df_2['deviation'].values

            # Mann-Whitney U test (non-parametric)
            u_stat, u_p = stats.mannwhitneyu(deviations_1, deviations_2, alternative='two-sided')

            # Kolmogorov-Smirnov test
            ks_stat, ks_p = stats.ks_2samp(deviations_1, deviations_2)

            # T-test for means
            t_stat, t_p = stats.ttest_ind(deviations_1, deviations_2)

            comparison_results['element'] = {
                'mannwhitney_u': {
                    'statistic': round(u_stat, 4),
                    'p_value': round(u_p, 4),
                    'significant': u_p < 0.05,
                    'interpretation': 'Different distributions' if u_p < 0.05 else 'Similar distributions'
                },
                'kolmogorov_smirnov': {
                    'statistic': round(ks_stat, 4),
                    'p_value': round(ks_p, 4),
                    'significant': ks_p < 0.05,
                    'interpretation': 'Different distributions' if ks_p < 0.05 else 'Similar distributions'
                },
                'ttest': {
                    'statistic': round(t_stat, 4),
                    'p_value': round(t_p, 4),
                    'significant': t_p < 0.05,
                    'interpretation': 'Different means' if t_p < 0.05 else 'Similar means'
                }
            }

        return comparison_results
