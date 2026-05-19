"""
SQL-backed judge excess-anomaly cache (``judge_excess_anomalies_cache``).

Invalidate per competition when results are re-scraped; rows are rebuilt on read.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from models import (
    Element,
    ElementScorePerJudge,
    JudgeExcessAnomaliesCache,
    PcsScorePerJudge,
    Segment,
    SkaterSegment,
)

_CHUNK_SIZE = 250


def _chunks(items: list[int], size: int) -> Iterable[list[int]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def allowed_errors_for_skater_count(skater_count: int) -> int:
    if skater_count <= 10:
        return 1
    if skater_count <= 20:
        return 2
    return 3


def invalidate_judge_excess_cache_for_competition(session: Session, competition_id: int) -> int:
    """Remove cached excess rows for all segments in a competition (after re-scrape)."""
    seg_ids = select(Segment.id).where(Segment.competition_id == competition_id)
    result = session.execute(
        delete(JudgeExcessAnomaliesCache).where(
            JudgeExcessAnomaliesCache.segment_id.in_(seg_ids)
        )
    )
    session.flush()
    return int(result.rowcount or 0)


def _anomaly_counts_for_segments(
    session: Session, segment_ids: list[int]
) -> tuple[dict[int, int], dict[tuple[int, int], int], dict[tuple[int, int], int]]:
    if not segment_ids:
        return {}, {}, {}

    skater_counts = dict(
        session.execute(
            select(SkaterSegment.segment_id, func.count())
            .where(SkaterSegment.segment_id.in_(segment_ids))
            .group_by(SkaterSegment.segment_id)
        ).all()
    )

    pcs_rows = session.execute(
        select(
            SkaterSegment.segment_id,
            PcsScorePerJudge.judge_id,
            func.count().label("pcs_anomalies"),
        )
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .where(SkaterSegment.segment_id.in_(segment_ids))
        .where(
            or_(
                func.abs(PcsScorePerJudge.deviation) >= 1.5,
                PcsScorePerJudge.is_rule_error,
            )
        )
        .group_by(SkaterSegment.segment_id, PcsScorePerJudge.judge_id)
    ).all()
    pcs_counts = {(seg_id, judge_id): int(cnt) for seg_id, judge_id, cnt in pcs_rows}

    elem_rows = session.execute(
        select(
            SkaterSegment.segment_id,
            ElementScorePerJudge.judge_id,
            func.count().label("element_anomalies"),
        )
        .join(Element, ElementScorePerJudge.element_id == Element.id)
        .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
        .where(SkaterSegment.segment_id.in_(segment_ids))
        .where(
            or_(
                func.abs(ElementScorePerJudge.deviation) >= 2,
                ElementScorePerJudge.is_rule_error,
            )
        )
        .group_by(SkaterSegment.segment_id, ElementScorePerJudge.judge_id)
    ).all()
    elem_counts = {(seg_id, judge_id): int(cnt) for seg_id, judge_id, cnt in elem_rows}

    return skater_counts, pcs_counts, elem_counts


def _cache_rows_for_segments(session: Session, segment_ids: list[int]) -> list[dict]:
    skater_counts, pcs_counts, elem_counts = _anomaly_counts_for_segments(
        session, segment_ids
    )
    keys = set(pcs_counts) | set(elem_counts)
    if not keys:
        return []

    now = datetime.now(timezone.utc)
    rows: list[dict] = []
    for segment_id, judge_id in keys:
        skater_count = skater_counts.get(segment_id)
        if not skater_count:
            continue
        allowed = allowed_errors_for_skater_count(skater_count)
        pcs_a = pcs_counts.get((segment_id, judge_id), 0)
        elem_a = elem_counts.get((segment_id, judge_id), 0)
        total_a = pcs_a + elem_a
        for score_type, observed in (
            ("pcs", pcs_a),
            ("element", elem_a),
            ("both", total_a),
        ):
            rows.append(
                {
                    "judge_id": judge_id,
                    "segment_id": segment_id,
                    "score_type": score_type,
                    "skater_count": skater_count,
                    "allowed_errors": allowed,
                    "total_anomalies": total_a,
                    "pcs_anomalies": pcs_a,
                    "element_anomalies": elem_a,
                    "excess_anomalies": max(0, observed - allowed),
                    "computed_at": now,
                }
            )
    return rows


def _upsert_cache_rows(session: Session, rows: list[dict]) -> None:
    if not rows:
        return
    stmt = pg_insert(JudgeExcessAnomaliesCache).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="judge_excess_anomalies_cache_judge_id_segment_id_score_type_key",
        set_={
            "skater_count": stmt.excluded.skater_count,
            "allowed_errors": stmt.excluded.allowed_errors,
            "total_anomalies": stmt.excluded.total_anomalies,
            "pcs_anomalies": stmt.excluded.pcs_anomalies,
            "element_anomalies": stmt.excluded.element_anomalies,
            "excess_anomalies": stmt.excluded.excess_anomalies,
            "computed_at": stmt.excluded.computed_at,
        },
    )
    session.execute(stmt)


def ensure_judge_excess_cache(
    session: Session, segment_ids: list[int], score_type: str
) -> None:
    """Populate cache rows for segments in scope that lack ``score_type`` entries."""
    if not segment_ids:
        return
    if score_type not in ("pcs", "element", "both"):
        score_type = "both"

    cached_ids = set(
        session.execute(
            select(JudgeExcessAnomaliesCache.segment_id)
            .where(JudgeExcessAnomaliesCache.segment_id.in_(segment_ids))
            .where(JudgeExcessAnomaliesCache.score_type == score_type)
            .distinct()
        )
        .scalars()
        .all()
    )
    missing = [sid for sid in segment_ids if sid not in cached_ids]
    for chunk in _chunks(missing, _CHUNK_SIZE):
        rows = _cache_rows_for_segments(session, chunk)
        _upsert_cache_rows(session, rows)
    session.flush()


def aggregate_excess_from_cache(
    session: Session,
    segment_ids: list[int],
    score_type: str,
    *,
    by_competition: bool = False,
) -> dict:
    """Sum ``excess_anomalies`` from cache for the given segments and score type."""
    if not segment_ids:
        return {}
    if score_type not in ("pcs", "element", "both"):
        score_type = "both"

    if by_competition:
        q = (
            select(
                JudgeExcessAnomaliesCache.judge_id,
                Segment.competition_id,
                func.sum(JudgeExcessAnomaliesCache.excess_anomalies),
            )
            .join(Segment, JudgeExcessAnomaliesCache.segment_id == Segment.id)
            .where(JudgeExcessAnomaliesCache.segment_id.in_(segment_ids))
            .where(JudgeExcessAnomaliesCache.score_type == score_type)
            .group_by(JudgeExcessAnomaliesCache.judge_id, Segment.competition_id)
        )
        out = defaultdict(int)
        for judge_id, comp_id, total in session.execute(q).all():
            out[(int(judge_id), int(comp_id))] = int(total or 0)
        return out

    q = (
        select(
            JudgeExcessAnomaliesCache.judge_id,
            func.sum(JudgeExcessAnomaliesCache.excess_anomalies),
        )
        .where(JudgeExcessAnomaliesCache.segment_id.in_(segment_ids))
        .where(JudgeExcessAnomaliesCache.score_type == score_type)
        .group_by(JudgeExcessAnomaliesCache.judge_id)
    )
    return {int(judge_id): int(total or 0) for judge_id, total in session.execute(q).all()}
