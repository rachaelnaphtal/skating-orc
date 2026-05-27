"""
Per-competition shards for cross-judge benchmarking.

Precompute with ``scripts/precompute_cross_judge_cache.py``. Rows are invalidated when
a competition is re-scraped (see ``downloadResults``).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import case, delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from models import (
    Competition,
    CrossJudgeCompetitionShard,
    Element,
    ElementScorePerJudge,
    PcsScorePerJudge,
    Segment,
    SkaterSegment,
)

_SHARD_INT_COLS = (
    "pcs_total",
    "pcs_throwouts",
    "pcs_anomalies",
    "pcs_rule_errors",
    "elem_total",
    "elem_throwouts",
    "elem_anomalies",
    "elem_rule_errors",
)
_SHARD_FLOAT_COLS = (
    "pcs_sum_deviation",
    "pcs_sum_abs_deviation",
    "elem_sum_deviation",
    "elem_sum_abs_deviation",
)


def _finalize_shard_row(bucket: dict[str, Any], *, computed_at: datetime) -> dict[str, Any]:
    """Ensure every column is present (ORM multiparam insert requires uniform keys)."""
    return {
        "competition_id": int(bucket["competition_id"]),
        "discipline_type_id": int(bucket["discipline_type_id"]),
        "judge_id": int(bucket["judge_id"]),
        "competition_year": str(bucket["competition_year"]),
        **{col: int(bucket.get(col) or 0) for col in _SHARD_INT_COLS},
        **{col: float(bucket.get(col) or 0) for col in _SHARD_FLOAT_COLS},
        "computed_at": computed_at,
    }


def ensure_cross_judge_cache_tables(bind: Engine) -> None:
    CrossJudgeCompetitionShard.__table__.create(bind, checkfirst=True)


def shard_cache_populated(session: Session) -> bool:
    ensure_cross_judge_cache_tables(session.get_bind())
    n = session.execute(
        select(func.count()).select_from(CrossJudgeCompetitionShard).limit(1)
    ).scalar()
    return bool(n)


def competition_has_shard_cache(session: Session, competition_id: int) -> bool:
    """True if at least one shard row exists for this competition."""
    ensure_cross_judge_cache_tables(session.get_bind())
    return (
        session.execute(
            select(CrossJudgeCompetitionShard.competition_id)
            .where(CrossJudgeCompetitionShard.competition_id == competition_id)
            .limit(1)
        ).first()
        is not None
    )


def iter_competitions_for_precompute(
    session: Session, competition_ids: list[int] | None = None
) -> list[tuple[int, str, str]]:
    """(competition_id, name, year) in id order."""
    q = select(Competition.id, Competition.name, Competition.year).order_by(
        Competition.id
    )
    if competition_ids is not None:
        q = q.where(Competition.id.in_(competition_ids))
    return [
        (int(r.id), str(r.name), str(r.year))
        for r in session.execute(q).all()
    ]


def invalidate_cross_judge_cache_for_competition(
    session: Session, competition_id: int
) -> int:
    ensure_cross_judge_cache_tables(session.get_bind())
    result = session.execute(
        delete(CrossJudgeCompetitionShard).where(
            CrossJudgeCompetitionShard.competition_id == competition_id
        )
    )
    session.flush()
    return int(result.rowcount or 0)


def _pcs_agg_rows(session: Session, competition_id: int | None) -> list:
    q = (
        select(
            Competition.id.label("competition_id"),
            Competition.year.label("competition_year"),
            Segment.discipline_type_id,
            PcsScorePerJudge.judge_id,
            func.count().label("pcs_total"),
            func.sum(case((PcsScorePerJudge.thrown_out, 1), else_=0)).label(
                "pcs_throwouts"
            ),
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
            func.sum(case((PcsScorePerJudge.is_rule_error, 1), else_=0)).label(
                "pcs_rule_errors"
            ),
            func.sum(PcsScorePerJudge.deviation).label("pcs_sum_deviation"),
            func.sum(func.abs(PcsScorePerJudge.deviation)).label(
                "pcs_sum_abs_deviation"
            ),
        )
        .select_from(PcsScorePerJudge)
        .join(SkaterSegment, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
        .group_by(
            Competition.id,
            Competition.year,
            Segment.discipline_type_id,
            PcsScorePerJudge.judge_id,
        )
    )
    if competition_id is not None:
        q = q.filter(Competition.id == competition_id)
    return list(session.execute(q).all())


def _elem_agg_rows(session: Session, competition_id: int | None) -> list:
    q = (
        select(
            Competition.id.label("competition_id"),
            Competition.year.label("competition_year"),
            Segment.discipline_type_id,
            ElementScorePerJudge.judge_id,
            func.count().label("elem_total"),
            func.sum(case((ElementScorePerJudge.thrown_out, 1), else_=0)).label(
                "elem_throwouts"
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
            ).label("elem_anomalies"),
            func.sum(case((ElementScorePerJudge.is_rule_error, 1), else_=0)).label(
                "elem_rule_errors"
            ),
            func.sum(ElementScorePerJudge.deviation).label("elem_sum_deviation"),
            func.sum(func.abs(ElementScorePerJudge.deviation)).label(
                "elem_sum_abs_deviation"
            ),
        )
        .select_from(ElementScorePerJudge)
        .join(Element, ElementScorePerJudge.element_id == Element.id)
        .join(SkaterSegment, Element.skater_segment_id == SkaterSegment.id)
        .join(Segment, SkaterSegment.segment_id == Segment.id)
        .join(Competition, Segment.competition_id == Competition.id)
        .group_by(
            Competition.id,
            Competition.year,
            Segment.discipline_type_id,
            ElementScorePerJudge.judge_id,
        )
    )
    if competition_id is not None:
        q = q.filter(Competition.id == competition_id)
    return list(session.execute(q).all())


def _merge_agg_into_shard_map(
    shard_map: dict[tuple[int, int, int], dict[str, Any]],
    rows: list,
    *,
    score_kind: str,
) -> None:
    prefix = "pcs" if score_kind == "pcs" else "elem"
    for row in rows:
        key = (int(row.competition_id), int(row.discipline_type_id), int(row.judge_id))
        bucket = shard_map.setdefault(
            key,
            {
                "competition_id": int(row.competition_id),
                "discipline_type_id": int(row.discipline_type_id),
                "judge_id": int(row.judge_id),
                "competition_year": str(row.competition_year),
            },
        )
        for field in (
            "total",
            "throwouts",
            "anomalies",
            "rule_errors",
            "sum_deviation",
            "sum_abs_deviation",
        ):
            bucket[f"{prefix}_{field}"] = int(getattr(row, f"{prefix}_{field}") or 0)


def build_cross_judge_shards_for_competition(
    session: Session, competition_id: int
) -> int:
    """Rebuild shard rows for one competition. Returns rows written."""
    ensure_cross_judge_cache_tables(session.get_bind())
    invalidate_cross_judge_cache_for_competition(session, competition_id)

    shard_map: dict[tuple[int, int, int], dict[str, Any]] = {}
    _merge_agg_into_shard_map(shard_map, _pcs_agg_rows(session, competition_id), score_kind="pcs")
    _merge_agg_into_shard_map(
        shard_map, _elem_agg_rows(session, competition_id), score_kind="elem"
    )

    if not shard_map:
        return 0

    now = datetime.now(timezone.utc)
    rows = [_finalize_shard_row(bucket, computed_at=now) for bucket in shard_map.values()]

    stmt = pg_insert(CrossJudgeCompetitionShard).values(rows)
    session.execute(stmt)
    session.flush()
    return len(rows)


def precompute_cross_judge_shards(
    session: Session,
    *,
    competition_ids: list[int] | None = None,
    skip_cached: bool = False,
    commit_each: bool = True,
    on_progress: Any | None = None,
) -> tuple[int, int, int]:
    """
    Warm shards for all competitions (or a subset).

    Returns ``(rows_written, built_count, skipped_count)``.
    When ``skip_cached`` is True, competitions that already have shard rows are
    left unchanged (use ``--force`` in the CLI to rebuild anyway).
    """
    targets = iter_competitions_for_precompute(session, competition_ids)
    n = len(targets)
    rows_written = 0
    built = 0
    skipped = 0
    for i, (cid, name, year) in enumerate(targets, start=1):
        if skip_cached and competition_has_shard_cache(session, cid):
            skipped += 1
            if on_progress is not None:
                on_progress(i, n, cid, name, year, 0, "skipped")
            continue
        n_rows = build_cross_judge_shards_for_competition(session, cid)
        rows_written += n_rows
        built += 1
        if commit_each:
            session.commit()
        if on_progress is not None:
            on_progress(i, n, cid, name, year, n_rows, "built")
    if not commit_each:
        session.commit()
    return rows_written, built, skipped


def _filtered_competition_ids(
    analytics,
    *,
    year_filter=None,
    competition_ids=None,
    competition_scope: str,
    event_start_date: date | None,
    event_end_date: date | None,
) -> list[int]:
    session = analytics.session
    q = select(Competition.id, Competition.name, Competition.year)
    q = analytics._filter_select_competition_scope(q, competition_scope)
    if year_filter:
        q = q.filter(Competition.year == str(year_filter))
    if competition_ids:
        q = q.filter(Competition.id.in_(competition_ids))
    rows = analytics._filter_competition_rows_by_event_dates(
        [(int(r.id), r.name, r.year) for r in session.execute(q).all()],
        event_start_date,
        event_end_date,
    )
    return [int(r[0]) for r in rows]


_OVERVIEW_HEATMAP_COLUMNS = [
    "judge_name",
    "metric_value",
    "total_scores",
    "pcs_scores",
    "element_scores",
]
_COMP_HEATMAP_COLUMNS = [
    "judge_name",
    "competition",
    "metric_value",
    "total_scores",
]


def _empty_overview_heatmap_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_OVERVIEW_HEATMAP_COLUMNS)


def _empty_competition_heatmap_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_COMP_HEATMAP_COLUMNS)


def _scoped_competition_ids(
    analytics,
    *,
    year_filter=None,
    competition_ids=None,
    competition_scope: str,
    event_start_date: date | None,
    event_end_date: date | None,
) -> list[int]:
    if not shard_cache_populated(analytics.session):
        return []
    return _filtered_competition_ids(
        analytics,
        year_filter=year_filter,
        competition_ids=competition_ids,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
    )


def _shard_agg_select(*, by_competition: bool):
    cols = [CrossJudgeCompetitionShard.judge_id]
    if by_competition:
        cols = [
            CrossJudgeCompetitionShard.competition_id,
            CrossJudgeCompetitionShard.judge_id,
        ]
    return select(
        *cols,
        func.sum(CrossJudgeCompetitionShard.pcs_total).label("pcs_total"),
        func.sum(CrossJudgeCompetitionShard.pcs_throwouts).label("pcs_throwouts"),
        func.sum(CrossJudgeCompetitionShard.pcs_anomalies).label("pcs_anomalies"),
        func.sum(CrossJudgeCompetitionShard.pcs_rule_errors).label("pcs_rule_errors"),
        func.sum(CrossJudgeCompetitionShard.pcs_sum_deviation).label("pcs_sum_deviation"),
        func.sum(CrossJudgeCompetitionShard.pcs_sum_abs_deviation).label(
            "pcs_sum_abs_deviation"
        ),
        func.sum(CrossJudgeCompetitionShard.elem_total).label("elem_total"),
        func.sum(CrossJudgeCompetitionShard.elem_throwouts).label("elem_throwouts"),
        func.sum(CrossJudgeCompetitionShard.elem_anomalies).label("elem_anomalies"),
        func.sum(CrossJudgeCompetitionShard.elem_rule_errors).label("elem_rule_errors"),
        func.sum(CrossJudgeCompetitionShard.elem_sum_deviation).label("elem_sum_deviation"),
        func.sum(CrossJudgeCompetitionShard.elem_sum_abs_deviation).label(
            "elem_sum_abs_deviation"
        ),
    )


def _bucket_from_agg_row(row, prefix: str) -> dict[str, Any]:
    total = int(getattr(row, f"{prefix}_total") or 0)
    return {
        "total": total,
        "throwouts": int(getattr(row, f"{prefix}_throwouts") or 0),
        "anomalies": int(getattr(row, f"{prefix}_anomalies") or 0),
        "rule_errors": int(getattr(row, f"{prefix}_rule_errors") or 0),
        "avg_dev": (
            float(getattr(row, f"{prefix}_sum_deviation") or 0) / total if total else 0.0
        ),
    }


def _load_shard_judge_aggregates_sql(
    session: Session,
    comp_ids: list[int],
    seg_discipline_ids: list[int] | None,
    *,
    by_competition: bool,
) -> tuple[dict[Any, dict], dict[Any, dict]]:
    """Sum shard rows in SQL (per judge, or per competition×judge)."""
    if not comp_ids:
        return {}, {}

    q = _shard_agg_select(by_competition=by_competition).where(
        CrossJudgeCompetitionShard.competition_id.in_(comp_ids)
    )
    if seg_discipline_ids is not None:
        q = q.filter(
            CrossJudgeCompetitionShard.discipline_type_id.in_(seg_discipline_ids)
        )
    group_cols = [CrossJudgeCompetitionShard.judge_id]
    if by_competition:
        group_cols = [
            CrossJudgeCompetitionShard.competition_id,
            CrossJudgeCompetitionShard.judge_id,
        ]
    q = q.group_by(*group_cols)

    pcs_raw: dict[Any, dict] = {}
    elem_raw: dict[Any, dict] = {}
    for row in session.execute(q).all():
        key: Any = int(row.judge_id)
        if by_competition:
            key = (int(row.competition_id), int(row.judge_id))
        pcs_raw[key] = _bucket_from_agg_row(row, "pcs")
        elem_raw[key] = _bucket_from_agg_row(row, "elem")
    return pcs_raw, elem_raw


def _load_shard_pooled_totals_sql(
    session: Session,
    comp_ids: list[int],
    seg_discipline_ids: list[int] | None,
) -> dict[str, float]:
    if not comp_ids:
        return {
            "pn": 0,
            "pthr": 0,
            "panom": 0,
            "pre": 0,
            "pabs": 0.0,
            "en": 0,
            "ethr": 0,
            "eanom": 0,
            "ere": 0,
            "eabs": 0.0,
        }
    q = select(
        func.sum(CrossJudgeCompetitionShard.pcs_total).label("pn"),
        func.sum(CrossJudgeCompetitionShard.pcs_throwouts).label("pthr"),
        func.sum(CrossJudgeCompetitionShard.pcs_anomalies).label("panom"),
        func.sum(CrossJudgeCompetitionShard.pcs_rule_errors).label("pre"),
        func.sum(CrossJudgeCompetitionShard.pcs_sum_abs_deviation).label("pabs"),
        func.sum(CrossJudgeCompetitionShard.elem_total).label("en"),
        func.sum(CrossJudgeCompetitionShard.elem_throwouts).label("ethr"),
        func.sum(CrossJudgeCompetitionShard.elem_anomalies).label("eanom"),
        func.sum(CrossJudgeCompetitionShard.elem_rule_errors).label("ere"),
        func.sum(CrossJudgeCompetitionShard.elem_sum_abs_deviation).label("eabs"),
    ).where(CrossJudgeCompetitionShard.competition_id.in_(comp_ids))
    if seg_discipline_ids is not None:
        q = q.filter(
            CrossJudgeCompetitionShard.discipline_type_id.in_(seg_discipline_ids)
        )
    row = session.execute(q).one()
    return {
        "pn": float(row.pn or 0),
        "pthr": float(row.pthr or 0),
        "panom": float(row.panom or 0),
        "pre": float(row.pre or 0),
        "pabs": float(row.pabs or 0),
        "en": float(row.en or 0),
        "ethr": float(row.ethr or 0),
        "eanom": float(row.eanom or 0),
        "ere": float(row.ere or 0),
        "eabs": float(row.eabs or 0),
    }


def _metric_value(
    metric: str,
    score_type: str,
    *,
    pcs: dict,
    elem: dict,
    excess: int = 0,
) -> float | None:
    if score_type == "pcs":
        total = int(pcs.get("total") or 0)
        throwouts = int(pcs.get("throwouts") or 0)
        anomalies = int(pcs.get("anomalies") or 0)
        rule_errors = int(pcs.get("rule_errors") or 0)
        pcs_n = int(pcs.get("total") or 0)
        elem_n = 0
    elif score_type == "element":
        total = int(elem.get("total") or 0)
        throwouts = int(elem.get("throwouts") or 0)
        anomalies = int(elem.get("anomalies") or 0)
        rule_errors = int(elem.get("rule_errors") or 0)
        pcs_n = 0
        elem_n = int(elem.get("total") or 0)
    else:
        total = int(pcs.get("total") or 0) + int(elem.get("total") or 0)
        throwouts = int(pcs.get("throwouts") or 0) + int(elem.get("throwouts") or 0)
        anomalies = int(pcs.get("anomalies") or 0) + int(elem.get("anomalies") or 0)
        rule_errors = int(pcs.get("rule_errors") or 0) + int(elem.get("rule_errors") or 0)
        pcs_n = int(pcs.get("total") or 0)
        elem_n = int(elem.get("total") or 0)

    if total <= 0:
        return None

    if metric == "throwout_rate":
        return (throwouts / total * 100) if total else 0.0
    if metric == "anomaly_rate":
        return (anomalies / total * 100) if total else 0.0
    if metric == "rule_error_rate":
        return (rule_errors / total * 100) if total else 0.0
    if metric == "rule_errors":
        return float(rule_errors) if rule_errors else None
    if metric == "excess_anomalies":
        return float(excess) if excess else None
    if metric == "avg_deviation":
        pcs_mean = float(pcs.get("avg_dev") or 0)
        elem_mean = float(elem.get("avg_dev") or 0)
        if score_type == "pcs":
            return abs(pcs_mean) if pcs_n else None
        if score_type == "element":
            return abs(elem_mean) if elem_n else None
        return (abs(pcs_mean) * pcs_n + abs(elem_mean) * elem_n) / total
    return None


def assemble_judge_overview_heatmap(
    analytics,
    *,
    metric: str,
    score_type: str,
    year_filter=None,
    competition_ids=None,
    discipline_type_ids=None,
    competition_scope: str,
    event_start_date: date | None,
    event_end_date: date | None,
) -> pd.DataFrame | None:
    session = analytics.session
    if not shard_cache_populated(session):
        return None
    core_disc = analytics._qualifying_core_disciplines_active(competition_scope)
    seg_discipline_ids = analytics._merged_segment_discipline_ids(
        core_disc, discipline_type_ids
    )
    comp_ids = _scoped_competition_ids(
        analytics,
        year_filter=year_filter,
        competition_ids=competition_ids,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
    )
    if not comp_ids:
        return _empty_overview_heatmap_df()

    judge_id_to_label = analytics.get_judge_id_to_identity_label()
    pcs_raw, elem_raw = _load_shard_judge_aggregates_sql(
        session, comp_ids, seg_discipline_ids, by_competition=False
    )
    if not pcs_raw and not elem_raw:
        return _empty_overview_heatmap_df()
    pcs_dict = analytics._merge_per_judge_stat_dicts_by_identity(
        pcs_raw, judge_id_to_label
    )
    elem_dict = analytics._merge_per_judge_stat_dicts_by_identity(
        elem_raw, judge_id_to_label
    )

    excess_anomalies: dict[str, int] | None = None
    if metric == "excess_anomalies":
        excess_raw = analytics._calculate_all_judge_excess_anomalies(
            year_filter=year_filter,
            competition_ids=competition_ids,
            discipline_ids=seg_discipline_ids,
            score_type=score_type,
            by_competition=False,
            competition_scope=competition_scope,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
        )
        excess_anomalies = analytics._merge_excess_map_by_identity(
            excess_raw, judge_id_to_label, by_competition=False
        )

    heatmap_data = []
    for judge_name in sorted(pcs_dict.keys() | elem_dict.keys(), key=str.lower):
        pcs = pcs_dict.get(judge_name, {})
        elem = elem_dict.get(judge_name, {})
        exc = (
            int(excess_anomalies.get(judge_name, 0))
            if excess_anomalies is not None
            else 0
        )
        value = _metric_value(
            metric, score_type, pcs=pcs, elem=elem, excess=exc
        )
        if value is None:
            continue
        if metric in ("rule_errors", "excess_anomalies") and value == 0:
            continue

        total_scores = int(pcs.get("total", 0) or 0) + int(elem.get("total", 0) or 0)
        if score_type == "pcs":
            total_scores = int(pcs.get("total", 0) or 0)
        elif score_type == "element":
            total_scores = int(elem.get("total", 0) or 0)

        heatmap_data.append(
            {
                "judge_name": judge_name,
                "metric_value": round(value, 4)
                if metric == "avg_deviation"
                else round(value, 2),
                "total_scores": total_scores,
                "pcs_scores": int(pcs.get("total", 0) or 0),
                "element_scores": int(elem.get("total", 0) or 0),
            }
        )

    return pd.DataFrame(heatmap_data)


def assemble_judge_competition_heatmap(
    analytics,
    *,
    metric: str,
    score_type: str,
    competition_scope: str,
    event_start_date: date | None,
    event_end_date: date | None,
) -> pd.DataFrame | None:
    session = analytics.session
    if not shard_cache_populated(session):
        return None
    core_disc = analytics._qualifying_core_disciplines_active(competition_scope)
    seg_discipline_ids = analytics._merged_segment_discipline_ids(core_disc, None)
    comp_ids = _scoped_competition_ids(
        analytics,
        competition_ids=None,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
    )
    if not comp_ids:
        return _empty_competition_heatmap_df()

    comp_q = select(Competition.id, Competition.name, Competition.year).where(
        Competition.id.in_(comp_ids)
    )
    comp_by_id = {
        int(r.id): (str(r.name), str(r.year))
        for r in session.execute(comp_q).all()
    }

    judge_id_to_label = analytics.get_judge_id_to_identity_label()
    pcs_raw, elem_raw = _load_shard_judge_aggregates_sql(
        session, comp_ids, seg_discipline_ids, by_competition=True
    )
    if not pcs_raw and not elem_raw:
        return _empty_competition_heatmap_df()
    pcs_dict = analytics._merge_competition_judge_stat_dicts_by_identity(
        pcs_raw, judge_id_to_label
    )
    elem_dict = analytics._merge_competition_judge_stat_dicts_by_identity(
        elem_raw, judge_id_to_label
    )

    excess_anomalies = None
    if metric == "excess_anomalies":
        excess_raw = analytics._calculate_all_judge_excess_anomalies(
            year_filter=None,
            competition_ids=None,
            discipline_ids=seg_discipline_ids,
            score_type=score_type,
            by_competition=True,
            competition_scope=competition_scope,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
        )
        excess_anomalies = analytics._merge_excess_map_by_identity(
            excess_raw, judge_id_to_label, by_competition=True
        )

    heatmap_data = []
    for comp_id, judge_name in sorted(
        pcs_dict.keys() | elem_dict.keys(),
        key=lambda k: (k[0], k[1].lower()),
    ):
        comp_name, comp_year = comp_by_id.get(comp_id, ("", ""))
        pcs = pcs_dict.get((comp_id, judge_name), {})
        elem = elem_dict.get((comp_id, judge_name), {})
        exc = 0
        if excess_anomalies is not None:
            exc = int(excess_anomalies.get((judge_name, comp_id), 0))
        value = _metric_value(metric, score_type, pcs=pcs, elem=elem, excess=exc)
        if value is None:
            continue
        if metric in ("rule_errors", "excess_anomalies") and value == 0:
            continue
        total_scores = int(pcs.get("total", 0) or 0) + int(elem.get("total", 0) or 0)
        if score_type == "pcs":
            total_scores = int(pcs.get("total", 0) or 0)
        elif score_type == "element":
            total_scores = int(elem.get("total", 0) or 0)
        heatmap_data.append(
            {
                "judge_name": judge_name,
                "competition": f"{comp_name} ({comp_year})",
                "metric_value": round(value, 4)
                if metric == "avg_deviation"
                else round(value, 2),
                "total_scores": total_scores,
            }
        )

    return pd.DataFrame(heatmap_data)


def assemble_pooled_cross_judge_metrics(
    analytics,
    *,
    score_type: str,
    year_filter=None,
    competition_ids=None,
    discipline_type_ids=None,
    competition_scope: str,
    include_excess: bool = True,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
) -> dict[str, Any] | None:
    session = analytics.session
    if not shard_cache_populated(session):
        return None
    core_disc = analytics._qualifying_core_disciplines_active(competition_scope)
    seg_discipline_ids = analytics._merged_segment_discipline_ids(
        core_disc, discipline_type_ids
    )
    comp_ids = _scoped_competition_ids(
        analytics,
        year_filter=year_filter,
        competition_ids=competition_ids,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
    )
    if not comp_ids:
        return {
            "total_scores": 0,
            "throwouts": 0,
            "throwout_rate_pct": 0.0,
            "anomalies": 0,
            "anomaly_rate_pct": 0.0,
            "rule_errors": 0,
            "rule_error_rate_pct": 0.0,
            "avg_abs_deviation": 0.0,
            "total_excess_anomalies": 0,
            "pcs_scores": 0,
            "element_scores": 0,
        }

    totals = _load_shard_pooled_totals_sql(session, comp_ids, seg_discipline_ids)
    pn = totals["pn"]
    pthr = totals["pthr"]
    panom = totals["panom"]
    pre = totals["pre"]
    pabs = totals["pabs"]
    en = totals["en"]
    ethr = totals["ethr"]
    eanom = totals["eanom"]
    ere = totals["ere"]
    eabs = totals["eabs"]

    if score_type == "pcs":
        total_scores, throwouts, anomalies, rule_errors = pn, pthr, panom, pre
        avg_abs_pool = (pabs / pn) if pn else 0.0
    elif score_type == "element":
        total_scores, throwouts, anomalies, rule_errors = en, ethr, eanom, ere
        avg_abs_pool = (eabs / en) if en else 0.0
    else:
        total_scores = pn + en
        throwouts = pthr + ethr
        anomalies = panom + eanom
        rule_errors = pre + ere
        avg_abs_pool = ((pabs + eabs) / total_scores) if total_scores else 0.0

    total_excess = 0
    if include_excess:
        excess_raw = analytics._calculate_all_judge_excess_anomalies(
            year_filter=year_filter,
            competition_ids=competition_ids,
            discipline_ids=seg_discipline_ids,
            score_type=score_type,
            by_competition=False,
            competition_scope=competition_scope,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
        )
        id_to_label = analytics.get_judge_id_to_identity_label()
        excess_map = analytics._merge_excess_map_by_identity(
            excess_raw, id_to_label, by_competition=False
        )
        total_excess = int(sum(excess_map.values()))

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
            "pcs_scores": int(pn),
            "element_scores": int(en),
        }

    return {
        "total_scores": int(total_scores),
        "throwouts": int(throwouts),
        "throwout_rate_pct": (throwouts / total_scores) * 100,
        "anomalies": int(anomalies),
        "anomaly_rate_pct": (anomalies / total_scores) * 100,
        "rule_errors": int(rule_errors),
        "rule_error_rate_pct": (rule_errors / total_scores) * 100,
        "avg_abs_deviation": round(avg_abs_pool, 6),
        "total_excess_anomalies": total_excess,
        "pcs_scores": int(pn),
        "element_scores": int(en),
    }
