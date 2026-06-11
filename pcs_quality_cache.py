"""
Database-backed shard cache for PCS quality analysis.

**Shard cache**: one row per (season, discipline, competition scope) with pickled marks.

**Summary cache**: mergeable per-judge×component stats per shard (cache-only reads
without loading raw marks).
"""

from __future__ import annotations

import hashlib
import json
import logging
import pickle
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from analytics import JudgeAnalytics
from database import ensure_orm_tables
from models import Competition, PcsQualityShardCache, PcsQualityShardSummaryCache
from pcs_quality_analysis import (
    PcsQualityShard,
    PCS_SHARD_MARK_COLUMNS,
    compute_mergeable_component_detail_from_marks,
    compute_pcs_quality_data_fingerprint,
    discipline_ids_for_pcs_quality,
    iter_pcs_quality_shards,
    load_pcs_quality_marks_for_shard,
    merge_mergeable_component_details,
    normalize_pcs_shard_marks,
    pcs_quality_result_from_component_detail,
    season_years_in_pcs_run_range,
)

_log = logging.getLogger(__name__)


def shard_cache_key(shard: PcsQualityShard) -> str:
    payload = json.dumps(
        {
            "season_year": shard.season_year,
            "discipline_type_id": shard.discipline_type_id,
            "competition_scope": shard.competition_scope,
            "event_start_iso": shard.event_start_iso,
            "event_end_iso": shard.event_end_iso,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _require_postgres(bind: Engine) -> None:
    if bind.dialect.name != "postgresql":
        raise RuntimeError(
            f"PCS quality shard cache requires PostgreSQL (got {bind.dialect.name})."
        )


def ensure_pcs_quality_cache_tables(session: Session) -> None:
    ensure_orm_tables(
        session,
        PcsQualityShardCache.__table__,
        PcsQualityShardSummaryCache.__table__,
    )


def _shard_fingerprint(
    session: Session, analytics: JudgeAnalytics, shard: PcsQualityShard
) -> str:
    event_start = (
        date.fromisoformat(shard.event_start_iso) if shard.event_start_iso else None
    )
    event_end = date.fromisoformat(shard.event_end_iso) if shard.event_end_iso else None
    return compute_pcs_quality_data_fingerprint(
        session,
        analytics,
        start_season_year=shard.season_year,
        end_season_year=shard.season_year,
        event_start_date=event_start,
        event_end_date=event_end,
        discipline_type_ids=[shard.discipline_type_id],
        competition_scope=shard.competition_scope,
    )


def _load_shard_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsQualityShard,
    *,
    validate_fingerprint: bool = True,
) -> pd.DataFrame | None:
    row = session.get(PcsQualityShardCache, shard_cache_key(shard))
    if row is None:
        return None
    if validate_fingerprint:
        expected = _shard_fingerprint(session, analytics, shard)
        if row.data_fingerprint != expected:
            session.expunge(row)
            return None
    try:
        df = pickle.loads(row.marks_payload)
    except Exception:
        session.expunge(row)
        return None
    if not isinstance(df, pd.DataFrame):
        return None
    return normalize_pcs_shard_marks(df)


def _save_shard_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsQualityShard,
    marks: pd.DataFrame,
) -> None:
    _require_postgres(session.get_bind())
    key = shard_cache_key(shard)
    payload = pickle.dumps(marks, protocol=pickle.HIGHEST_PROTOCOL)
    fingerprint = _shard_fingerprint(session, analytics, shard)
    now = datetime.now(timezone.utc)
    row = {
        "shard_key": key,
        "season_year": shard.season_year,
        "discipline_type_id": shard.discipline_type_id,
        "competition_scope": shard.competition_scope,
        "event_start_iso": shard.event_start_iso,
        "event_end_iso": shard.event_end_iso,
        "data_fingerprint": fingerprint,
        "marks_payload": payload,
        "n_marks": len(marks),
        "computed_at": now,
    }
    write_session = sessionmaker(bind=session.get_bind())()
    try:
        existing = write_session.get(PcsQualityShardCache, key)
        if existing:
            for k, v in row.items():
                setattr(existing, k, v)
        else:
            write_session.add(PcsQualityShardCache(**row))
        write_session.commit()
    except Exception:
        write_session.rollback()
        raise
    finally:
        write_session.close()


def _load_shard_summary_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsQualityShard,
    *,
    validate_fingerprint: bool = True,
) -> pd.DataFrame | None:
    sk = shard_cache_key(shard)
    row = session.get(PcsQualityShardSummaryCache, sk)
    if row is None:
        return None
    if validate_fingerprint:
        expected = _shard_fingerprint(session, analytics, shard)
        if row.data_fingerprint != expected:
            session.expunge(row)
            return None
    try:
        payload = pickle.loads(row.summary_payload)
    except Exception:
        session.expunge(row)
        return None
    if not isinstance(payload, pd.DataFrame):
        return None
    return payload


def _save_shard_summary_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsQualityShard,
    mergeable_detail: pd.DataFrame,
    *,
    n_marks: int,
) -> None:
    _require_postgres(session.get_bind())
    sk = shard_cache_key(shard)
    fingerprint = _shard_fingerprint(session, analytics, shard)
    payload = pickle.dumps(mergeable_detail, protocol=pickle.HIGHEST_PROTOCOL)
    now = datetime.now(timezone.utc)
    row = {
        "cache_key": sk,
        "shard_key": sk,
        "data_fingerprint": fingerprint,
        "summary_payload": payload,
        "n_marks": n_marks,
        "computed_at": now,
    }
    write_session = sessionmaker(bind=session.get_bind())()
    try:
        existing = write_session.get(PcsQualityShardSummaryCache, sk)
        if existing:
            for k, v in row.items():
                setattr(existing, k, v)
        else:
            write_session.add(PcsQualityShardSummaryCache(**row))
        write_session.commit()
    except Exception:
        write_session.rollback()
        raise
    finally:
        write_session.close()


def _pcs_scope_kwargs(
    *,
    start_season_year: str | None,
    end_season_year: str | None,
    event_start_date: date | None,
    event_end_date: date | None,
    discipline_type_ids: list[int] | None,
    competition_scope: str,
) -> dict[str, Any]:
    return {
        "start_season_year": start_season_year,
        "end_season_year": end_season_year,
        "event_start_date": event_start_date,
        "event_end_date": event_end_date,
        "discipline_type_ids": discipline_type_ids,
        "competition_scope": competition_scope,
    }


def assemble_pcs_quality_from_summaries(
    analytics: JudgeAnalytics,
    *,
    start_season_year: str | None = None,
    end_season_year: str | None = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: list[int] | None = None,
    competition_scope: str,
) -> dict[str, Any] | None:
    """Build PCS quality results from per-shard summary rows only."""
    session = analytics.session
    scope = _pcs_scope_kwargs(
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
    )
    shards = iter_pcs_quality_shards(analytics, **scope)
    if not shards:
        return pcs_quality_result_from_component_detail(pd.DataFrame(), n_raw_marks=0)

    parts: list[pd.DataFrame] = []
    n_raw = 0
    for shard in shards:
        detail = _load_shard_summary_row(
            session, analytics, shard, validate_fingerprint=False
        )
        if detail is None:
            return None
        if not detail.empty:
            parts.append(detail)
        n_raw += int(detail["n_marks"].sum()) if not detail.empty else 0

    merged = merge_mergeable_component_details(parts)
    result = pcs_quality_result_from_component_detail(merged, n_raw_marks=n_raw)
    result["_from_summary_cache"] = True
    return result


def persist_pcs_shard_summaries_for_scope(
    session: Session,
    analytics: JudgeAnalytics,
    judge_id_to_identity: dict[int, str],
    *,
    start_season_year: str | None = None,
    end_season_year: str | None = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: list[int] | None = None,
    competition_scope: str,
) -> int:
    """Warm summary rows for each shard in scope. Returns shards written."""
    ensure_pcs_quality_cache_tables(session)
    scope = _pcs_scope_kwargs(
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
    )
    n = 0
    for shard in iter_pcs_quality_shards(analytics, **scope):
        marks = _load_shard_row(session, analytics, shard)
        if marks is None:
            marks = load_pcs_quality_marks_for_shard(analytics, shard)
        mergeable = compute_mergeable_component_detail_from_marks(
            marks, judge_id_to_identity
        )
        _save_shard_summary_row(
            session, analytics, shard, mergeable, n_marks=len(marks)
        )
        n += 1
    return n


def collect_pcs_marks_for_run(
    analytics: JudgeAnalytics,
    *,
    start_season_year: str | None = None,
    end_season_year: str | None = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: list[int] | None = None,
    competition_scope: str,
    cache_only: bool = False,
    persist_shards: bool = False,
) -> pd.DataFrame | None:
    """
    Load PCS marks for every (season × discipline) shard; optionally read/write cache.

    Returns ``None`` when ``cache_only=True`` and any required shard is missing or stale.
    """
    session = analytics.session
    if persist_shards or not cache_only:
        ensure_pcs_quality_cache_tables(session)
    shards = iter_pcs_quality_shards(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
    )
    if not shards:
        return pd.DataFrame(columns=list(PCS_SHARD_MARK_COLUMNS))

    parts: list[pd.DataFrame] = []
    for shard in shards:
        marks = _load_shard_row(
            session, analytics, shard, validate_fingerprint=not cache_only
        )
        if marks is None:
            if cache_only:
                return None
            marks = load_pcs_quality_marks_for_shard(analytics, shard)
            if persist_shards:
                _save_shard_row(session, analytics, shard, marks)
        if not marks.empty:
            parts.append(marks)

    if not parts:
        return pd.DataFrame(columns=list(PCS_SHARD_MARK_COLUMNS))
    return pd.concat(parts, ignore_index=True)


def load_cached_pcs_quality(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    start_season_year: str | None = None,
    end_season_year: str | None = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: list[int] | None = None,
    competition_scope: str,
) -> dict | None:
    """Assemble PCS quality results from summary cache only."""
    from pcs_quality_analysis import apply_min_pcs_marks_to_result, run_pcs_quality_analysis

    result = run_pcs_quality_analysis(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        cache_only=True,
        persist_shards=False,
    )
    if result.get("error"):
        return None
    return apply_min_pcs_marks_to_result(result, 0)


def precompute_pcs_quality_shards(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    competition_scope: str,
    season_years: list[str] | None = None,
    discipline_type_ids: list[int] | None = None,
) -> int:
    """Warm shard cache for each season × discipline. Returns shards written."""
    ensure_pcs_quality_cache_tables(session)
    years = season_years or season_years_in_pcs_run_range(
        None, None, [str(y) for y in analytics.get_years()]
    )
    disc_ids = discipline_ids_for_pcs_quality(
        analytics, discipline_type_ids, competition_scope
    )
    n = 0
    for sy in years:
        for dt_id in disc_ids:
            shard = PcsQualityShard(
                season_year=sy,
                discipline_type_id=dt_id,
                competition_scope=competition_scope,
            )
            marks = load_pcs_quality_marks_for_shard(analytics, shard)
            _save_shard_row(session, analytics, shard, marks)
            n += 1
            print(f"  shard {sy} discipline_id={dt_id}: {len(marks):,} marks")
    return n


def precompute_pcs_quality_summaries(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    competition_scope: str,
    season_years: list[str] | None = None,
    discipline_type_ids: list[int] | None = None,
) -> int:
    """Warm summary cache for each season × discipline. Returns shards written."""
    id_map = analytics.get_judge_id_to_identity_label()
    years = season_years or season_years_in_pcs_run_range(
        None, None, [str(y) for y in analytics.get_years()]
    )
    start_sy = years[-1] if years else None
    end_sy = years[0] if years else None
    n = persist_pcs_shard_summaries_for_scope(
        session,
        analytics,
        id_map,
        start_season_year=start_sy,
        end_season_year=end_sy,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
    )
    return n


def invalidate_pcs_quality_cache_for_competition(
    session: Session, competition_id: int
) -> int:
    ensure_pcs_quality_cache_tables(session)
    year = session.execute(
        select(Competition.year).where(Competition.id == competition_id)
    ).scalar_one_or_none()
    if not year:
        return 0
    year_s = str(year)
    shard_keys_subq = select(PcsQualityShardCache.shard_key).where(
        PcsQualityShardCache.season_year == year_s
    )
    summary_del = session.execute(
        delete(PcsQualityShardSummaryCache).where(
            PcsQualityShardSummaryCache.shard_key.in_(shard_keys_subq)
        )
    )
    shard_del = session.execute(
        delete(PcsQualityShardCache).where(PcsQualityShardCache.season_year == year_s)
    )
    session.flush()
    return int(summary_del.rowcount or 0) + int(shard_del.rowcount or 0)
