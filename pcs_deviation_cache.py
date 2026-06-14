"""
Database-backed cache for PCS deviation ranking.

**Shard cache**: one row per (season, discipline, competition scope, event dates,
segment level preset) with pickled PCS marks.

**σ̂ cache**: fitted bin parameters for a benchmark season window.

**Summary shard cache**: mergeable per-judge stats per season×discipline at a fixed σ̂ fit.
"""

from __future__ import annotations

import hashlib
import json
import logging
import pickle
from datetime import date, datetime, timezone
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import and_, delete, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from analytics import JudgeAnalytics
from database import ensure_orm_tables
from element_deviation_ranking import (
    ELEMENT_RANKING_LEVEL_FILTER_ALL,
    merge_mergeable_judge_summaries,
)
from models import (
    Competition,
    PcsDeviationRankingShardCache,
    PcsDeviationRankingShardSummaryCache,
    PcsDeviationRankingSigmaCache,
)
from pcs_deviation_analysis import (
    FLOOR_SIGMA,
    MIN_BIN_COUNT,
    PCS_DEVIATION_SHARD_MARK_COLUMNS,
    PcsDeviationShard,
    annotate_normalized_marks_pcs,
    apply_min_marks_to_pcs_deviation_result,
    benchmark_competition_scope,
    benchmark_scope_kwargs_from_run_params,
    benchmark_season_bounds,
    benchmark_segment_level_preset,
    build_ranking_display_table_pcs,
    build_sigma_bins_dataframe_pcs,
    compute_errors,
    compute_mergeable_judge_summary_pcs,
    compute_pcs_deviation_data_fingerprint,
    discipline_ids_for_pcs_deviation,
    finish_pcs_deviation_rankings_from_marks,
    fit_sigma_params_from_marks,
    iter_pcs_deviation_shards,
    load_judge_identity_map,
    load_pcs_deviation_marks,
    marking_score_summary_pcs,
    normalize_pcs_deviation_shard_marks,
    ranking_scope_kwargs_from_run_params,
    run_params_benchmark_compute_key,
    season_years_in_run_range,
    unpack_pcs_deviation_run_params,
    uses_separate_benchmark_pool,
)

_log = logging.getLogger(__name__)

_BENCHMARK_SEGMENT_LEVEL_UNSET = object()


def benchmark_sigma_cache_key(run_params: tuple) -> str:
    payload = json.dumps(
        run_params_benchmark_compute_key(run_params), sort_keys=True, default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def shard_cache_key(shard: PcsDeviationShard) -> str:
    payload = json.dumps(
        {
            "season_year": shard.season_year,
            "discipline_type_id": shard.discipline_type_id,
            "competition_scope": shard.competition_scope,
            "event_start_iso": shard.event_start_iso,
            "event_end_iso": shard.event_end_iso,
            "segment_level_preset": shard.segment_level_preset
            or ELEMENT_RANKING_LEVEL_FILTER_ALL,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def shard_summary_cache_key(shard_key: str, sigma_key: str, floor_sigma: float) -> str:
    payload = json.dumps(
        {"shard_key": shard_key, "sigma_key": sigma_key, "floor_sigma": float(floor_sigma)},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _require_postgres(bind: Engine) -> None:
    if bind.dialect.name != "postgresql":
        raise RuntimeError(
            f"PCS deviation cache requires PostgreSQL (got {bind.dialect.name})."
        )


def ensure_pcs_deviation_cache_tables(session: Session) -> None:
    ensure_orm_tables(
        session,
        PcsDeviationRankingShardCache.__table__,
        PcsDeviationRankingShardSummaryCache.__table__,
        PcsDeviationRankingSigmaCache.__table__,
    )


def _shard_fingerprint(
    session: Session, analytics: JudgeAnalytics, shard: PcsDeviationShard
) -> str:
    from pcs_deviation_analysis import segment_levels_for_ranking_preset

    event_start = (
        date.fromisoformat(shard.event_start_iso) if shard.event_start_iso else None
    )
    event_end = date.fromisoformat(shard.event_end_iso) if shard.event_end_iso else None
    return compute_pcs_deviation_data_fingerprint(
        session,
        analytics,
        start_season_year=shard.season_year,
        end_season_year=shard.season_year,
        event_start_date=event_start,
        event_end_date=event_end,
        discipline_type_ids=[shard.discipline_type_id],
        segment_levels=segment_levels_for_ranking_preset(shard.segment_level_preset),
        competition_scope=shard.competition_scope,
    )


def _benchmark_pool_fingerprint(
    session: Session, analytics: JudgeAnalytics, scope: dict[str, Any]
) -> str:
    from pcs_deviation_analysis import segment_levels_for_ranking_preset

    shards = iter_pcs_deviation_shards(analytics, **scope)
    if not shards:
        preset = scope.get("segment_level_preset")
        fp_scope = {k: v for k, v in scope.items() if k != "segment_level_preset"}
        fp_scope["segment_levels"] = segment_levels_for_ranking_preset(preset)
        return compute_pcs_deviation_data_fingerprint(session, analytics, **fp_scope)
    parts = sorted(_shard_fingerprint(session, analytics, s) for s in shards)
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:64]


def _load_marks_from_db(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsDeviationShard,
    *,
    judge_ids: Iterable[int] | None = None,
) -> pd.DataFrame:
    event_start = (
        date.fromisoformat(shard.event_start_iso) if shard.event_start_iso else None
    )
    event_end = date.fromisoformat(shard.event_end_iso) if shard.event_end_iso else None
    df = load_pcs_deviation_marks(
        analytics,
        start_season_year=shard.season_year,
        end_season_year=shard.season_year,
        event_start_date=event_start,
        event_end_date=event_end,
        discipline_type_ids=[shard.discipline_type_id],
        competition_scope=shard.competition_scope,
        segment_level_preset=shard.segment_level_preset,
        judge_ids=judge_ids,
    )
    if df.empty:
        return pd.DataFrame(columns=list(PCS_DEVIATION_SHARD_MARK_COLUMNS))
    return normalize_pcs_deviation_shard_marks(df, analytics)


def _normalize_shard_marks(
    df: pd.DataFrame,
    analytics: JudgeAnalytics,
    *,
    id_map: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=list(PCS_DEVIATION_SHARD_MARK_COLUMNS))
    return normalize_pcs_deviation_shard_marks(df, analytics, id_map=id_map)


def _load_shard_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsDeviationShard,
    *,
    validate_fingerprint: bool = True,
    id_map: pd.DataFrame | None = None,
) -> pd.DataFrame | None:
    row = session.get(PcsDeviationRankingShardCache, shard_cache_key(shard))
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
    return _normalize_shard_marks(df, analytics, id_map=id_map)


def _save_shard_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsDeviationShard,
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
        existing = write_session.get(PcsDeviationRankingShardCache, key)
        if existing:
            for k, v in row.items():
                setattr(existing, k, v)
        else:
            write_session.add(PcsDeviationRankingShardCache(**row))
        write_session.commit()
    except Exception:
        write_session.rollback()
        raise
    finally:
        write_session.close()


def _shard_cache_is_fresh(
    session: Session, analytics: JudgeAnalytics, shard: PcsDeviationShard
) -> bool:
    row = session.get(PcsDeviationRankingShardCache, shard_cache_key(shard))
    if row is None:
        return False
    return row.data_fingerprint == _shard_fingerprint(session, analytics, shard)


def _sigma_cache_is_fresh(
    session: Session, analytics: JudgeAnalytics, run_params: tuple
) -> bool:
    return (
        _load_sigma_cache_row(
            session, analytics, run_params, validate_fingerprint=True
        )
        is not None
    )


def _shard_summary_cache_is_fresh(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsDeviationShard,
    run_params: tuple,
    *,
    floor_sigma: float,
) -> bool:
    sk = shard_cache_key(shard)
    sigma_key = benchmark_sigma_cache_key(run_params)
    key = shard_summary_cache_key(sk, sigma_key, floor_sigma)
    row = session.get(PcsDeviationRankingShardSummaryCache, key)
    if row is None:
        return False
    if float(row.floor_sigma) != float(floor_sigma):
        return False
    return row.data_fingerprint == _shard_fingerprint(session, analytics, shard)


def collect_marks_for_run(
    analytics: JudgeAnalytics,
    *,
    start_season_year: str | None = None,
    end_season_year: str | None = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: list[int] | None = None,
    competition_scope: str,
    segment_level_preset: str | None = None,
    cache_only: bool = False,
    persist_shards: bool = False,
) -> pd.DataFrame | None:
    session = analytics.session
    if persist_shards or not cache_only:
        ensure_pcs_deviation_cache_tables(session)
    shards = iter_pcs_deviation_shards(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        segment_level_preset=segment_level_preset,
    )
    if not shards:
        return pd.DataFrame()

    parts: list[pd.DataFrame] = []
    for shard in shards:
        marks = _load_shard_row(
            session, analytics, shard, validate_fingerprint=not cache_only
        )
        if marks is None:
            if cache_only:
                return None
            marks = _load_marks_from_db(session, analytics, shard)
            if persist_shards:
                _save_shard_row(session, analytics, shard, marks)
        if not marks.empty:
            parts.append(marks)

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def collect_marks_for_judge_detail(
    analytics: JudgeAnalytics,
    judge_ids: Iterable[int],
    *,
    start_season_year: str | None = None,
    end_season_year: str | None = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: list[int] | None = None,
    competition_scope: str,
    segment_level_preset: str | None = None,
) -> pd.DataFrame:
    """
    PCS marks for one identity.

    Prefer a single judge-scoped SQL load (fast for drill-down). Fall back to cached
    shards only when SQL returns no rows but shard pickles exist.
    """
    judge_id_set = {int(j) for j in judge_ids}
    if not judge_id_set:
        return pd.DataFrame()

    sql_df = load_pcs_deviation_marks(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        segment_level_preset=segment_level_preset,
        judge_ids=judge_id_set,
    )
    if not sql_df.empty:
        return normalize_pcs_deviation_shard_marks(sql_df, analytics)

    session = analytics.session
    shards = iter_pcs_deviation_shards(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        segment_level_preset=segment_level_preset,
    )
    if not shards:
        return pd.DataFrame()

    parts: list[pd.DataFrame] = []
    missing_shard = False
    for shard in shards:
        marks = _load_shard_row(
            session, analytics, shard, validate_fingerprint=False
        )
        if marks is None:
            missing_shard = True
            break
        if marks.empty:
            continue
        filtered = marks.loc[marks["judge_id"].isin(judge_id_set)]
        if not filtered.empty:
            parts.append(filtered)

    if not missing_shard:
        if parts:
            return pd.concat(parts, ignore_index=True)
        return pd.DataFrame()

    return pd.DataFrame()


def load_cached_sigma_params_for_run(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
) -> dict | None:
    """σ̂ lookup table for drill-down when the packaged result omits ``params``."""
    return _load_sigma_cache_row(
        session, analytics, run_params, validate_fingerprint=False
    )


def _load_sigma_cache_row(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    *,
    validate_fingerprint: bool = True,
) -> dict | None:
    key = benchmark_sigma_cache_key(run_params)
    row = session.get(PcsDeviationRankingSigmaCache, key)
    if row is None:
        return None
    if validate_fingerprint:
        bench_scope = benchmark_scope_kwargs_from_run_params(run_params)
        expected = _benchmark_pool_fingerprint(session, analytics, bench_scope)
        if row.data_fingerprint != expected:
            session.expunge(row)
            return None
    try:
        params = pickle.loads(row.params_payload)
    except Exception:
        session.expunge(row)
        return None
    if not isinstance(params, dict):
        return None
    return params


def _save_sigma_cache_row(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    params: dict,
    *,
    n_marks: int,
) -> None:
    _require_postgres(session.get_bind())
    rp = unpack_pcs_deviation_run_params(run_params)
    bench_scope = benchmark_scope_kwargs_from_run_params(run_params)
    bs, be = benchmark_season_bounds(run_params)
    key = benchmark_sigma_cache_key(run_params)
    fingerprint = _benchmark_pool_fingerprint(session, analytics, bench_scope)
    now = datetime.now(timezone.utc)
    row = {
        "sigma_key": key,
        "benchmark_start_season_year": bs,
        "benchmark_end_season_year": be,
        "scope_json": json.dumps(run_params_benchmark_compute_key(run_params), default=str),
        "data_fingerprint": fingerprint,
        "params_payload": pickle.dumps(params, protocol=pickle.HIGHEST_PROTOCOL),
        "floor_sigma": float(rp[7]),
        "min_bin_count": int(rp[8]),
        "n_marks": n_marks,
        "computed_at": now,
    }
    write_session = sessionmaker(bind=session.get_bind())()
    try:
        existing = write_session.get(PcsDeviationRankingSigmaCache, key)
        if existing:
            for k, v in row.items():
                setattr(existing, k, v)
        else:
            write_session.add(PcsDeviationRankingSigmaCache(**row))
        write_session.commit()
    except Exception:
        write_session.rollback()
        raise
    finally:
        write_session.close()


def get_or_fit_benchmark_sigma_params(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    *,
    cache_only: bool = False,
    persist_shards: bool = True,
    persist_sigma: bool = True,
) -> tuple[dict, pd.DataFrame | None, bool] | None:
    rp = unpack_pcs_deviation_run_params(run_params)
    bench_scope = benchmark_scope_kwargs_from_run_params(run_params)
    separate = uses_separate_benchmark_pool(run_params)

    cached = _load_sigma_cache_row(
        session, analytics, run_params, validate_fingerprint=not cache_only
    )
    if cached is not None:
        sigma_ref = None
        if separate:
            sigma_ref = collect_marks_for_run(
                analytics,
                **bench_scope,
                cache_only=cache_only,
                persist_shards=persist_shards and not cache_only,
            )
            if sigma_ref is None:
                return None
        return cached, sigma_ref, True

    if cache_only:
        return None

    bench_marks = collect_marks_for_run(
        analytics,
        **bench_scope,
        cache_only=False,
        persist_shards=persist_shards,
    )
    if bench_marks is None or bench_marks.empty:
        return {}, pd.DataFrame() if bench_marks is not None else None, False

    params = fit_sigma_params_from_marks(bench_marks, min_bin_count=int(rp[8]))
    if persist_sigma and params:
        _save_sigma_cache_row(
            session, analytics, run_params, params, n_marks=len(bench_marks)
        )
    sigma_ref = bench_marks if separate else None
    return params, sigma_ref, False


def _parse_shard_summary_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsDeviationShard,
    row: PcsDeviationRankingShardSummaryCache,
    *,
    floor_sigma: float,
    validate_fingerprint: bool,
) -> dict[str, Any] | None:
    if float(row.floor_sigma) != float(floor_sigma):
        session.expunge(row)
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
    if not isinstance(payload, dict):
        return None
    return payload


def load_shard_summary_payloads_for_scope(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    *,
    validate_fingerprint: bool = False,
    require_all: bool = True,
) -> list[tuple[PcsDeviationShard, dict[str, Any]]] | None:
    rank_scope = ranking_scope_kwargs_from_run_params(run_params)
    rp = unpack_pcs_deviation_run_params(run_params)
    floor_sigma = float(rp[7])
    sigma_key = benchmark_sigma_cache_key(run_params)
    shards = iter_pcs_deviation_shards(analytics, **rank_scope)
    if not shards:
        return [] if not require_all else None

    shard_entries = [
        (
            shard,
            shard_summary_cache_key(shard_cache_key(shard), sigma_key, floor_sigma),
        )
        for shard in shards
    ]
    cache_keys = [ck for _, ck in shard_entries]
    rows = (
        session.execute(
            select(PcsDeviationRankingShardSummaryCache).where(
                PcsDeviationRankingShardSummaryCache.cache_key.in_(cache_keys)
            )
        )
        .scalars()
        .all()
    )
    by_cache_key = {row.cache_key: row for row in rows}

    out: list[tuple[PcsDeviationShard, dict[str, Any]]] = []
    for shard, ck in shard_entries:
        row = by_cache_key.get(ck)
        if row is None:
            if require_all:
                return None
            continue
        payload = _parse_shard_summary_row(
            session,
            analytics,
            shard,
            row,
            floor_sigma=floor_sigma,
            validate_fingerprint=validate_fingerprint,
        )
        if payload is None:
            if require_all:
                return None
            continue
        out.append((shard, payload))

    if require_all and len(out) != len(shard_entries):
        return None
    return out


def _save_shard_summary_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: PcsDeviationShard,
    *,
    sigma_key: str,
    floor_sigma: float,
    mergeable_summary: pd.DataFrame,
    n_marks: int,
) -> None:
    _require_postgres(session.get_bind())
    sk = shard_cache_key(shard)
    key = shard_summary_cache_key(sk, sigma_key, floor_sigma)
    fingerprint = _shard_fingerprint(session, analytics, shard)
    payload = pickle.dumps(
        {"mergeable_summary": mergeable_summary},
        protocol=pickle.HIGHEST_PROTOCOL,
    )
    now = datetime.now(timezone.utc)
    row = {
        "cache_key": key,
        "shard_key": sk,
        "sigma_key": sigma_key,
        "floor_sigma": float(floor_sigma),
        "data_fingerprint": fingerprint,
        "summary_payload": payload,
        "n_marks": n_marks,
        "computed_at": now,
    }
    write_session = sessionmaker(bind=session.get_bind())()
    try:
        existing = write_session.get(PcsDeviationRankingShardSummaryCache, key)
        if existing:
            for k, v in row.items():
                setattr(existing, k, v)
        else:
            write_session.add(PcsDeviationRankingShardSummaryCache(**row))
        write_session.commit()
    except Exception:
        write_session.rollback()
        raise
    finally:
        write_session.close()


def _persist_shard_summaries_for_scope(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    params: dict,
    rank_scope: dict[str, Any],
    *,
    floor_sigma: float,
    cache_only_marks: bool = False,
    skip_unchanged: bool = False,
) -> tuple[int, int]:
    if not params:
        return 0, 0
    ensure_pcs_deviation_cache_tables(session)
    sigma_key = benchmark_sigma_cache_key(run_params)
    id_map = load_judge_identity_map(analytics)
    written = 0
    skipped = 0
    for shard in iter_pcs_deviation_shards(analytics, **rank_scope):
        if skip_unchanged and _shard_summary_cache_is_fresh(
            session, analytics, shard, run_params, floor_sigma=floor_sigma
        ):
            skipped += 1
            continue
        marks = _load_shard_row(session, analytics, shard, id_map=id_map)
        if marks is None:
            if cache_only_marks:
                continue
            marks = _load_marks_from_db(session, analytics, shard)
        if marks.empty:
            continue
        work = annotate_normalized_marks_pcs(
            compute_errors(marks), params, floor_sigma=floor_sigma
        )
        mergeable = compute_mergeable_judge_summary_pcs(work)
        _save_shard_summary_row(
            session,
            analytics,
            shard,
            sigma_key=sigma_key,
            floor_sigma=floor_sigma,
            mergeable_summary=mergeable,
            n_marks=len(marks),
        )
        written += 1
    return written, skipped


def _empty_ranking_error(message: str) -> dict[str, Any]:
    return {
        "marking": pd.DataFrame(),
        "summary": pd.DataFrame(),
        "sigma_bins": pd.DataFrame(),
        "judge_discipline_detail": pd.DataFrame(),
        "judge_component_detail": pd.DataFrame(),
        "judge_control_bin_detail": pd.DataFrame(),
        "judge_summary_all": pd.DataFrame(),
        "params": {},
        "n_raw_marks": 0,
        "n_sigma_buckets": 0,
        "error": message,
    }


def _try_assemble_ranking_from_shard_summaries(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    params: dict,
    rank_scope: dict[str, Any],
    *,
    floor_sigma: float,
    min_bin_count: int,
    sigma_reference_df: pd.DataFrame | None,
) -> dict[str, Any] | None:
    if not iter_pcs_deviation_shards(analytics, **rank_scope):
        return None

    shard_payloads = load_shard_summary_payloads_for_scope(
        session,
        analytics,
        run_params,
        validate_fingerprint=False,
        require_all=True,
    )
    if shard_payloads is None:
        return None

    mergeable_parts: list[pd.DataFrame] = []
    n_raw = 0
    for _shard, payload in shard_payloads:
        mergeable = payload.get("mergeable_summary")
        if not isinstance(mergeable, pd.DataFrame):
            return None
        mergeable_parts.append(mergeable)
        n_raw += int(mergeable["n_marks"].sum()) if not mergeable.empty else 0

    judge_summary_all = merge_mergeable_judge_summaries(mergeable_parts)
    if judge_summary_all.empty:
        return _empty_ranking_error("No PCS score rows found for the selected filters.")

    disc_map = {int(i): n for i, n in analytics.get_discipline_types()}
    bins_df = sigma_reference_df if sigma_reference_df is not None else pd.DataFrame()
    sigma_bins = (
        build_sigma_bins_dataframe_pcs(
            compute_errors(bins_df.copy()) if not bins_df.empty else bins_df,
            params,
            disc_map,
            min_bin_count=min_bin_count,
        )
        if not bins_df.empty
        else pd.DataFrame()
    )
    marking = build_ranking_display_table_pcs(judge_summary_all)
    return {
        "marking": marking,
        "summary": marking_score_summary_pcs(judge_summary_all),
        "sigma_bins": sigma_bins,
        "judge_summary_all": judge_summary_all,
        "judge_discipline_detail": pd.DataFrame(),
        "judge_component_detail": pd.DataFrame(),
        "judge_control_bin_detail": pd.DataFrame(),
        "judge_discipline_detail_all": pd.DataFrame(),
        "judge_component_detail_all": pd.DataFrame(),
        "judge_control_bin_detail_all": pd.DataFrame(),
        "params": params,
        "n_raw_marks": n_raw,
        "n_sigma_buckets": len(params),
        "error": None,
        "benchmark_start_season_year": benchmark_season_bounds(run_params)[0],
        "benchmark_end_season_year": benchmark_season_bounds(run_params)[1],
        "benchmark_competition_scope": benchmark_competition_scope(run_params),
        "_from_summary_cache": True,
    }


def run_pcs_deviation_ranking_pipeline(
    analytics: JudgeAnalytics,
    *,
    start_season_year: str | None = None,
    end_season_year: str | None = None,
    event_start_date: date | None = None,
    event_end_date: date | None = None,
    discipline_type_ids: list[int] | None = None,
    competition_scope: str,
    min_marks: int = 0,
    floor_sigma: float,
    min_bin_count: int,
    segment_level_preset: str | None = None,
    benchmark_start_season_year: str | None = None,
    benchmark_end_season_year: str | None = None,
    benchmark_competition_scope_key: str | None = None,
    benchmark_segment_level_preset: str | None | object = _BENCHMARK_SEGMENT_LEVEL_UNSET,
    cache_only: bool = False,
    persist_shards: bool = True,
) -> dict[str, Any]:
    bench_levels = (
        segment_level_preset
        if benchmark_segment_level_preset is _BENCHMARK_SEGMENT_LEVEL_UNSET
        else benchmark_segment_level_preset
    )
    run_params = (
        start_season_year,
        end_season_year,
        tuple(discipline_type_ids) if discipline_type_ids else None,
        competition_scope,
        event_start_date.isoformat() if event_start_date else None,
        event_end_date.isoformat() if event_end_date else None,
        int(min_marks),
        float(floor_sigma),
        int(min_bin_count),
        benchmark_start_season_year,
        benchmark_end_season_year,
        benchmark_competition_scope_key,
        segment_level_preset,
        bench_levels,
    )
    session = analytics.session
    if not cache_only:
        ensure_pcs_deviation_cache_tables(session)
    rank_scope = ranking_scope_kwargs_from_run_params(run_params)
    bs, be = benchmark_season_bounds(run_params)
    bench_scope_key = benchmark_competition_scope(run_params)
    separate = uses_separate_benchmark_pool(run_params)

    cached_params: dict | None = None
    sigma_ref: pd.DataFrame | None = None
    if cache_only:
        if separate:
            sigma_out = get_or_fit_benchmark_sigma_params(
                session,
                analytics,
                run_params,
                cache_only=True,
                persist_shards=False,
                persist_sigma=False,
            )
            if sigma_out is None:
                return _empty_ranking_error(
                    "Missing or stale shard/σ̂ cache for benchmark pool."
                )
            cached_params, sigma_ref, _ = sigma_out
        else:
            cached_params = _load_sigma_cache_row(
                session, analytics, run_params, validate_fingerprint=False
            )
        if cached_params:
            summary_result = _try_assemble_ranking_from_shard_summaries(
                session,
                analytics,
                run_params,
                cached_params,
                rank_scope,
                floor_sigma=floor_sigma,
                min_bin_count=min_bin_count,
                sigma_reference_df=sigma_ref,
            )
            if summary_result is not None:
                return apply_min_marks_to_pcs_deviation_result(
                    summary_result, int(min_marks)
                )

    ranking_marks = collect_marks_for_run(
        analytics,
        **rank_scope,
        cache_only=cache_only,
        persist_shards=persist_shards and not cache_only,
    )
    if ranking_marks is None:
        return _empty_ranking_error("Missing or stale shard cache for ranking scope.")

    params: dict | None
    if separate:
        sigma_out = get_or_fit_benchmark_sigma_params(
            session,
            analytics,
            run_params,
            cache_only=cache_only,
            persist_shards=persist_shards,
            persist_sigma=persist_shards,
        )
        if sigma_out is None:
            return _empty_ranking_error(
                "Missing or stale shard/σ̂ cache for benchmark pool."
            )
        params, sigma_ref, _from_sigma_cache = sigma_out
    else:
        params = _load_sigma_cache_row(session, analytics, run_params)
        if params is None and cache_only:
            params = None
        elif params is None and not cache_only:
            params = fit_sigma_params_from_marks(
                ranking_marks, min_bin_count=int(min_bin_count)
            )
            if persist_shards and params:
                _save_sigma_cache_row(
                    session,
                    analytics,
                    run_params,
                    params,
                    n_marks=len(ranking_marks),
                )
            sigma_ref = None

    if ranking_marks.empty:
        return finish_pcs_deviation_rankings_from_marks(
            analytics, ranking_marks, min_marks=0, params=params or {}
        )

    result = finish_pcs_deviation_rankings_from_marks(
        analytics,
        ranking_marks,
        min_marks=0,
        floor_sigma=floor_sigma,
        min_bin_count=min_bin_count,
        params=params if params else None,
        sigma_reference_df=sigma_ref,
    )
    result["benchmark_start_season_year"] = bs
    result["benchmark_end_season_year"] = be
    result["benchmark_competition_scope"] = bench_scope_key

    if persist_shards and params:
        try:
            _persist_shard_summaries_for_scope(
                session,
                analytics,
                run_params,
                params,
                rank_scope,
                floor_sigma=floor_sigma,
            )
        except Exception:
            _log.exception("Failed to persist PCS deviation shard summaries")

    return apply_min_marks_to_pcs_deviation_result(result, int(min_marks))


def load_cached_pcs_deviation_rankings(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
) -> dict[str, Any] | None:
    result = run_pcs_deviation_ranking_pipeline(
        analytics,
        **ranking_scope_kwargs_from_run_params(run_params),
        min_marks=int(run_params[6] or 0),
        floor_sigma=float(run_params[7]),
        min_bin_count=int(run_params[8]),
        benchmark_start_season_year=benchmark_season_bounds(run_params)[0],
        benchmark_end_season_year=benchmark_season_bounds(run_params)[1],
        benchmark_competition_scope_key=benchmark_competition_scope(run_params),
        benchmark_segment_level_preset=benchmark_segment_level_preset(run_params),
        cache_only=True,
        persist_shards=False,
    )
    if result.get("error"):
        return None
    return result


def save_cached_pcs_deviation_rankings(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    result: dict[str, Any],
) -> None:
    if result.get("error"):
        raise ValueError(f"Cannot cache failed run: {result['error']}")

    rank_scope = ranking_scope_kwargs_from_run_params(run_params)
    bench_scope = benchmark_scope_kwargs_from_run_params(run_params)
    collect_marks_for_run(
        analytics,
        **rank_scope,
        cache_only=False,
        persist_shards=True,
    )
    if uses_separate_benchmark_pool(run_params):
        collect_marks_for_run(
            analytics,
            **bench_scope,
            cache_only=False,
            persist_shards=True,
        )
    params = result.get("params")
    if isinstance(params, dict) and params:
        bench_marks = collect_marks_for_run(
            analytics,
            **bench_scope,
            cache_only=False,
            persist_shards=False,
        )
        _save_sigma_cache_row(
            session,
            analytics,
            run_params,
            params,
            n_marks=len(bench_marks) if bench_marks is not None else 0,
        )
        try:
            _persist_shard_summaries_for_scope(
                session,
                analytics,
                run_params,
                params,
                rank_scope,
                floor_sigma=float(run_params[7]),
            )
        except Exception:
            _log.exception("Failed to persist PCS deviation shard summaries on save")


def try_save_pcs_deviation_cache(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    result: dict[str, Any],
) -> tuple[bool, str | None]:
    try:
        save_cached_pcs_deviation_rankings(session, analytics, run_params, result)
        return True, None
    except Exception as exc:
        _log.exception("Failed to save PCS deviation cache")
        return False, str(exc)


def build_precompute_pcs_deviation_run_params(
    competition_scope: str,
    *,
    segment_level_preset: str | None = None,
) -> tuple:
    return (
        None,
        None,
        None,
        competition_scope,
        None,
        None,
        0,
        float(FLOOR_SIGMA),
        int(MIN_BIN_COUNT),
        None,
        None,
        competition_scope,
        segment_level_preset,
        segment_level_preset,
    )


def precompute_pcs_deviation_shards(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    competition_scope: str,
    season_years: list[str] | None = None,
    discipline_type_ids: list[int] | None = None,
    segment_level_preset: str | None = None,
    skip_unchanged: bool = False,
) -> tuple[int, int]:
    years = season_years or season_years_in_run_range(
        None, None, [str(y) for y in analytics.get_years()]
    )
    disc_ids = discipline_ids_for_pcs_deviation(
        analytics, discipline_type_ids, competition_scope
    )
    written = 0
    skipped = 0
    for sy in years:
        for dt_id in disc_ids:
            shard = PcsDeviationShard(
                season_year=sy,
                discipline_type_id=dt_id,
                competition_scope=competition_scope,
                segment_level_preset=segment_level_preset,
            )
            if skip_unchanged and _shard_cache_is_fresh(session, analytics, shard):
                skipped += 1
                print(f"  shard {sy} discipline_id={dt_id}: skipped (unchanged)")
                continue
            marks = _load_marks_from_db(session, analytics, shard)
            _save_shard_row(session, analytics, shard, marks)
            written += 1
            print(f"  shard {sy} discipline_id={dt_id}: {len(marks):,} marks")
    return written, skipped


def precompute_pcs_deviation_sigma(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    *,
    skip_unchanged: bool = False,
) -> str | None:
    if skip_unchanged and _sigma_cache_is_fresh(session, analytics, run_params):
        return benchmark_sigma_cache_key(run_params)
    sigma_out = get_or_fit_benchmark_sigma_params(
        session,
        analytics,
        run_params,
        cache_only=False,
        persist_shards=True,
        persist_sigma=True,
    )
    if sigma_out is None:
        return None
    params, _, _ = sigma_out
    if not params:
        return None
    return benchmark_sigma_cache_key(run_params)


def precompute_pcs_deviation_shard_summaries(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    *,
    rank_scope: dict[str, Any] | None = None,
    summaries_only: bool = False,
    skip_unchanged: bool = False,
) -> tuple[int, int]:
    sigma_out = get_or_fit_benchmark_sigma_params(
        session,
        analytics,
        run_params,
        cache_only=summaries_only,
        persist_shards=not summaries_only,
        persist_sigma=not summaries_only and not skip_unchanged,
    )
    if sigma_out is None:
        return 0, 0
    params, _, _ = sigma_out
    if not params:
        return 0, 0
    rp = unpack_pcs_deviation_run_params(run_params)
    scope = rank_scope or ranking_scope_kwargs_from_run_params(run_params)
    return _persist_shard_summaries_for_scope(
        session,
        analytics,
        run_params,
        params,
        scope,
        floor_sigma=float(rp[7]),
        cache_only_marks=summaries_only,
        skip_unchanged=skip_unchanged,
    )


def invalidate_pcs_deviation_cache_for_competition(
    session: Session, competition_id: int
) -> int:
    ensure_pcs_deviation_cache_tables(session)
    year = session.execute(
        select(Competition.year).where(Competition.id == competition_id)
    ).scalar_one_or_none()
    if not year:
        return 0
    year_s = str(year)
    shard_keys_subq = select(PcsDeviationRankingShardCache.shard_key).where(
        PcsDeviationRankingShardCache.season_year == year_s
    )
    summary_del = session.execute(
        delete(PcsDeviationRankingShardSummaryCache).where(
            PcsDeviationRankingShardSummaryCache.shard_key.in_(shard_keys_subq)
        )
    )
    shard_del = session.execute(
        delete(PcsDeviationRankingShardCache).where(
            PcsDeviationRankingShardCache.season_year == year_s
        )
    )
    sigma_del = session.execute(
        delete(PcsDeviationRankingSigmaCache).where(
            and_(
                or_(
                    PcsDeviationRankingSigmaCache.benchmark_start_season_year.is_(None),
                    PcsDeviationRankingSigmaCache.benchmark_start_season_year <= year_s,
                ),
                or_(
                    PcsDeviationRankingSigmaCache.benchmark_end_season_year.is_(None),
                    PcsDeviationRankingSigmaCache.benchmark_end_season_year >= year_s,
                ),
            )
        )
    )
    session.flush()
    return (
        int(shard_del.rowcount or 0)
        + int(summary_del.rowcount or 0)
        + int(sigma_del.rowcount or 0)
    )
