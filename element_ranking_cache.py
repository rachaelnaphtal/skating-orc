"""
Database-backed cache for element deviation ranking.

**Shard cache** (primary): one row per (season, discipline, competition scope, event dates)
with pickled element marks. Ranking and σ̂ benchmark pools each concatenate matching shards.

**σ̂ cache**: fitted bin parameters for a benchmark season window (reused when ranking
scope is narrower).

**Full-run cache** (legacy): exact filter-set blob; still checked first for old rows.
"""

from __future__ import annotations

import hashlib
import json
import logging
import pickle
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import and_, delete, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from analytics import JudgeAnalytics
from element_deviation_ranking import (
    ElementRankingShard,
    SHARD_MARK_COLUMNS,
    apply_min_marks_to_ranking_result,
    attach_judge_identities,
    benchmark_scope_kwargs_from_run_params,
    benchmark_competition_scope,
    benchmark_season_bounds,
    compute_element_ranking_data_fingerprint,
    discipline_ids_for_element_ranking,
    finish_element_deviation_rankings_from_marks,
    fit_sigma_params_from_marks,
    iter_element_ranking_shards,
    load_element_marking_data,
    ranking_scope_kwargs_from_run_params,
    run_params_benchmark_compute_key,
    run_params_compute_key,
    run_params_ranking_compute_key,
    season_years_in_run_range,
    unpack_element_ranking_run_params,
    uses_separate_benchmark_pool,
)
from element_deviation_ranking_job import merge_ranking_result_from_storage
from models import (
    Competition,
    ElementDeviationRankingCache,
    ElementDeviationRankingShardCache,
    ElementDeviationRankingSigmaCache,
)

_log = logging.getLogger(__name__)



def run_params_cache_key(run_params: tuple) -> str:
    """Hash of ranking + benchmark scope + model params (min marks excluded)."""
    payload = json.dumps(
        {
            "ranking": run_params_ranking_compute_key(run_params),
            "benchmark": run_params_benchmark_compute_key(run_params),
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def benchmark_sigma_cache_key(run_params: tuple) -> str:
    """Cache key for σ̂ parameters fitted on the benchmark mark pool."""
    payload = json.dumps(
        run_params_benchmark_compute_key(run_params), sort_keys=True, default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def shard_cache_key(shard: ElementRankingShard) -> str:
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


def element_ranking_scope_kwargs(run_params: tuple) -> dict[str, Any]:
    """Ranking mark scope (not the σ̂ benchmark pool)."""
    return ranking_scope_kwargs_from_run_params(run_params)


def element_ranking_filter_kwargs(run_params: tuple) -> dict[str, Any]:
    return {
        **element_ranking_scope_kwargs(run_params),
        "floor_sigma": run_params[7],
    }


def _require_postgres(bind: Engine) -> None:
    if bind.dialect.name != "postgresql":
        raise RuntimeError(
            f"element ranking cache requires PostgreSQL (got {bind.dialect.name})."
        )


def ensure_element_ranking_cache_tables(bind: Engine) -> None:
    ElementDeviationRankingShardCache.__table__.create(bind, checkfirst=True)
    ElementDeviationRankingSigmaCache.__table__.create(bind, checkfirst=True)
    ElementDeviationRankingCache.__table__.create(bind, checkfirst=True)


def _benchmark_pool_fingerprint(
    session: Session, analytics: JudgeAnalytics, scope: dict[str, Any]
) -> str:
    """Combined shard fingerprints for every season×discipline in the benchmark pool."""
    shards = iter_element_ranking_shards(analytics, **scope)
    if not shards:
        return compute_element_ranking_data_fingerprint(session, analytics, **scope)
    parts = sorted(_shard_fingerprint(session, analytics, s) for s in shards)
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return digest[:64]


def _load_sigma_cache_row(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
) -> dict | None:
    ensure_element_ranking_cache_tables(session.get_bind())
    key = benchmark_sigma_cache_key(run_params)
    row = session.get(ElementDeviationRankingSigmaCache, key)
    if row is None:
        return None
    bench_scope = benchmark_scope_kwargs_from_run_params(run_params)
    expected = _benchmark_pool_fingerprint(session, analytics, bench_scope)
    if row.data_fingerprint != expected:
        session.delete(row)
        session.commit()
        return None
    try:
        params = pickle.loads(row.params_payload)
    except Exception:
        session.delete(row)
        session.commit()
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
    ensure_element_ranking_cache_tables(session.get_bind())
    rp = unpack_element_ranking_run_params(run_params)
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
        existing = write_session.get(ElementDeviationRankingSigmaCache, key)
        if existing:
            for k, v in row.items():
                setattr(existing, k, v)
        else:
            write_session.add(ElementDeviationRankingSigmaCache(**row))
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
    """
    Load or fit σ̂ on the benchmark mark pool.

    Returns ``(params, sigma_reference_df, from_cache)`` or ``None`` when
    ``cache_only=True`` and σ̂ or required shards are missing.
    """
    rp = unpack_element_ranking_run_params(run_params)
    bench_scope = benchmark_scope_kwargs_from_run_params(run_params)
    separate = uses_separate_benchmark_pool(run_params)

    cached = _load_sigma_cache_row(session, analytics, run_params)
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
    if bench_marks.empty:
        return {}, bench_marks, False

    params = fit_sigma_params_from_marks(
        bench_marks, min_bin_count=int(rp[8])
    )
    if persist_sigma and params:
        _save_sigma_cache_row(
            session, analytics, run_params, params, n_marks=len(bench_marks)
        )
    sigma_ref = bench_marks if separate else None
    return params, sigma_ref, False


def run_element_deviation_ranking_pipeline(
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
    include_judge_detail: bool | None = None,
    benchmark_start_season_year: str | None = None,
    benchmark_end_season_year: str | None = None,
    benchmark_competition_scope_key: str | None = None,
    cache_only: bool = False,
    persist_shards: bool = True,
) -> dict[str, Any]:
    """Assemble ranking-scope marks; apply σ̂ from benchmark pool (cached when possible)."""
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
    )
    session = analytics.session
    rank_scope = ranking_scope_kwargs_from_run_params(run_params)
    bs, be = benchmark_season_bounds(run_params)
    bench_scope_key = benchmark_competition_scope(run_params)

    ranking_marks = collect_marks_for_run(
        analytics,
        **rank_scope,
        cache_only=cache_only,
        persist_shards=persist_shards and not cache_only,
    )
    if ranking_marks is None:
        return _empty_ranking_error("Missing or stale shard cache for ranking scope.")

    separate = uses_separate_benchmark_pool(run_params)
    params: dict | None
    sigma_ref: pd.DataFrame | None = None
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
            bench_marks = ranking_marks
            params = fit_sigma_params_from_marks(
                bench_marks, min_bin_count=int(min_bin_count)
            )
            if persist_shards and params:
                _save_sigma_cache_row(
                    session,
                    analytics,
                    run_params,
                    params,
                    n_marks=len(bench_marks),
                )

    result = finish_element_deviation_rankings_from_marks(
        analytics,
        ranking_marks,
        min_marks=0,
        floor_sigma=floor_sigma,
        min_bin_count=min_bin_count,
        include_judge_detail=include_judge_detail,
        params=params if params else None,
        sigma_reference_df=sigma_ref,
        benchmark_start_season_year=bs,
        benchmark_end_season_year=be,
        benchmark_competition_scope_key=bench_scope_key,
    )
    return apply_min_marks_to_ranking_result(result, int(min_marks))


def _empty_ranking_error(message: str) -> dict[str, Any]:
    from element_deviation_ranking import memory_efficient_mode

    return {
        "marking": pd.DataFrame(),
        "summary": pd.DataFrame(),
        "sigma_bins": pd.DataFrame(),
        "judge_discipline_detail": pd.DataFrame(),
        "judge_element_detail": pd.DataFrame(),
        "control_by_element": pd.DataFrame(),
        "params": {},
        "n_raw_marks": 0,
        "n_sigma_buckets": 0,
        "error": message,
        "low_memory": memory_efficient_mode(),
    }


def _shard_fingerprint(
    session: Session, analytics: JudgeAnalytics, shard: ElementRankingShard
) -> str:
    event_start = (
        date.fromisoformat(shard.event_start_iso) if shard.event_start_iso else None
    )
    event_end = date.fromisoformat(shard.event_end_iso) if shard.event_end_iso else None
    return compute_element_ranking_data_fingerprint(
        session,
        analytics,
        start_season_year=shard.season_year,
        end_season_year=shard.season_year,
        event_start_date=event_start,
        event_end_date=event_end,
        discipline_type_ids=[shard.discipline_type_id],
        competition_scope=shard.competition_scope,
    )


def _normalize_shard_marks(
    df: pd.DataFrame, session: Session, analytics: JudgeAnalytics
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=list(SHARD_MARK_COLUMNS))
    if "judge_name" not in df.columns:
        df = attach_judge_identities(df, analytics)
    for col in SHARD_MARK_COLUMNS:
        if col not in df.columns:
            if col == "judge_name":
                df = attach_judge_identities(df, analytics)
            else:
                raise ValueError(f"Shard marks missing required column: {col}")
    return df[list(SHARD_MARK_COLUMNS)].copy()


def _load_marks_from_db(
    session: Session, analytics: JudgeAnalytics, shard: ElementRankingShard
) -> pd.DataFrame:
    event_start = (
        date.fromisoformat(shard.event_start_iso) if shard.event_start_iso else None
    )
    event_end = date.fromisoformat(shard.event_end_iso) if shard.event_end_iso else None
    df = load_element_marking_data(
        session,
        analytics,
        start_season_year=shard.season_year,
        end_season_year=shard.season_year,
        event_start_date=event_start,
        event_end_date=event_end,
        discipline_type_ids=[shard.discipline_type_id],
        competition_scope=shard.competition_scope,
    )
    if df.empty:
        return pd.DataFrame(columns=list(SHARD_MARK_COLUMNS))
    return _normalize_shard_marks(df, session, analytics)


def _load_shard_row(
    session: Session, analytics: JudgeAnalytics, shard: ElementRankingShard
) -> pd.DataFrame | None:
    ensure_element_ranking_cache_tables(session.get_bind())
    row = session.get(ElementDeviationRankingShardCache, shard_cache_key(shard))
    if row is None:
        return None
    expected = _shard_fingerprint(session, analytics, shard)
    if row.data_fingerprint != expected:
        session.delete(row)
        session.commit()
        return None
    try:
        df = pickle.loads(row.marks_payload)
    except Exception:
        session.delete(row)
        session.commit()
        return None
    if not isinstance(df, pd.DataFrame):
        return None
    return _normalize_shard_marks(df, session, analytics)


def _save_shard_row(
    session: Session,
    analytics: JudgeAnalytics,
    shard: ElementRankingShard,
    marks: pd.DataFrame,
) -> None:
    _require_postgres(session.get_bind())
    ensure_element_ranking_cache_tables(session.get_bind())
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
        existing = write_session.get(ElementDeviationRankingShardCache, key)
        if existing:
            for k, v in row.items():
                setattr(existing, k, v)
        else:
            write_session.add(ElementDeviationRankingShardCache(**row))
        write_session.commit()
    except Exception:
        write_session.rollback()
        raise
    finally:
        write_session.close()


def collect_marks_for_run(
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
) -> pd.DataFrame:
    """
    Load marks for every (season × discipline) shard; optionally read/write shard cache.

    Returns empty DataFrame if no marks. Returns ``None`` when ``cache_only=True`` and
    any required shard is missing or stale.
    """
    session = analytics.session
    shards = iter_element_ranking_shards(
        analytics,
        start_season_year=start_season_year,
        end_season_year=end_season_year,
        discipline_type_ids=discipline_type_ids,
        competition_scope=competition_scope,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
    )
    if not shards:
        return pd.DataFrame()

    parts: list[pd.DataFrame] = []
    for shard in shards:
        marks = _load_shard_row(session, analytics, shard)
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


def _load_legacy_full_cache(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
) -> dict[str, Any] | None:
    cache_key = run_params_cache_key(run_params)
    row = session.get(ElementDeviationRankingCache, cache_key)
    if row is None:
        return None
    expected = compute_element_ranking_data_fingerprint(
        session, analytics, **element_ranking_scope_kwargs(run_params)
    )
    if row.data_fingerprint != expected:
        session.delete(row)
        session.commit()
        return None
    try:
        main = pickle.loads(row.result_payload)
    except Exception:
        session.delete(row)
        session.commit()
        return None
    if not isinstance(main, dict):
        return None
    return merge_ranking_result_from_storage(
        main, row.ctrl_payload, row.params_payload
    )


def load_cached_rankings(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
) -> dict[str, Any] | None:
    ensure_element_ranking_cache_tables(session.get_bind())
    legacy = _load_legacy_full_cache(session, analytics, run_params)
    if legacy is not None:
        return apply_min_marks_to_ranking_result(legacy, int(run_params[6] or 0))

    result = run_element_deviation_ranking_pipeline(
        analytics,
        **ranking_scope_kwargs_from_run_params(run_params),
        min_marks=int(run_params[6] or 0),
        floor_sigma=float(run_params[7]),
        min_bin_count=int(run_params[8]),
        benchmark_start_season_year=benchmark_season_bounds(run_params)[0],
        benchmark_end_season_year=benchmark_season_bounds(run_params)[1],
        benchmark_competition_scope_key=benchmark_competition_scope(run_params),
        cache_only=True,
        persist_shards=False,
    )
    if result.get("error"):
        return None
    return result


def save_cached_rankings(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    result: dict[str, Any],
) -> str:
    """Persist ranking and benchmark shards plus σ̂ cache rows."""
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
    n_rank_shards = len(
        list(
            iter_element_ranking_shards(
                analytics,
                **rank_scope,
            )
        )
    )
    _log.info("Saved %s ranking shard(s) for element ranking cache", n_rank_shards)
    return run_params_cache_key(run_params)


def try_save_element_ranking_cache(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
    result: dict[str, Any],
) -> tuple[bool, str | None]:
    try:
        save_cached_rankings(session, analytics, run_params, result)
        return True, None
    except Exception as exc:
        _log.exception("Failed to save element ranking cache")
        return False, str(exc)


def precompute_element_ranking_shards(
    session: Session,
    analytics: JudgeAnalytics,
    *,
    competition_scope: str,
    season_years: list[str] | None = None,
    discipline_type_ids: list[int] | None = None,
) -> int:
    """Warm shard cache for each season × discipline. Returns shards written."""
    years = season_years or season_years_in_run_range(
        None, None, [str(y) for y in analytics.get_years()]
    )
    disc_ids = discipline_ids_for_element_ranking(
        analytics, discipline_type_ids, competition_scope
    )
    n = 0
    for sy in years:
        for dt_id in disc_ids:
            shard = ElementRankingShard(
                season_year=sy,
                discipline_type_id=dt_id,
                competition_scope=competition_scope,
            )
            marks = _load_marks_from_db(session, analytics, shard)
            _save_shard_row(session, analytics, shard, marks)
            n += 1
            print(f"  shard {sy} discipline_id={dt_id}: {len(marks):,} marks")
    return n


def invalidate_element_ranking_cache_for_competition(
    session: Session, competition_id: int
) -> int:
    ensure_element_ranking_cache_tables(session.get_bind())
    year = session.execute(
        select(Competition.year).where(Competition.id == competition_id)
    ).scalar_one_or_none()
    if not year:
        return 0
    year_s = str(year)
    shard_del = session.execute(
        delete(ElementDeviationRankingShardCache).where(
            ElementDeviationRankingShardCache.season_year == year_s
        )
    )
    full_del = session.execute(
        delete(ElementDeviationRankingCache).where(
            and_(
                or_(
                    ElementDeviationRankingCache.start_season_year.is_(None),
                    ElementDeviationRankingCache.start_season_year <= year_s,
                ),
                or_(
                    ElementDeviationRankingCache.end_season_year.is_(None),
                    ElementDeviationRankingCache.end_season_year >= year_s,
                ),
            )
        )
    )
    sigma_del = session.execute(
        delete(ElementDeviationRankingSigmaCache).where(
            and_(
                or_(
                    ElementDeviationRankingSigmaCache.benchmark_start_season_year.is_(
                        None
                    ),
                    ElementDeviationRankingSigmaCache.benchmark_start_season_year
                    <= year_s,
                ),
                or_(
                    ElementDeviationRankingSigmaCache.benchmark_end_season_year.is_(
                        None
                    ),
                    ElementDeviationRankingSigmaCache.benchmark_end_season_year
                    >= year_s,
                ),
            )
        )
    )
    session.flush()
    return (
        int(shard_del.rowcount or 0)
        + int(full_del.rowcount or 0)
        + int(sigma_del.rowcount or 0)
    )


def precompute_element_ranking_sigma(
    session: Session,
    analytics: JudgeAnalytics,
    run_params: tuple,
) -> str | None:
    """Warm σ̂ cache for a benchmark scope. Returns sigma_key or None if no marks."""
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


# Backward compatibility
ensure_element_ranking_cache_table = ensure_element_ranking_cache_tables
