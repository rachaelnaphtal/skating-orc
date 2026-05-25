"""
Subprocess-friendly runner for element deviation rankings.

Used by the Streamlit page so a long run can be terminated without blocking
navigation. Started via ``python -m element_deviation_ranking_job`` so the
child never imports ``analysis_app.py`` (avoids Streamlit ScriptRunContext
warnings).
"""

from __future__ import annotations

import argparse
import os
import pickle
import subprocess
import sys
import traceback
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from analytics import JudgeAnalytics
from database import get_db_session
from element_deviation_ranking import compute_element_deviation_rankings

_REPO_ROOT = Path(__file__).resolve().parent

ElementRankingRunParams = tuple[
    str | None,
    str | None,
    tuple[int, ...] | None,
    str,
    str | None,
    str | None,
    int,
    float,
    int,
]


@dataclass
class RankingJobHandle:
    """Running ``python -m element_deviation_ranking_job`` child."""

    popen: subprocess.Popen
    params_path: str
    result_path: str

    def is_alive(self) -> bool:
        return self.popen.poll() is None

    @property
    def exitcode(self) -> int | None:
        return self.popen.poll()


def execute_element_deviation_rankings(run_params: ElementRankingRunParams) -> dict:
    """Run the full pipeline with a fresh DB session (no Streamlit)."""
    (
        start_season_year,
        end_season_year,
        discipline_ids_tuple,
        competition_scope,
        event_start_iso,
        event_end_iso,
        min_marks,
        floor_sigma,
        min_bin_count,
    ) = run_params

    event_start = date.fromisoformat(event_start_iso) if event_start_iso else None
    event_end = date.fromisoformat(event_end_iso) if event_end_iso else None
    discipline_type_ids = (
        list(discipline_ids_tuple) if discipline_ids_tuple else None
    )

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        return compute_element_deviation_rankings(
            analytics,
            start_season_year=start_season_year,
            end_season_year=end_season_year,
            event_start_date=event_start,
            event_end_date=event_end,
            discipline_type_ids=discipline_type_ids,
            competition_scope=competition_scope,
            min_marks=min_marks,
            floor_sigma=floor_sigma,
            min_bin_count=min_bin_count,
        )
    finally:
        session.close()


def package_element_ranking_result(result: dict, base_pickle_path: str) -> dict:
    """
    Move large objects to sidecar pickles so the parent Streamlit process
    does not load panel medians / σ̂ params when reading the main result.
    """
    out = dict(result)
    ctrl = out.pop("control_by_element", None)
    if isinstance(ctrl, pd.DataFrame) and not ctrl.empty:
        ctrl_path = base_pickle_path + ".ctrl.pkl"
        with open(ctrl_path, "wb") as f:
            pickle.dump(ctrl, f, protocol=pickle.HIGHEST_PROTOCOL)
        out["control_by_element_path"] = ctrl_path
    params = out.pop("params", None)
    if params:
        params_path = base_pickle_path + ".params.pkl"
        with open(params_path, "wb") as f:
            pickle.dump(params, f, protocol=pickle.HIGHEST_PROTOCOL)
        out["params_path"] = params_path
    return out


def load_ranking_params(result: dict) -> dict:
    params = result.get("params")
    if params:
        return params
    path = result.get("params_path")
    if path and os.path.isfile(path):
        with open(path, "rb") as f:
            return pickle.load(f)
    return {}


def load_control_by_element(result: dict) -> pd.DataFrame:
    ctrl = result.get("control_by_element")
    if isinstance(ctrl, pd.DataFrame) and not ctrl.empty:
        return ctrl
    path = result.get("control_by_element_path")
    if path and os.path.isfile(path):
        with open(path, "rb") as f:
            loaded = pickle.load(f)
        if isinstance(loaded, pd.DataFrame):
            return loaded
    return pd.DataFrame()


def _worker_main(run_params: ElementRankingRunParams, out_pickle_path: str) -> None:
    err_path = out_pickle_path + ".err"
    try:
        result = execute_element_deviation_rankings(run_params)
        packaged = package_element_ranking_result(result, out_pickle_path)
        with open(out_pickle_path, "wb") as f:
            pickle.dump(packaged, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        with open(err_path, "w", encoding="utf-8") as f:
            f.write(traceback.format_exc())
        raise


def start_ranking_subprocess(
    run_params: ElementRankingRunParams, out_pickle_path: str
) -> RankingJobHandle:
    """Start analysis in a child process (does not import the Streamlit app)."""
    import tempfile

    fd, params_path = tempfile.mkstemp(prefix="elem_rank_params_", suffix=".pkl")
    os.close(fd)
    with open(params_path, "wb") as f:
        pickle.dump(run_params, f)

    cmd = [
        sys.executable,
        "-m",
        "element_deviation_ranking_job",
        "--result",
        out_pickle_path,
        "--params",
        params_path,
    ]
    popen = subprocess.Popen(
        cmd,
        cwd=str(_REPO_ROOT),
        stdin=subprocess.DEVNULL,
    )
    return RankingJobHandle(
        popen=popen, params_path=params_path, result_path=out_pickle_path
    )


def terminate_ranking_subprocess(
    handle: RankingJobHandle | None, *, join_seconds: float = 2.0
) -> None:
    if handle is None:
        return
    if handle.is_alive():
        handle.popen.terminate()
        try:
            handle.popen.wait(timeout=join_seconds)
        except subprocess.TimeoutExpired:
            handle.popen.kill()
            handle.popen.wait(timeout=join_seconds)


def cleanup_ranking_artifacts(
    result_path: str | None, params_path: str | None = None
) -> None:
    paths: list[str] = []
    if result_path:
        paths.extend(
            [
                result_path,
                result_path + ".err",
                result_path + ".ctrl.pkl",
                result_path + ".params.pkl",
            ]
        )
    if params_path:
        paths.append(params_path)
    for path in paths:
        try:
            os.remove(path)
        except OSError:
            pass


def load_ranking_result(pickle_path: str) -> dict[str, Any]:
    with open(pickle_path, "rb") as f:
        return pickle.load(f)


def read_ranking_error(pickle_path: str) -> str | None:
    err_path = pickle_path + ".err"
    if not os.path.isfile(err_path):
        return None
    with open(err_path, encoding="utf-8") as f:
        return f.read().strip() or "Unknown error"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Element deviation ranking worker")
    parser.add_argument("--result", required=True, help="Output pickle path")
    parser.add_argument("--params", required=True, help="Input params pickle path")
    args = parser.parse_args(argv)

    try:
        with open(args.params, "rb") as f:
            run_params = pickle.load(f)
        _worker_main(run_params, args.result)
        return 0
    except Exception:
        return 1
    finally:
        try:
            os.remove(args.params)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
