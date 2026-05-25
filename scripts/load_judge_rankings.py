#!/usr/bin/env python3
"""
Fit element marking scores per judge (control-score / sigma model).

Judges linked to the same directory official in Admin are merged into one row
(same identity groups as Cross-Judge Benchmarking / Individual Judge Analysis).

Requires DATABASE_URL and project deps (``pip install -r requirements.txt`` includes
plotly for ``--histogram``). Run from repo root:

    python scripts/load_judge_rankings.py
    python scripts/load_judge_rankings.py --output judge_marking_scores_elements.csv
    python scripts/load_judge_rankings.py --min-marks 500 --histogram
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import plotly.express as px

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from database import get_db_session  # noqa: E402
from analytics import JudgeAnalytics  # noqa: E402
from element_deviation_ranking import (  # noqa: E402
    FLOOR_SIGMA,
    MIN_BIN_COUNT,
    compute_element_deviation_rankings,
)

DEFAULT_OUT_CSV = "judge_marking_scores_elements.csv"
DEFAULT_PARAMS_JSON = "judge_sigma_params_elements.json"


def save_params_json_discrete(
    params: dict,
    path: Path,
    start_year,
    end_year,
    discipline_type_ids,
) -> None:
    payload = {
        "meta": {
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "start_year": start_year,
            "end_year": end_year,
            "discipline_type_ids": list(discipline_type_ids) if discipline_type_ids else None,
            "floor_sigma": FLOOR_SIGMA,
            "min_bin_count": MIN_BIN_COUNT,
            "model": "discrete_sd_by_int_control_score",
        },
        "params": {
            f"{d}-{e}-{k}": sigma for (d, e, k), sigma in params.items()
        },
    }
    path.write_text(json.dumps(payload, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute element judge marking scores.")
    parser.add_argument("--output", "-o", default=DEFAULT_OUT_CSV)
    parser.add_argument("--params-output", default=DEFAULT_PARAMS_JSON)
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--discipline-type-id", type=int, action="append")
    parser.add_argument("--min-marks", type=int, default=0)
    parser.add_argument("--histogram", action="store_true")
    parser.add_argument("--histogram-output")

    args = parser.parse_args()
    start_sy = str(args.start_year) if args.start_year is not None else None
    end_sy = str(args.end_year) if args.end_year is not None else None

    session = get_db_session()
    try:
        analytics = JudgeAnalytics(session)
        result = compute_element_deviation_rankings(
            analytics,
            start_season_year=start_sy,
            end_season_year=end_sy,
            discipline_type_ids=args.discipline_type_id,
            min_marks=args.min_marks,
        )
        if result["error"]:
            print(result["error"])
            return
        marking = result["marking"].rename(
            columns={
                "Judge": "judge_name",
                "Element marks": "n_marks",
                "Marking score": "marking_score",
            }
        )[["judge_name", "n_marks", "marking_score"]]
    finally:
        session.close()

    if marking.empty:
        print("No judges in output.")
        return

    print("\n--- marking_score (included judges) ---")
    print(marking["marking_score"].describe())
    print(f"n judges: {len(marking)}")

    out_path = Path(args.output)
    marking.to_csv(out_path, index=False)
    print(f"\nWrote {len(marking)} judge marking scores to {out_path.resolve()}")

    if args.histogram and not marking.empty:
        hist_path = (
            Path(args.histogram_output)
            if args.histogram_output
            else out_path.with_name(f"{out_path.stem}_histogram.html")
        )
        fig = px.histogram(
            marking,
            x="marking_score",
            nbins=min(40, max(10, len(marking) // 5)),
            title="Element marking scores (lower = closer to control-score model)",
        )
        fig.write_html(str(hist_path), include_plotlyjs="cdn")
        print(f"Wrote histogram to {hist_path.resolve()}")

    save_params_json_discrete(
        result["params"],
        Path(args.params_output),
        args.start_year,
        args.end_year,
        args.discipline_type_id,
    )


if __name__ == "__main__":
    main()
