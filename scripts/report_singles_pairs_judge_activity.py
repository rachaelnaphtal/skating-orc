#!/usr/bin/env python3
"""
Excel report of Singles/Pairs Competition Judge activity for named officials.

Counts competitions (NQ, NQS, Sectional, National), segments, Junior/Senior segments,
and skaters over a USFS season window. Includes one worksheet per official with
segment-level detail.

Example::

    python scripts/report_singles_pairs_judge_activity.py
    python scripts/report_singles_pairs_judge_activity.py -o analysisTemp/judge_activity.xlsx
    python scripts/report_singles_pairs_judge_activity.py --names "Jane Doe" "John Smith"
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, select, text
from sqlalchemy.orm import Session

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from activityAnalysis.international_listing_seasons import season_codes_ending_at  # noqa: E402
from activityAnalysis.load_activity_data import (  # noqa: E402
    NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,
    NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES,
    TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS,
    TOTAL_ACTIVITY_BUCKET_NONQUALIFYING_DOMESTIC,
    TOTAL_ACTIVITY_BUCKET_NQS,
    TOTAL_ACTIVITY_BUCKET_SECTIONALS,
    TOTAL_ACTIVITY_BUCKET_DISPLAY,
    _competition_season_code_sql,
    _nqs_panel_role_sql_predicate,
    _segment_competition_year_sql_predicate,
    _segment_counts_junior_senior,
    activity_competition_bucket,
    activity_database_is_postgresql,
    calendar_years_for_usfs_season_codes,
    engine,
)
from activityAnalysis.officials_analysis_models import Officials  # noqa: E402

DEFAULT_OFFICIAL_NAMES: tuple[str, ...] = (
    "Kanae Tagawa",
    "Cassy Papajohn",
    "Bridgit Wallace",
    "Leslie Gianelli",
    "Jennifer Simon",
    "Lori Dunn",
    "Dann Krueger",
    "Linda Chihara",
    "Alexander Enzmann",
    "Allison Duarte",
    "Gretchen Bonnie",
    "Charlotte Heidenreich",
    "Lisa Hernand",
    "Marcia Chaffee",
    "Victoria Hildebrand",
    "Kathleen Krieger",
    "Deborah Weidman",
    "Coco Gram Shean",
    "Kirsten Novak",
)

DEFAULT_END_SEASON = 2526
DEFAULT_N_SEASONS = 3

SUMMARY_BUCKETS: tuple[str, ...] = (
    TOTAL_ACTIVITY_BUCKET_NONQUALIFYING_DOMESTIC,
    TOTAL_ACTIVITY_BUCKET_NQS,
    TOTAL_ACTIVITY_BUCKET_SECTIONALS,
    TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS,
)

BUCKET_LABELS: dict[str, str] = {
    TOTAL_ACTIVITY_BUCKET_NONQUALIFYING_DOMESTIC: "NQ",
    TOTAL_ACTIVITY_BUCKET_NQS: "NQS",
    TOTAL_ACTIVITY_BUCKET_SECTIONALS: "Sectional",
    TOTAL_ACTIVITY_BUCKET_CHAMPIONSHIPS: "National",
}


def _resolve_official_ids(names: list[str]) -> pd.DataFrame:
    """Map display names to ``officials_analysis.officials.id`` (exact, then unique partial)."""
    with Session(engine) as session:
        rows = session.execute(select(Officials.id, Officials.full_name)).all()
    by_exact = {(r[1] or "").strip().lower(): (int(r[0]), r[1]) for r in rows if r[1]}
    resolved: list[dict[str, Any]] = []
    missing: list[str] = []
    for name in names:
        key = name.strip().lower()
        if key in by_exact:
            oid, full_name = by_exact[key]
            resolved.append({"official_id": oid, "Name": full_name, "requested_name": name})
            continue
        partial = [
            (int(r[0]), r[1])
            for r in rows
            if key in (r[1] or "").lower() or (r[1] or "").lower() in key
        ]
        if len(partial) == 1:
            oid, full_name = partial[0]
            resolved.append({"official_id": oid, "Name": full_name, "requested_name": name})
        elif len(partial) > 1:
            options = ", ".join(f"{fn} ({oid})" for oid, fn in partial[:5])
            raise SystemExit(
                f"Ambiguous name {name!r}: {options}"
                + (" …" if len(partial) > 5 else "")
            )
        else:
            missing.append(name)
    if missing:
        raise SystemExit(f"Could not find officials: {', '.join(missing)}")
    out = pd.DataFrame(resolved)
    return out.sort_values("Name", kind="mergesort").reset_index(drop=True)


def _query_segment_activity_rows(
    official_ids: list[int],
    season_codes: list[int],
) -> pd.DataFrame:
    """Segment-level Singles/Pairs judge panels attributed to directory officials."""
    cols = [
        "official_id",
        "season_code",
        "competition_id",
        "competition_name",
        "start_date",
        "end_date",
        "international",
        "qualifying",
        "nqs",
        "competition_type_id",
        "segment_id",
        "segment_name",
        "segment_level",
        "discipline",
        "skater_count",
    ]
    if not official_ids or not season_codes or not activity_database_is_postgresql():
        return pd.DataFrame(columns=cols)

    calendar_years = calendar_years_for_usfs_season_codes(season_codes)
    if not calendar_years:
        return pd.DataFrame(columns=cols)

    season_expr = _competition_season_code_sql("c")
    role_pred = _nqs_panel_role_sql_predicate("judge")
    year_pred = _segment_competition_year_sql_predicate()
    discipline_ids = (
        NQS_SEGMENT_DISCIPLINE_TYPE_SINGLES,
        NQS_SEGMENT_DISCIPLINE_TYPE_PAIRS,
    )

    stmt = text(
        f"""
        WITH team_counts AS (
            SELECT segment_id, COUNT(*)::integer AS skater_count
            FROM public.skater_segment
            GROUP BY segment_id
        )
        SELECT
            attrib.directory_official_id AS official_id,
            {season_expr} AS season_code,
            c.id AS competition_id,
            c.name AS competition_name,
            c.start_date,
            c.end_date,
            COALESCE(c.international, false) AS international,
            COALESCE(c.qualifying, false) AS qualifying,
            COALESCE(c.nqs, false) AS nqs,
            c.officials_analysis_competition_type_id AS competition_type_id,
            s.id AS segment_id,
            s.name AS segment_name,
            s.level AS segment_level,
            dt.name AS discipline,
            COALESCE(tc.skater_count, 0) AS skater_count
        FROM public.segment_official so
        INNER JOIN public.segment s ON s.id = so.segment_id
        INNER JOIN public.competition c ON c.id = s.competition_id
        LEFT JOIN public.discipline_type dt ON dt.id = s.discipline_type_id
        LEFT JOIN team_counts tc ON tc.segment_id = s.id
        CROSS JOIN LATERAL (
            SELECT COALESCE(
                CASE WHEN so.official_id IN :official_ids THEN so.official_id END,
                (
                    SELECT MIN(jol.official_id)
                    FROM public.judge_official_link jol
                    INNER JOIN public.judge j ON j.id = jol.judge_id
                    WHERE jol.status = 'linked'
                      AND jol.official_id IN :official_ids
                      AND jol.official_id IS NOT NULL
                      AND so.official_name IS NOT NULL
                      AND lower(btrim(j.name)) = lower(btrim(so.official_name))
                )
            ) AS directory_official_id
        ) AS attrib
        WHERE attrib.directory_official_id IS NOT NULL
{year_pred}              AND s.discipline_type_id IN :discipline_type_ids
              AND ({role_pred})
        """
    ).bindparams(
        bindparam("official_ids", expanding=True),
        bindparam("discipline_type_ids", expanding=True),
        bindparam("season_year_codes", expanding=True),
        bindparam("calendar_year_codes", expanding=True),
    )
    params = {
        "official_ids": [int(x) for x in official_ids],
        "discipline_type_ids": list(discipline_ids),
        "season_year_codes": [int(x) for x in season_codes],
        "calendar_year_codes": [int(x) for x in calendar_years],
    }
    with Session(engine) as session:
        rows = session.execute(stmt, params).mappings().all()
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    df["season_code"] = pd.to_numeric(df["season_code"], errors="coerce")
    df = df.loc[df["season_code"].notna()].copy()
    df["season_code"] = df["season_code"].astype(int)
    df["activity_bucket"] = df.apply(
        lambda r: activity_competition_bucket(
            international=bool(r["international"]),
            qualifying=bool(r["qualifying"]),
            nqs=bool(r["nqs"]),
            competition_type_id=r["competition_type_id"],
        ),
        axis=1,
    )
    df = (
        df.sort_values(
            ["official_id", "segment_id", "competition_id"],
            kind="mergesort",
        )
        .drop_duplicates(subset=["official_id", "segment_id"], keep="first")
        .reset_index(drop=True)
    )
    return df


def _season_comp_column(season_code: int) -> str:
    return f"{int(season_code)} comps"


def _season_jr_sr_seg_column(season_code: int) -> str:
    return f"{int(season_code)} Jr/Sr Segs"


def _season_jr_sr_starts_column(season_code: int) -> str:
    return f"{int(season_code)} Jr/Sr Starts"


def _junior_senior_mask(segment_rows: pd.DataFrame) -> pd.Series:
    return segment_rows.apply(
        lambda r: _segment_counts_junior_senior(
            r["segment_level"],
            r["skater_count"],
            junior_senior_min_team_count=None,
        ),
        axis=1,
    )


def _aggregate_summary(
    segment_rows: pd.DataFrame,
    officials: pd.DataFrame,
    season_codes: list[int],
) -> pd.DataFrame:
    """One summary row per official with bucket competition counts and activity totals."""
    season_cols = [_season_comp_column(sc) for sc in sorted(season_codes, reverse=True)]
    rows_out: list[dict[str, Any]] = []
    for _, off in officials.iterrows():
        oid = int(off["official_id"])
        sub = segment_rows.loc[segment_rows["official_id"] == oid]
        jr_sr = sub.loc[_junior_senior_mask(sub)] if not sub.empty else sub
        row: dict[str, Any] = {"Name": off["Name"], "official_id": oid}
        row["Total comps"] = int(sub["competition_id"].nunique())
        for season_code in sorted(season_codes, reverse=True):
            season_sub = sub.loc[sub["season_code"] == int(season_code)]
            season_jr_sr = jr_sr.loc[jr_sr["season_code"] == int(season_code)]
            row[_season_comp_column(season_code)] = int(
                season_sub["competition_id"].nunique()
            )
            row[_season_jr_sr_seg_column(season_code)] = int(
                season_jr_sr["segment_id"].nunique()
            )
            row[_season_jr_sr_starts_column(season_code)] = int(
                season_jr_sr["skater_count"].sum()
            )
        for bucket in SUMMARY_BUCKETS:
            label = BUCKET_LABELS[bucket]
            bucket_sub = sub.loc[sub["activity_bucket"] == bucket]
            row[f"{label} comps"] = int(bucket_sub["competition_id"].nunique())
        row["Segments"] = int(sub["segment_id"].nunique())
        row["Jr/Sr Segments"] = int(jr_sr["segment_id"].nunique())
        row["Jr/Sr Starts"] = int(jr_sr["skater_count"].sum())
        row["Skaters"] = int(sub["skater_count"].sum())
        rows_out.append(row)
    out = pd.DataFrame(rows_out)
    per_season_jr_sr_cols: list[str] = []
    for sc in sorted(season_codes, reverse=True):
        per_season_jr_sr_cols.extend(
            [_season_jr_sr_seg_column(sc), _season_jr_sr_starts_column(sc)]
        )
    col_order = (
        ["Name", "Total comps", *season_cols]
        + [f"{BUCKET_LABELS[b]} comps" for b in SUMMARY_BUCKETS]
        + ["Segments", "Jr/Sr Segments", "Jr/Sr Starts", *per_season_jr_sr_cols, "Skaters", "official_id"]
    )
    return out[[c for c in col_order if c in out.columns]]


def _format_detail_sheet(segment_rows: pd.DataFrame) -> pd.DataFrame:
    """Segment rows for a per-official worksheet."""
    if segment_rows.empty:
        return pd.DataFrame(
            columns=[
                "Season",
                "Competition",
                "Date",
                "Segment",
                "Level",
                "Discipline",
                "Comp type",
                "# Skaters",
            ]
        )
    detail = segment_rows.copy()
    detail["Date"] = pd.to_datetime(
        detail["end_date"].fillna(detail["start_date"]), errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    detail["Comp type"] = detail["activity_bucket"].map(
        lambda b: BUCKET_LABELS.get(b, TOTAL_ACTIVITY_BUCKET_DISPLAY.get(b, b))
    )
    detail = detail.sort_values(
        ["Date", "competition_name", "segment_name"],
        ascending=[False, True, True],
        kind="mergesort",
        na_position="last",
    )
    return detail.rename(
        columns={
            "season_code": "Season",
            "competition_name": "Competition",
            "segment_name": "Segment",
            "segment_level": "Level",
            "discipline": "Discipline",
            "skater_count": "# Skaters",
        }
    )[
        [
            "Season",
            "Competition",
            "Date",
            "Segment",
            "Level",
            "Discipline",
            "Comp type",
            "# Skaters",
        ]
    ]


def _safe_sheet_name(name: str, used: set[str]) -> str:
    base = re.sub(r"[\[\]:*?/\\]", "", name)[:31].strip() or "Official"
    candidate = base
    n = 2
    while candidate in used:
        suffix = f" ({n})"
        candidate = f"{base[: 31 - len(suffix)]}{suffix}"
        n += 1
    used.add(candidate)
    return candidate


def build_report(
    names: list[str],
    *,
    end_season: int = DEFAULT_END_SEASON,
    n_seasons: int = DEFAULT_N_SEASONS,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    season_codes = season_codes_ending_at(int(end_season), int(n_seasons))
    officials = _resolve_official_ids(names)
    segment_rows = _query_segment_activity_rows(
        officials["official_id"].astype(int).tolist(),
        season_codes,
    )
    summary = _aggregate_summary(segment_rows, officials, season_codes)
    detail_by_name: dict[str, pd.DataFrame] = {}
    for _, off in officials.iterrows():
        oid = int(off["official_id"])
        sub = segment_rows.loc[segment_rows["official_id"] == oid]
        detail_by_name[str(off["Name"])] = _format_detail_sheet(sub)
    return summary, detail_by_name


def write_excel(
    path: Path,
    summary: pd.DataFrame,
    detail_by_name: dict[str, pd.DataFrame],
    *,
    season_codes: list[int],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    used_sheet_names: set[str] = {"Summary"}
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary_display = summary.drop(columns=["official_id"], errors="ignore")
        summary_display.to_excel(writer, sheet_name="Summary", index=False)
        ws = writer.sheets["Summary"]
        ws.insert_rows(1)
        ws["A1"] = (
            f"Singles/Pairs Competition Judge activity — seasons "
            f"{', '.join(f'{sc // 100}-{sc % 100:02d}' for sc in sorted(season_codes))}"
        )
        for name, detail in detail_by_name.items():
            sheet = _safe_sheet_name(name, used_sheet_names)
            detail.to_excel(writer, sheet_name=sheet, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--names",
        nargs="*",
        default=list(DEFAULT_OFFICIAL_NAMES),
        help="Directory official names (default: built-in list of 19 judges).",
    )
    parser.add_argument(
        "--end-season",
        type=int,
        default=DEFAULT_END_SEASON,
        help=f"Last USFS season code in window (default {DEFAULT_END_SEASON}).",
    )
    parser.add_argument(
        "--seasons",
        type=int,
        default=DEFAULT_N_SEASONS,
        help=f"Number of seasons ending at --end-season (default {DEFAULT_N_SEASONS}).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("analysisTemp/singles_pairs_judge_activity.xlsx"),
        help="Output .xlsx path.",
    )
    args = parser.parse_args()

    if not activity_database_is_postgresql():
        print(
            "This report requires PostgreSQL (DATABASE_URL). Activity tables are not "
            "available on SQLite.",
            file=sys.stderr,
        )
        return 1

    season_codes = season_codes_ending_at(args.end_season, args.seasons)
    summary, detail_by_name = build_report(
        args.names,
        end_season=args.end_season,
        n_seasons=args.seasons,
    )
    write_excel(args.output, summary, detail_by_name, season_codes=season_codes)
    print(f"Wrote {args.output} ({len(summary)} officials, seasons {season_codes})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
