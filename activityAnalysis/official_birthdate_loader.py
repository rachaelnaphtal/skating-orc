"""Load official date_of_birth from a USFS ages / directory CSV export."""

from __future__ import annotations

import os
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from activityAnalysis.officials_directory_loader import get_conn
from activityAnalysis.qualifying_availability_ingest import normalize_member_number_value


def _normalize_ages_csv_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = (
        out.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("#", "num", regex=False)
    )
    return out


def parse_ages_csv(csv_path: str) -> Tuple[pd.DataFrame, List[str]]:
    """
    Read ages export and return one row per member with parsed ``date_of_birth``.

    Returns ``(frame, warnings)`` where frame columns are ``mbr_number``,
    ``date_of_birth`` (Python ``date``).
    """
    raw = pd.read_csv(csv_path)
    df = _normalize_ages_csv_columns(raw)

    member_col = None
    for candidate in ("member_num", "mbr_num"):
        if candidate in df.columns:
            member_col = candidate
            break
    if member_col is None:
        raise ValueError(
            f"Expected a Member # column in {csv_path!r}; got: {list(raw.columns)}"
        )
    if "dob" not in df.columns:
        raise ValueError(
            f"Expected a DOB column in {csv_path!r}; got: {list(raw.columns)}"
        )

    warnings: List[str] = []
    df["mbr_number"] = df[member_col].map(normalize_member_number_value)
    df = df[df["mbr_number"] != ""].copy()

    parsed = pd.to_datetime(df["dob"], format="%m/%d/%Y", errors="coerce")
    if parsed.isna().any():
        bad = int(parsed.isna().sum())
        warnings.append(f"Skipped {bad} row(s) with unparseable DOB (expected MM/DD/YYYY).")
    df["date_of_birth"] = parsed.dt.date
    df = df.dropna(subset=["date_of_birth"])

    by_member = (
        df[["mbr_number", "date_of_birth"]]
        .drop_duplicates(subset=["mbr_number"])
        .reset_index(drop=True)
    )
    if len(by_member) < df["mbr_number"].nunique():
        warnings.append(
            "Multiple DOB values for the same member number; kept first occurrence per member."
        )

    return by_member, warnings


def load_official_birthdates_from_csv(
    csv_path: str,
    *,
    dry_run: bool = False,
    log: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    """
    Set ``officials_analysis.officials.date_of_birth`` for rows whose ``mbr_number``
    appears in the CSV. Officials not in the database are skipped.

    Requires migration ``027_officials_date_of_birth.sql`` and ``DATABASE_URL``.
    """
    log_fn = log if log is not None else print
    frame, parse_warnings = parse_ages_csv(csv_path)
    for w in parse_warnings:
        log_fn(w)

    csv_by_mbr: Dict[str, date] = {
        row.mbr_number: row.date_of_birth for row in frame.itertuples(index=False)
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT mbr_number, date_of_birth FROM officials WHERE mbr_number IS NOT NULL"
            )
            db_rows = cur.fetchall()

    db_by_mbr: Dict[str, Optional[date]] = {}
    for mbr, existing in db_rows:
        key = normalize_member_number_value(mbr)
        if key:
            db_by_mbr[key] = existing

    updates: List[Tuple[date, str]] = []
    not_in_db: List[str] = []
    unchanged = 0

    for mbr, dob in csv_by_mbr.items():
        if mbr not in db_by_mbr:
            not_in_db.append(mbr)
            continue
        if db_by_mbr[mbr] == dob:
            unchanged += 1
            continue
        updates.append((dob, mbr))

    summary: Dict[str, Any] = {
        "csv_members": len(csv_by_mbr),
        "matched_in_db": len(csv_by_mbr) - len(not_in_db),
        "not_in_db": len(not_in_db),
        "unchanged": unchanged,
        "updated": len(updates),
        "dry_run": dry_run,
    }

    log_fn(
        f"CSV members: {summary['csv_members']}; "
        f"in officials table: {summary['matched_in_db']}; "
        f"not in DB: {summary['not_in_db']}; "
        f"unchanged: {summary['unchanged']}; "
        f"to update: {summary['updated']}"
    )

    if dry_run or not updates:
        return summary

    sql = """
        UPDATE officials
        SET date_of_birth = %s,
            last_modified = CURRENT_TIMESTAMP
        WHERE mbr_number = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, updates)
        conn.commit()

    log_fn(f"Updated {len(updates)} official(s).")
    return summary


def main(argv: Optional[List[str]] = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Load DOB from USFS ages CSV into officials_analysis.officials."
    )
    parser.add_argument("csv_path", help="Path to ages CSV (Member #, DOB, …)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report counts only; do not write to the database",
    )
    args = parser.parse_args(argv)

    if not os.path.isfile(args.csv_path):
        raise SystemExit(f"File not found: {args.csv_path}")

    load_official_birthdates_from_csv(args.csv_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
