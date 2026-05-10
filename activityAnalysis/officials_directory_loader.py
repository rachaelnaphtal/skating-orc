import os
import sys
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse

_conn_params_cache: Optional[dict] = None


def _conn_params() -> dict:
    global _conn_params_cache
    if _conn_params_cache is None:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is not set.")
        parsed = urlparse(database_url)
        _conn_params_cache = {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "dbname": parsed.path.lstrip("/"),
            "user": parsed.username,
            "password": parsed.password,
            "options": "-csearch_path=officials_analysis",
        }
    return _conn_params_cache


def get_conn():
    return psycopg2.connect(**_conn_params())


# Same as assignments / activity tracker: store "no discipline" as id 7 (not NULL).
NO_DISCIPLINE_ID = 7


def _discipline_id_from_excel_cell(row, disciplines_map) -> Optional[int]:
    """
    Map export ``disciplines`` column to ``disciplines.id``. Empty / NaN -> ``NO_DISCIPLINE_ID``.
    Unknown text still returns None (caller may skip or log).
    """
    raw = row.get("discipline")
    if raw is None:
        return NO_DISCIPLINE_ID
    if isinstance(raw, float) and pd.isna(raw):
        return NO_DISCIPLINE_ID
    s = str(raw).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return NO_DISCIPLINE_ID
    return disciplines_map.get(s)


def na_to_none(val):
    """Convert any pandas/numpy NA/NaN to Python None."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def upsert_lookup(table, rows, key_col="name", extra_cols=None):
    """
    rows: list of tuples matching (key_col, *extra_cols)
    Upserts via ON CONFLICT DO UPDATE.
    """
    if not rows:
        return

    all_cols = [key_col] + (extra_cols or [])
    col_list = ", ".join(all_cols)
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in all_cols if c != key_col
    ) or f"{key_col} = EXCLUDED.{key_col}"

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ({key_col}) DO UPDATE SET {updates}
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()


def name_rows(series):
    return [(v,) for v in series.dropna().drop_duplicates().tolist()]


def _norm_key_part(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def run_officials_directory_import(
    excel_path: str,
    *,
    sheet_name: str = "Officials",
    log: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Load US Figure Skating officials directory export (Excel) into ``officials_analysis``.

    ``log`` defaults to ``print``; pass a callable(str) to capture messages (e.g. Streamlit).
    """
    log_fn = log if log is not None else print
    logs: List[str] = []

    def _log(msg: str) -> None:
        logs.append(msg)
        log_fn(msg)

    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("#", "num", regex=False)
    )

    df["mbr_number"] = df["mbr_num"].astype(str).str.strip()
    df["is_coach"] = df["coach"].map({"Yes": True, "No": False})
    df["full_name"] = df["first_name"].str.strip() + " " + df["last_name"].str.strip()
    df["appointment_type"] = df["types"].astype(str).str.strip().replace("nan", None)
    df["discipline"] = df["disciplines"].astype(str).str.strip().replace("nan", None)
    df["level"] = df["levels"].astype(str).str.strip().replace("nan", None)
    df["appointed_date"] = pd.to_datetime(df["appointed_date"], errors="coerce")
    df["achieved_date"] = pd.to_datetime(df["achieved_date"], errors="coerce")
    df["mentor"] = (
        df["mentor"].astype(str).str.strip().replace("nan", None).replace("", None)
    )

    _log("Upserting lookup tables...")
    upsert_lookup("appointment_types", name_rows(df["appointment_type"]))
    upsert_lookup("disciplines", name_rows(df["discipline"]))
    upsert_lookup("levels", name_rows(df["level"]))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO disciplines (id, name)
                VALUES (%s, 'No Discipline')
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                """,
                (NO_DISCIPLINE_ID,),
            )
        conn.commit()
    _log("  done.")

    officials_cols = [
        "mbr_number",
        "first_name",
        "last_name",
        "full_name",
        "is_coach",
        "email",
        "phone",
        "city",
        "state",
        "region",
    ]

    officials_df = df[officials_cols].drop_duplicates(subset=["mbr_number"])

    officials_rows = [
        tuple(na_to_none(row[c]) for c in officials_cols)
        for _, row in officials_df.iterrows()
    ]

    col_list = ", ".join(officials_cols)
    update_cols = [c for c in officials_cols if c != "mbr_number"]
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    # Without a WHERE clause, Postgres runs UPDATE on every conflict even when values
    # match, causing needless row versions / WAL. Skip when nothing changed.
    distinct_checks = " OR ".join(
        f"officials.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in update_cols
    )

    officials_sql = f"""
        INSERT INTO officials ({col_list})
        VALUES %s
        ON CONFLICT (mbr_number) DO UPDATE SET {updates}
        WHERE {distinct_checks}
    """

    _log(f"Upserting {len(officials_rows)} officials...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, officials_sql, officials_rows, page_size=500
            )
        conn.commit()
    _log("  done.")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, mbr_number FROM officials")
            officials_map = {row[1]: row[0] for row in cur.fetchall()}

            cur.execute("SELECT id, name FROM appointment_types")
            types_map = {row[1]: row[0] for row in cur.fetchall()}

            cur.execute("SELECT id, name FROM disciplines")
            disciplines_map = {row[1]: row[0] for row in cur.fetchall()}

            cur.execute("SELECT id, name FROM levels")
            levels_map = {row[1]: row[0] for row in cur.fetchall()}

    appointment_rows = []
    for _, row in df.iterrows():
        official_id = officials_map.get(row["mbr_number"])
        appointment_type_id = (
            types_map.get(row["appointment_type"]) if row["appointment_type"] else None
        )
        discipline_id = _discipline_id_from_excel_cell(row, disciplines_map)
        if discipline_id is None:
            _log(
                f"Skipping: unknown discipline {row.get('discipline')!r} "
                f"(mbr {row.get('mbr_number')})"
            )
            continue
        level_id = levels_map.get(row["level"]) if row["level"] else None
        appointed_date = na_to_none(row["appointed_date"])
        achieved_date = na_to_none(row["achieved_date"])
        mentor = na_to_none(row["mentor"])

        if official_id is None:
            continue

        if hasattr(appointed_date, "to_pydatetime"):
            appointed_date = appointed_date.to_pydatetime()
        if hasattr(achieved_date, "to_pydatetime"):
            achieved_date = achieved_date.to_pydatetime()

        appointment_rows.append(
            (
                official_id,
                appointment_type_id,
                discipline_id,
                level_id,
                appointed_date,
                achieved_date,
                mentor,
                True,
            )
        )

    appointments_sql = """
        INSERT INTO appointments
            (official_id, appointment_type_id, discipline_id, level_id,
             appointed_date, achieved_date, mentor, active)
        VALUES %s
        ON CONFLICT ON CONSTRAINT appointments_unique
        DO UPDATE SET
            appointed_date = EXCLUDED.appointed_date,
            achieved_date  = EXCLUDED.achieved_date,
            mentor         = EXCLUDED.mentor,
            active         = true
    """

    seen = {}
    for row in appointment_rows:
        key = (
            row[0],
            _norm_key_part(row[1]),
            _norm_key_part(row[2]),
            _norm_key_part(row[3]),
        )
        seen[key] = row
    appointment_rows = list(seen.values())

    import_keys = [(r[0], r[1], r[2], r[3]) for r in appointment_rows]

    deactivate_not_in_file_sql = """
        UPDATE appointments AS a
        SET active = false
        WHERE NOT EXISTS (
            SELECT 1
            FROM _directory_import_keys AS k
            WHERE a.official_id = k.official_id
              AND a.appointment_type_id IS NOT DISTINCT FROM k.appointment_type_id
              AND a.discipline_id IS NOT DISTINCT FROM k.discipline_id
              AND a.level_id IS NOT DISTINCT FROM k.level_id
        )
    """

    _log(f"Upserting {len(appointment_rows)} appointments (after dedup)...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur, appointments_sql, appointment_rows, page_size=500
            )
        conn.commit()
    _log("  done.")

    dedupe_appointments_sql = """
        DELETE FROM appointments
        WHERE id IN (
            SELECT id
            FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY official_id,
                                        appointment_type_id,
                                        discipline_id,
                                        level_id
                           ORDER BY last_modified DESC NULLS LAST,
                                    id DESC
                       ) AS rn
                FROM appointments
            ) sub
            WHERE sub.rn > 1
        )
    """

    _log(
        "Removing duplicate appointment rows "
        "(same official / type / discipline / level, NULLs match)..."
    )
    n_deduped = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(dedupe_appointments_sql)
            n_deduped = cur.rowcount
        conn.commit()
    _log(f"  done ({n_deduped} duplicate row(s) removed).")

    n_inactive = 0
    if not import_keys:
        _log(
            "Skipping inactive pass: no appointment keys in this file — "
            "would not set anything active / would overwrite data incorrectly."
        )
    else:
        _log("Marking appointments not in this file as inactive (active = false)...")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS _directory_import_keys")
                cur.execute(
                    """
                    CREATE TEMP TABLE _directory_import_keys (
                        official_id int NOT NULL,
                        appointment_type_id int,
                        discipline_id int,
                        level_id int
                    ) ON COMMIT DROP
                    """
                )
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO _directory_import_keys "
                    "(official_id, appointment_type_id, discipline_id, level_id) "
                    "VALUES %s",
                    import_keys,
                    page_size=5000,
                )
                cur.execute(deactivate_not_in_file_sql)
                n_inactive = cur.rowcount
            conn.commit()
        _log(f"  done ({n_inactive} row(s) set inactive).")

    _log("\nImport complete!")
    return {
        "logs": logs,
        "officials_upserted": len(officials_rows),
        "appointments_upserted": len(appointment_rows),
        "appointments_deduped_removed": int(n_deduped),
        "appointments_marked_inactive": int(n_inactive),
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python officials_directory_loader.py <path-to-export.xlsx>")
        sys.exit(1)
    run_officials_directory_import(sys.argv[1])
