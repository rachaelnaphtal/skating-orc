import os
import pandas as pd
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse

# -------------------------
# DB CONNECTION
# -------------------------
DATABASE_URL = os.environ["DATABASE_URL"]
parsed = urlparse(DATABASE_URL)

conn_params = {
    "host":     parsed.hostname,
    "port":     parsed.port or 5432,
    "dbname":   parsed.path.lstrip("/"),
    "user":     parsed.username,
    "password": parsed.password,
    "options": "-csearch_path=officials_analysis"
}


def get_conn():
    return psycopg2.connect(**conn_params)


# -------------------------
# LOAD EXCEL
# -------------------------
EXCEL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "/Users/rachaelnaphtal/Downloads/OfficialsDirectoryExport (19).xlsx"
)

df = pd.read_excel(EXCEL_PATH, sheet_name="Officials")

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_", regex=False)
    .str.replace("#", "num", regex=False)
)

# -------------------------
# NORMALIZE COLUMNS
# -------------------------
df["mbr_number"]       = df["mbr_num"].astype(str).str.strip()
df["is_coach"]         = df["coach"].map({"Yes": True, "No": False})
df["full_name"]        = df["first_name"].str.strip() + " " + df["last_name"].str.strip()
df["appointment_type"] = df["types"].astype(str).str.strip().replace("nan", None)
df["discipline"]       = df["disciplines"].astype(str).str.strip().replace("nan", None)
df["level"]            = df["levels"].astype(str).str.strip().replace("nan", None)
df["appointed_date"]   = pd.to_datetime(df["appointed_date"], errors="coerce")
df["achieved_date"]    = pd.to_datetime(df["achieved_date"], errors="coerce")
df["mentor"]           = df["mentor"].astype(str).str.strip().replace("nan", None).replace("", None)


def na_to_none(val):
    """Convert any pandas/numpy NA/NaN to Python None."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


# -------------------------
# UPSERT LOOKUP TABLE (name TEXT UNIQUE)
# -------------------------
def upsert_lookup(table, rows, key_col="name", extra_cols=None):
    """
    rows: list of tuples matching (key_col, *extra_cols)
    Upserts via ON CONFLICT DO UPDATE.
    """
    if not rows:
        return

    all_cols = [key_col] + (extra_cols or [])
    col_list  = ", ".join(all_cols)
    placeholders = ", ".join(["%s"] * len(all_cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in all_cols if c != key_col) or f"{key_col} = EXCLUDED.{key_col}"

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ({key_col}) DO UPDATE SET {updates}
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()


# -------------------------
# DIMENSION TABLES
# -------------------------
def name_rows(series):
    return [(v,) for v in series.dropna().drop_duplicates().tolist()]

print("Upserting lookup tables...")
upsert_lookup("appointment_types", name_rows(df["appointment_type"]))
upsert_lookup("disciplines",       name_rows(df["discipline"]))
upsert_lookup("levels",            name_rows(df["level"]))
print("  done.")

# -------------------------
# OFFICIALS TABLE
# -------------------------
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

officials_all_cols = officials_cols
col_list     = ", ".join(officials_all_cols)
placeholders = ", ".join(["%s"] * len(officials_all_cols))
updates      = ", ".join(f"{c} = EXCLUDED.{c}" for c in officials_all_cols if c != "mbr_number")

officials_sql = f"""
    INSERT INTO officials ({col_list})
    VALUES %s
    ON CONFLICT (mbr_number) DO UPDATE SET {updates}
"""

print(f"Upserting {len(officials_rows)} officials...")
with get_conn() as conn:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, officials_sql, officials_rows, page_size=500)
    conn.commit()
print("  done.")

# -------------------------
# LOAD IDS FROM DB
# -------------------------
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

# -------------------------
# BUILD APPOINTMENTS ROWS
# -------------------------
appointment_rows = []
for _, row in df.iterrows():
    official_id        = officials_map.get(row["mbr_number"])
    appointment_type_id = types_map.get(row["appointment_type"]) if row["appointment_type"] else None
    discipline_id      = disciplines_map.get(row["discipline"]) if row["discipline"] else None
    level_id           = levels_map.get(row["level"]) if row["level"] else None
    appointed_date     = na_to_none(row["appointed_date"])
    achieved_date      = na_to_none(row["achieved_date"])
    mentor             = na_to_none(row["mentor"])

    if official_id is None:
        continue

    # Convert pandas Timestamps to Python date
    if hasattr(appointed_date, "to_pydatetime"):
        appointed_date = appointed_date.to_pydatetime()
    if hasattr(achieved_date, "to_pydatetime"):
        achieved_date = achieved_date.to_pydatetime()

    appointment_rows.append((
        official_id, appointment_type_id, discipline_id, level_id,
        appointed_date, achieved_date, mentor
    ))

appointments_sql = """
    INSERT INTO appointments
        (official_id, appointment_type_id, discipline_id, level_id,
         appointed_date, achieved_date, mentor)
    VALUES %s
    ON CONFLICT ON CONSTRAINT appointments_unique
    DO UPDATE SET
        appointed_date = EXCLUDED.appointed_date,
        achieved_date  = EXCLUDED.achieved_date,
        mentor         = EXCLUDED.mentor
"""

# Deduplicate: keep the last row for each unique combination (constraint key)
seen = {}
for row in appointment_rows:
    key = (row[0], row[1], row[2], row[3])  # official_id, type_id, discipline_id, level_id
    seen[key] = row
appointment_rows = list(seen.values())

print(f"Upserting {len(appointment_rows)} appointments (after dedup)...")
with get_conn() as conn:
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, appointments_sql, appointment_rows, page_size=500)
    conn.commit()
print("  done.")

print("\nImport complete!")
