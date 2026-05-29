"""
Shared helpers for judge ↔ directory linking (CLI + Streamlit).

* USFS roster: ``officials_analysis.officials`` via ``judge_official_link``
* ISU roster: ``officials_analysis.isu_official`` via ``judge_isu_official_link``

Uses PostgreSQL with search_path public,officials_analysis — same as activity data.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine import RowMapping

try:
    from rapidfuzz import fuzz, process
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "The rapidfuzz package is required for judge ↔ directory matching "
        "(list rapidfuzz in requirements.txt / pip metadata — import unavailable)."
    ) from e

DDL_JUDGE_OFFICIAL_LINK = """
CREATE TABLE IF NOT EXISTS judge_official_link (
    judge_id INTEGER NOT NULL PRIMARY KEY
        REFERENCES judge(id) ON DELETE CASCADE,
    official_id INTEGER
        REFERENCES officials_analysis.officials(id) ON DELETE SET NULL,
    status TEXT NOT NULL
        CHECK (status IN ('linked', 'outside_directory')),
    note TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT judge_official_link_linked_requires_official
        CHECK (
            (status = 'linked' AND official_id IS NOT NULL)
            OR (status = 'outside_directory' AND official_id IS NULL)
        )
);

CREATE INDEX IF NOT EXISTS idx_judge_official_link_official_id
    ON judge_official_link (official_id)
    WHERE official_id IS NOT NULL;
"""

DDL_JUDGE_ISU_OFFICIAL_LINK = """
CREATE TABLE IF NOT EXISTS officials_analysis.isu_official (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    federation_code TEXT NOT NULL,
    full_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    name_normalized TEXT NOT NULL,
    season TEXT NOT NULL,
    communication_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT isu_official_roster_unique
        UNIQUE (federation_code, name_normalized, season)
);

CREATE TABLE IF NOT EXISTS public.isu_official_name_alias (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alias_normalized TEXT NOT NULL,
    isu_official_id INTEGER NOT NULL
        REFERENCES officials_analysis.isu_official (id) ON DELETE CASCADE,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT isu_official_name_alias_alias_normalized_key UNIQUE (alias_normalized)
);

CREATE TABLE IF NOT EXISTS public.judge_isu_official_link (
    judge_id INTEGER NOT NULL PRIMARY KEY
        REFERENCES judge(id) ON DELETE CASCADE,
    isu_official_id INTEGER NOT NULL
        REFERENCES officials_analysis.isu_official (id) ON DELETE CASCADE,
    note TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_judge_isu_official_link_isu_official_id
    ON judge_isu_official_link (isu_official_id);
"""


def database_url() -> str:
    url = (os.environ.get("DATABASE_URL") or "").strip()
    if url:
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql://", 1)
        return url
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE", "postgres")
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return f"postgresql://{user}@{host}:{port}/{database}"


def make_engine() -> Engine:
    url = database_url()
    if not url.startswith("postgresql"):
        raise RuntimeError(
            "PostgreSQL required (postgresql://…). Set DATABASE_URL to the database "
            "that has both public.judge and officials_analysis.officials."
        )
    return create_engine(
        url,
        echo=False,
        connect_args={"options": "-csearch_path=public,officials_analysis"},
    )


def ensure_table(engine: Engine) -> None:
    """Create US + ISU judge link tables if missing (idempotent)."""
    with engine.begin() as conn:
        conn.execute(text(DDL_JUDGE_OFFICIAL_LINK))
        conn.execute(text(DDL_JUDGE_ISU_OFFICIAL_LINK))


def normalize_name(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(s.lower().split()).strip()


def official_exists(engine: Engine, official_id: int) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(
                text("SELECT 1 FROM officials_analysis.officials WHERE id = :id"),
                {"id": official_id},
            ).first()
            is not None
        )


def fetch_official_choices(conn: Connection) -> dict[int, str]:
    rows = conn.execute(
        text(
            """
            SELECT id,
                TRIM(full_name) || CASE
                    WHEN mbr_number IS NOT NULL AND TRIM(mbr_number) <> ''
                    THEN ' [' || TRIM(mbr_number) || ']'
                    ELSE ''
                END AS label
            FROM officials_analysis.officials
            WHERE full_name IS NOT NULL AND TRIM(full_name) <> ''
            ORDER BY id
            """
        )
    ).mappings().all()
    return {int(r["id"]): str(r["label"]) for r in rows}


def isu_official_exists(engine: Engine, isu_official_id: int) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(
                text("SELECT 1 FROM officials_analysis.isu_official WHERE id = :id"),
                {"id": isu_official_id},
            ).first()
            is not None
        )


def fetch_isu_official_choices(conn: Connection) -> dict[int, str]:
    rows = conn.execute(
        text(
            """
            SELECT id,
                TRIM(full_name) || ' [' || TRIM(federation_code) || ']'
                || ' · ' || TRIM(season) AS label
            FROM officials_analysis.isu_official
            WHERE full_name IS NOT NULL AND TRIM(full_name) <> ''
            ORDER BY federation_code, full_name, id
            """
        )
    ).mappings().all()
    return {int(r["id"]): str(r["label"]) for r in rows}


def official_select_display_maps(
    labels: dict[int, str],
) -> tuple[list[str], dict[str, int], dict[int, str]]:
    """
    Build UI strings for picking an official by directory name (not raw id).

    Returns ``(select_options, display_to_id, id_to_display)`` where ``select_options``
    starts with ``""`` (no selection), then sorted display strings. Duplicate
    ``labels`` values get `` · id {official_id}`` so each row maps uniquely.
    """
    by_label: dict[str, list[int]] = {}
    for oid, lbl in labels.items():
        by_label.setdefault(lbl, []).append(int(oid))
    display_to_id: dict[str, int] = {}
    id_to_display: dict[int, str] = {}
    for lbl, oids in by_label.items():
        oids_sorted = sorted(oids)
        for oid in oids_sorted:
            if len(oids_sorted) > 1:
                disp = f"{lbl} · id {oid}"
            else:
                disp = lbl
            display_to_id[disp] = oid
            id_to_display[oid] = disp
    options = [""] + sorted(display_to_id.keys(), key=str.casefold)
    return options, display_to_id, id_to_display


def isu_official_select_display_maps(
    labels: dict[int, str],
) -> tuple[list[str], dict[str, int], dict[int, str]]:
    """Same as ``official_select_display_maps`` for ISU roster labels."""
    return official_select_display_maps(labels)


def fetch_unmapped_judges(conn: Connection, limit: int | None = None) -> list[RowMapping]:
    lim_sql = " LIMIT :lim" if limit is not None else ""
    q = text(
        f"""
        SELECT j.id, j.name, j.location
        FROM judge j
        LEFT JOIN judge_official_link l ON l.judge_id = j.id
        WHERE l.judge_id IS NULL
        ORDER BY lower(j.name), j.id
        {lim_sql}
        """
    )
    params: dict[str, Any] = {}
    if limit is not None:
        params["lim"] = limit
    return list(conn.execute(q, params).mappings().all())


def fetch_judges_needing_link(conn: Connection, limit: int | None = None) -> list[RowMapping]:
    """
    Judges without a US ``linked`` row and without an ISU link row.

    Includes judges with no ``judge_official_link`` row or ``outside_directory`` status.
    """
    lim_sql = " LIMIT :lim" if limit is not None else ""
    q = text(
        f"""
        SELECT j.id, j.name, j.location
        FROM judge j
        LEFT JOIN judge_official_link l ON l.judge_id = j.id
        LEFT JOIN judge_isu_official_link il ON il.judge_id = j.id
        WHERE il.judge_id IS NULL
          AND (l.judge_id IS NULL OR l.status <> 'linked')
        ORDER BY lower(j.name), j.id
        {lim_sql}
        """
    )
    params: dict[str, Any] = {}
    if limit is not None:
        params["lim"] = limit
    return list(conn.execute(q, params).mappings().all())


def suggest_matches(
    protocol_name: str | None,
    choices: dict[int, str],
    *,
    top: int = 8,
    min_score: float = 0.0,
) -> list[tuple[int, float, str]]:
    """Return (official_id, score, label) best-first using token_set_ratio."""
    if not choices:
        return []
    query = normalize_name(protocol_name or "")
    if not query:
        return []
    extracted = process.extract(
        query,
        {oid: normalize_name(lbl) for oid, lbl in choices.items()},
        scorer=fuzz.token_set_ratio,
        limit=top,
    )
    out: list[tuple[int, float, str]] = []
    # For dict inputs, RapidFuzz returns tuples as: (choice_value, score, key).
    # Here, key is our official_id.
    for _choice_value, score, official_id in extracted:
        if score < min_score:
            continue
        oid = int(official_id)
        out.append((oid, float(score), choices.get(oid, "?")))
    return out


def upsert_link(
    engine: Engine,
    judge_id: int,
    official_id: int,
    note: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO judge_official_link (judge_id, official_id, status, note, updated_at)
                VALUES (:jid, :oid, 'linked', :note, :ts)
                ON CONFLICT (judge_id) DO UPDATE SET
                    official_id = EXCLUDED.official_id,
                    status = 'linked',
                    note = COALESCE(EXCLUDED.note, judge_official_link.note),
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {"jid": judge_id, "oid": official_id, "note": note, "ts": now},
        )


def upsert_outside(engine: Engine, judge_id: int, note: str | None = None) -> None:
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO judge_official_link (judge_id, official_id, status, note, updated_at)
                VALUES (:jid, NULL, 'outside_directory', :note, :ts)
                ON CONFLICT (judge_id) DO UPDATE SET
                    official_id = NULL,
                    status = 'outside_directory',
                    note = COALESCE(EXCLUDED.note, judge_official_link.note),
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {"jid": judge_id, "note": note, "ts": now},
        )


def auto_link_by_score(
    engine: Engine,
    *,
    officials: dict[int, str],
    min_score: float,
    limit_judges: int | None = 5000,
) -> tuple[int, int]:
    """
    For each unmapped judge, if top fuzzy match >= min_score, write link.
    Returns (linked_count, skipped_count).
    """
    linked = 0
    skipped = 0
    with engine.connect() as conn:
        judges = fetch_unmapped_judges(conn, limit=limit_judges)
    for j in judges:
        matches = suggest_matches(j["name"], officials, top=1, min_score=min_score)
        if not matches:
            skipped += 1
            continue
        oid, _score, _ = matches[0]
        upsert_link(engine, int(j["id"]), oid, note=None)
        linked += 1
    return linked, skipped


def upsert_isu_link(
    engine: Engine,
    judge_id: int,
    isu_official_id: int,
    note: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO judge_isu_official_link (judge_id, isu_official_id, note, updated_at)
                VALUES (:jid, :ioid, :note, :ts)
                ON CONFLICT (judge_id) DO UPDATE SET
                    isu_official_id = EXCLUDED.isu_official_id,
                    note = COALESCE(EXCLUDED.note, judge_isu_official_link.note),
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {"jid": judge_id, "ioid": isu_official_id, "note": note, "ts": now},
        )


def auto_link_isu_by_score(
    engine: Engine,
    *,
    isu_officials: dict[int, str],
    min_score: float,
    limit_judges: int | None = 5000,
) -> tuple[int, int]:
    """
    For judges still needing a link, if top ISU fuzzy match >= min_score, write ISU link.
    Skips judges that already have a US linked official.
    """
    linked = 0
    skipped = 0
    with engine.connect() as conn:
        judges = fetch_judges_needing_link(conn, limit=limit_judges)
    for j in judges:
        matches = suggest_matches(j["name"], isu_officials, top=1, min_score=min_score)
        if not matches:
            skipped += 1
            continue
        ioid, _score, _ = matches[0]
        upsert_isu_link(engine, int(j["id"]), ioid, note=None)
        linked += 1
    return linked, skipped
