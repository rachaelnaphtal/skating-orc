#!/usr/bin/env python3
"""
Admin CLI: link judging `judge` rows to `officials_analysis.officials` with fuzzy hints.

Requires PostgreSQL (same DATABASE_URL as the main app). Run once:

    python scripts/judge_official_admin.py init-db

Bulk links (CSV): prepare a file with columns judge_id, official_id, optional note, then:

    python scripts/judge_official_admin.py unmapped-export > to_link.csv
    # edit in Excel / Sheets, add official_id (and note), save as CSV UTF-8
    python scripts/judge_official_admin.py batch-link to_link.csv

Environment: DATABASE_URL, or PGUSER/PGPASSWORD/PGHOST/PGPORT/PGDATABASE.

Web UI (bulk approve + auto-link): ``streamlit run judge_official_matcher_app.py``
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Sequence

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import judge_official_link_core as jol_core

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _engine() -> Engine:
    try:
        return jol_core.make_engine()
    except RuntimeError as e:
        raise SystemExit(str(e)) from e


def cmd_init_db(engine: Engine) -> None:
    jol_core.ensure_table(engine)
    print("OK: judge_official_link and ISU directory link tables are ready.")


def cmd_unmapped(engine: Engine, limit: int) -> None:
    q = text(
        """
        SELECT j.id, j.name, j.location
        FROM judge j
        LEFT JOIN judge_official_link l ON l.judge_id = j.id
        WHERE l.judge_id IS NULL
        ORDER BY lower(j.name), j.id
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, {"lim": limit}).mappings().all()
    if not rows:
        print("No unmapped judges (every judge has a link row).")
        return
    print(f"{'id':>8}  {'name':40}  location")
    print("-" * 72)
    for r in rows:
        loc = (r["location"] or "")[:30]
        name = (r["name"] or "")[:40]
        print(f"{r['id']:>8}  {name:40}  {loc}")


def cmd_suggest(engine: Engine, judge_id: int, top: int, min_score: float) -> None:
    with engine.connect() as conn:
        j = conn.execute(
            text("SELECT id, name, location FROM judge WHERE id = :id"),
            {"id": judge_id},
        ).mappings().first()
        if not j:
            print(f"No judge with id={judge_id}", file=sys.stderr)
            raise SystemExit(2)
        link = conn.execute(
            text(
                "SELECT status, official_id, note FROM judge_official_link "
                "WHERE judge_id = :id"
            ),
            {"id": judge_id},
        ).mappings().first()
        choices = jol_core.fetch_official_choices(conn)

    protocol = j["name"] or ""
    print(f"Judge id={j['id']}  name={protocol!r}  location={j['location']!r}")
    if link:
        print(
            f"Existing link: status={link['status']} official_id={link['official_id']} "
            f"note={link['note']!r}"
        )

    if not choices:
        print("No officials with full_name in directory.")
        return

    matches = jol_core.suggest_matches(protocol, choices, top=top, min_score=min_score)
    print(f"\nTop matches (token_set_ratio, min_score={min_score}):")
    if not matches:
        print("  (none above min_score — try lowering --min-score)")
        return
    for official_id, score, label in matches:
        print(f"  {score:5.1f}  official_id={official_id}  {label}")


def cmd_link(engine: Engine, judge_id: int, official_id: int, note: str | None) -> None:
    jol_core.upsert_link(engine, judge_id, official_id, note)
    print(f"OK: judge {judge_id} linked to official {official_id}.")


def cmd_mark_outside(engine: Engine, judge_id: int, note: str | None) -> None:
    jol_core.upsert_outside(engine, judge_id, note)
    print(
        f"OK: judge {judge_id} marked outside_directory "
        f"(no US directory row expected; fuzzy suggestions can skip this id)."
    )


def cmd_clear(engine: Engine, judge_id: int) -> None:
    with engine.begin() as conn:
        r = conn.execute(
            text("DELETE FROM judge_official_link WHERE judge_id = :id RETURNING judge_id"),
            {"id": judge_id},
        )
        if r.rowcount == 0:
            print(f"No link row for judge_id={judge_id}.")
            return
    print(f"OK: removed link row for judge {judge_id} (judge is unmapped again).")


def cmd_show(engine: Engine, judge_id: int) -> None:
    with engine.connect() as conn:
        j = conn.execute(
            text("SELECT id, name, location FROM judge WHERE id = :id"),
            {"id": judge_id},
        ).mappings().first()
        if not j:
            print(f"No judge with id={judge_id}", file=sys.stderr)
            raise SystemExit(2)
        link = conn.execute(
            text(
                """
                SELECT l.status, l.official_id, l.note, l.updated_at,
                       o.full_name, o.mbr_number, o.region
                FROM judge_official_link l
                LEFT JOIN officials_analysis.officials o ON o.id = l.official_id
                WHERE l.judge_id = :id
                """
            ),
            {"id": judge_id},
        ).mappings().first()
    print(f"judge id={j['id']}  name={j['name']!r}  location={j['location']!r}")
    if not link:
        print("Link: (none — unmapped)")
        return
    print(
        f"Link: status={link['status']} official_id={link['official_id']} "
        f"updated_at={link['updated_at']}"
    )
    print(f"  note: {link['note']!r}")
    if link["full_name"]:
        print(
            f"  official: {link['full_name']!r}  mbr={link['mbr_number']!r}  region={link['region']!r}"
        )


def cmd_list_linked(engine: Engine, limit: int) -> None:
    q = text(
        """
        SELECT l.judge_id, j.name AS judge_name, l.official_id, o.full_name, l.note, l.updated_at
        FROM judge_official_link l
        JOIN judge j ON j.id = l.judge_id
        LEFT JOIN officials_analysis.officials o ON o.id = l.official_id
        WHERE l.status = 'linked'
        ORDER BY l.updated_at DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, {"lim": limit}).mappings().all()
    if not rows:
        print("No linked rows.")
        return
    for r in rows:
        print(
            f"judge_id={r['judge_id']} {r['judge_name']!r} -> "
            f"official_id={r['official_id']} {r['full_name']!r}  ({r['updated_at']})"
        )


def _normalize_csv_header(h: str) -> str:
    return h.strip().lower().replace(" ", "_")


def _read_csv_rows(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"CSV has no header row: {path}")
        header_norm = {_normalize_csv_header(h) for h in reader.fieldnames}
        if "judge_id" not in header_norm:
            raise SystemExit(
                f"CSV must include a judge_id column (found: {list(reader.fieldnames)}). "
                "Use unmapped-export or a header like judge_id / Judge ID."
            )
        rows: list[dict[str, str]] = []
        for row in reader:
            if row is None:
                continue
            if not any((v or "").strip() for v in row.values()):
                continue
            rows.append(dict(row))
    return rows


def _cell(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        for hk, hv in row.items():
            if _normalize_csv_header(hk) == k:
                return (hv or "").strip()
    return ""


def _parse_int_id(s: str) -> int:
    """Parse judge/official id from CSV; tolerates Excel numeric cells saved as 1234.0."""
    t = (s or "").strip()
    if "." in t:
        return int(float(t))
    return int(t)


def cmd_unmapped_export(engine: Engine) -> None:
    """Print CSV (stdout): judge_id,name,location for unmapped judges — fill official_id and re-import with batch-link."""
    q = text(
        """
        SELECT j.id AS judge_id, j.name, j.location
        FROM judge j
        LEFT JOIN judge_official_link l ON l.judge_id = j.id
        WHERE l.judge_id IS NULL
        ORDER BY lower(j.name), j.id
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q).mappings().all()
    w = csv.writer(sys.stdout, lineterminator="\n")
    w.writerow(["judge_id", "official_id", "note", "name", "location"])
    for r in rows:
        w.writerow(
            [
                r["judge_id"],
                "",  # fill in official_id in your spreadsheet
                "",
                r["name"] or "",
                r["location"] or "",
            ]
        )


def cmd_batch_link(
    engine: Engine,
    path: str,
    *,
    dry_run: bool,
    fail_fast: bool,
) -> None:
    rows = _read_csv_rows(path)
    if not rows:
        print("No data rows in CSV.", file=sys.stderr)
        raise SystemExit(2)

    ok = 0
    failed: list[str] = []

    for idx, row in enumerate(rows, start=2):
        jid_s = _cell(row, "judge_id")
        oid_s = _cell(row, "official_id")
        note = _cell(row, "note") or None
        if not jid_s:
            msg = f"line {idx}: missing judge_id"
            failed.append(msg)
            if fail_fast:
                raise SystemExit(msg)
            continue
        if not oid_s:
            msg = f"line {idx}: missing official_id (leave row out or fill official_id)"
            failed.append(msg)
            if fail_fast:
                raise SystemExit(msg)
            continue
        try:
            judge_id = _parse_int_id(jid_s)
            official_id = _parse_int_id(oid_s)
        except ValueError as e:
            msg = f"line {idx}: invalid judge_id or official_id: {e}"
            failed.append(msg)
            if fail_fast:
                raise SystemExit(msg)
            continue

        with engine.connect() as conn:
            if not conn.execute(
                text("SELECT 1 FROM judge WHERE id = :id"), {"id": judge_id}
            ).first():
                msg = f"line {idx}: no judge with id={judge_id}"
                failed.append(msg)
                if fail_fast:
                    raise SystemExit(msg)
                continue
            if not conn.execute(
                text("SELECT 1 FROM officials_analysis.officials WHERE id = :id"), {"id": official_id}
            ).first():
                msg = f"line {idx}: no official with id={official_id}"
                failed.append(msg)
                if fail_fast:
                    raise SystemExit(msg)
                continue

        if dry_run:
            print(f"would link judge_id={judge_id} official_id={official_id} note={note!r}")
            ok += 1
            continue

        jol_core.upsert_link(engine, judge_id, official_id, note)
        ok += 1

    dup_note = ""
    paired_jids: list[int] = []
    for r in rows:
        if _cell(r, "judge_id") and _cell(r, "official_id"):
            try:
                paired_jids.append(_parse_int_id(_cell(r, "judge_id")))
            except ValueError:
                pass
    if len(paired_jids) != len(set(paired_jids)):
        dup_note = " (duplicate judge_id in file: last successful row wins)"

    print(f"batch-link: {ok} row(s) {'validated' if dry_run else 'applied'}.{dup_note}")
    for msg in failed:
        print(f"  ERROR {msg}", file=sys.stderr)
    if failed:
        raise SystemExit(1)


def cmd_batch_mark_outside(
    engine: Engine,
    path: str,
    *,
    dry_run: bool,
    fail_fast: bool,
) -> None:
    rows = _read_csv_rows(path)
    if not rows:
        print("No data rows in CSV.", file=sys.stderr)
        raise SystemExit(2)

    ok = 0
    failed: list[str] = []

    for idx, row in enumerate(rows, start=2):
        jid_s = _cell(row, "judge_id")
        note = _cell(row, "note") or None
        if not jid_s:
            msg = f"line {idx}: missing judge_id"
            failed.append(msg)
            if fail_fast:
                raise SystemExit(msg)
            continue
        try:
            judge_id = _parse_int_id(jid_s)
        except ValueError as e:
            msg = f"line {idx}: invalid judge_id: {e}"
            failed.append(msg)
            if fail_fast:
                raise SystemExit(msg)
            continue

        with engine.connect() as conn:
            if not conn.execute(
                text("SELECT 1 FROM judge WHERE id = :id"), {"id": judge_id}
            ).first():
                msg = f"line {idx}: no judge with id={judge_id}"
                failed.append(msg)
                if fail_fast:
                    raise SystemExit(msg)
                continue

        if dry_run:
            print(f"would mark-outside judge_id={judge_id} note={note!r}")
            ok += 1
            continue

        jol_core.upsert_outside(engine, judge_id, note)
        ok += 1

    print(f"batch-mark-outside: {ok} row(s) {'validated' if dry_run else 'applied'}.")
    for msg in failed:
        print(f"  ERROR {msg}", file=sys.stderr)
    if failed:
        raise SystemExit(1)


def cmd_list_outside(engine: Engine, limit: int) -> None:
    q = text(
        """
        SELECT l.judge_id, j.name, l.note, l.updated_at
        FROM judge_official_link l
        JOIN judge j ON j.id = l.judge_id
        WHERE l.status = 'outside_directory'
        ORDER BY l.updated_at DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, {"lim": limit}).mappings().all()
    if not rows:
        print("No outside_directory rows.")
        return
    for r in rows:
        print(f"judge_id={r['judge_id']} name={r['name']!r} note={r['note']!r} ({r['updated_at']})")


def cmd_load_isu_pdf(
    engine: Engine,
    source: str,
    *,
    season: str,
    communication_ref: str | None,
    output: str | None,
    limit: int | None,
    dry_run: bool,
) -> None:
    """Parse and load an ISU officials PDF using ``load_isu_officials_pdf.py``."""
    from scripts import load_isu_officials_pdf as isu_loader

    rows = isu_loader.parse_pdf_source(
        source,
        season=season,
        communication_ref=communication_ref or "",
        limit=limit,
    )
    if output:
        isu_loader.write_csv(output, rows)
        print(f"Wrote {len(rows)} parsed ISU officials to {output}.")
    count = isu_loader.load_rows(rows, dry_run=dry_run, engine=engine)
    action = "planned" if dry_run else "loaded"
    print(f"{action} {count if not dry_run else len(rows)} ISU officials.")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Link judges to officials directory (admin CLI).")
    p.add_argument(
        "--database-url",
        dest="database_url",
        help="Override DATABASE_URL for this run only.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create judge_official_link if missing.")

    p_un = sub.add_parser("unmapped", help="List judges with no link row.")
    p_un.add_argument("--limit", type=int, default=100)

    p_su = sub.add_parser("suggest", help="Fuzzy-match one judge name to directory officials.")
    p_su.add_argument("judge_id", type=int)
    p_su.add_argument("--top", type=int, default=12)
    p_su.add_argument("--min-score", type=float, default=75.0)

    p_ln = sub.add_parser("link", help="Set linked official for a judge.")
    p_ln.add_argument("judge_id", type=int)
    p_ln.add_argument("official_id", type=int)
    p_ln.add_argument("--note", default=None)

    p_out = sub.add_parser(
        "mark-outside",
        help="Mark judge as not in US directory (intl / no roster row); clears official_id.",
    )
    p_out.add_argument("judge_id", type=int)
    p_out.add_argument("--note", default=None)

    p_cl = sub.add_parser("clear", help="Delete link row so judge is unmapped again.")
    p_cl.add_argument("judge_id", type=int)

    p_sh = sub.add_parser("show", help="Show judge + link + official names.")
    p_sh.add_argument("judge_id", type=int)

    p_ll = sub.add_parser("list-linked", help="Recent linked mappings.")
    p_ll.add_argument("--limit", type=int, default=200)

    p_lo = sub.add_parser("list-outside", help="Recent outside_directory marks.")
    p_lo.add_argument("--limit", type=int, default=200)

    p_ue = sub.add_parser(
        "unmapped-export",
        help="Print CSV to stdout: unmapped judges with empty official_id column to fill, then batch-link.",
    )

    p_bl = sub.add_parser(
        "batch-link",
        help="Apply many links from a CSV (columns: judge_id, official_id, optional note).",
    )
    p_bl.add_argument("csv_path", help="Path to UTF-8 CSV (e.g. from unmapped-export, edited).")
    p_bl.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print actions without writing to the database.",
    )
    p_bl.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first error instead of processing the rest of the file.",
    )

    p_bm = sub.add_parser(
        "batch-mark-outside",
        help="Mark many judges outside_directory from CSV (columns: judge_id, optional note).",
    )
    p_bm.add_argument("csv_path")
    p_bm.add_argument("--dry-run", action="store_true")
    p_bm.add_argument("--fail-fast", action="store_true")

    p_isu = sub.add_parser(
        "load-isu-pdf",
        help="Parse/load an ISU officials communication PDF into the ISU roster table.",
    )
    p_isu.add_argument("source", help="ISU officials PDF URL or local PDF path")
    p_isu.add_argument("--season", required=True, help="Roster season, e.g. 2526")
    p_isu.add_argument("--communication-ref", default=None, help="Communication number, e.g. 2735")
    p_isu.add_argument("-o", "--output", default=None, help="Optional parsed CSV output path")
    p_isu.add_argument("--limit", type=int, default=None, help="Limit parsed rows for testing")
    p_isu.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and print planned rows without writing to the database.",
    )

    return p


def main(argv: Sequence[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = _build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "database_url", None):
        os.environ["DATABASE_URL"] = args.database_url

    engine = _engine()
    cmd = args.command
    if cmd == "init-db":
        cmd_init_db(engine)
    elif cmd == "unmapped":
        cmd_unmapped(engine, args.limit)
    elif cmd == "suggest":
        cmd_suggest(engine, args.judge_id, args.top, args.min_score)
    elif cmd == "link":
        cmd_link(engine, args.judge_id, args.official_id, args.note)
    elif cmd == "mark-outside":
        cmd_mark_outside(engine, args.judge_id, args.note)
    elif cmd == "clear":
        cmd_clear(engine, args.judge_id)
    elif cmd == "show":
        cmd_show(engine, args.judge_id)
    elif cmd == "list-linked":
        cmd_list_linked(engine, args.limit)
    elif cmd == "list-outside":
        cmd_list_outside(engine, args.limit)
    elif cmd == "unmapped-export":
        cmd_unmapped_export(engine)
    elif cmd == "batch-link":
        cmd_batch_link(engine, args.csv_path, dry_run=args.dry_run, fail_fast=args.fail_fast)
    elif cmd == "batch-mark-outside":
        cmd_batch_mark_outside(
            engine, args.csv_path, dry_run=args.dry_run, fail_fast=args.fail_fast
        )
    elif cmd == "load-isu-pdf":
        cmd_load_isu_pdf(
            engine,
            args.source,
            season=args.season,
            communication_ref=args.communication_ref,
            output=args.output,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    else:  # pragma: no cover
        parser.error(f"Unknown command {cmd!r}")


if __name__ == "__main__":
    main()
