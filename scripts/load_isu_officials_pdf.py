#!/usr/bin/env python3
"""
Load ISU communication PDF official rosters into ``officials_analysis.isu_official``.

The PDF text frequently places section headers and names on the same line, for example:

    REFEREE & JUDGE Andrew Rebecca, Ms.
    ISU Judge Alexandre Elizabeth, Ms. International Judge Andrew Rebecca, Ms.

This parser treats federation lines (``AUS - AUSTRALIA``) as headers, strips known
category / role headers from each name segment, and only emits segments ending in an
official title such as ``, Ms.`` or ``, Mr.``.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import pdfplumber
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


FEDERATION_RE = re.compile(r"^([A-Z]{3})\s*[-–]\s+(.+?)\s*$")
TITLE_RE = re.compile(r",\s*(?:Mr|Ms|Mrs|Miss)\.", re.IGNORECASE)

HEADER_PREFIXES = (
    "SINGLE & PAIR SKATING",
    "SYNCHRONIZED SKATING",
    "ICE DANCE",
    "DATA & REPLAY OPERATOR - ALL CATEGORIES",
    "TECHNICAL CONTROLLER - SINGLE",
    "TECHNICAL CONTROLLER - PAIR",
    "TECHNICAL SPECIALIST - SINGLE",
    "TECHNICAL SPECIALIST - PAIR",
    "TECHNICAL CONTROLLER",
    "TECHNICAL SPECIALIST",
    "REFEREE & JUDGE",
    "ISU DATA & REPLAY OPERATOR",
    "INTERNATIONAL DATA & REPLAY OPERATOR",
    "ISU TECHNICAL CONTROLLER",
    "INTERNATIONAL TECHNICAL CONTROLLER",
    "ISU TECHNICAL SPECIALIST",
    "INTERNATIONAL TECHNICAL SPECIALIST",
    "ISU REFEREE",
    "INTERNATIONAL REFEREE",
    "ISU JUDGE",
    "INTERNATIONAL JUDGE",
)

HEADER_WORD_RE = re.compile(
    r"\b(?:SKATING|REFEREE|JUDGE|TECHNICAL|CONTROLLER|SPECIALIST|OPERATOR|CATEGORY|CATEGORIES)\b",
    re.IGNORECASE,
)

DISCIPLINE_HEADERS = {
    "SINGLE & PAIR SKATING": "Single & Pair Skating",
    "ICE DANCE": "Ice Dance",
    "SYNCHRONIZED SKATING": "Synchronized Skating",
    "DATA & REPLAY OPERATOR - ALL CATEGORIES": "All Categories",
}

ROLE_HEADERS = (
    ("ISU DATA & REPLAY OPERATOR", "Data & Replay Operator", "ISU"),
    ("INTERNATIONAL DATA & REPLAY OPERATOR", "Data & Replay Operator", "International"),
    ("ISU TECHNICAL CONTROLLER", "Technical Controller", "ISU"),
    ("INTERNATIONAL TECHNICAL CONTROLLER", "Technical Controller", "International"),
    ("ISU TECHNICAL SPECIALIST", "Technical Specialist", "ISU"),
    ("INTERNATIONAL TECHNICAL SPECIALIST", "Technical Specialist", "International"),
    ("ISU REFEREE", "Referee", "ISU"),
    ("INTERNATIONAL REFEREE", "Referee", "International"),
    ("ISU JUDGE", "Judge", "ISU"),
    ("INTERNATIONAL JUDGE", "Judge", "International"),
)

APPOINTMENT_GROUP_HEADERS = {
    "REFEREE & JUDGE": "",
    "TECHNICAL CONTROLLER - SINGLE": "Technical Controller",
    "TECHNICAL CONTROLLER - PAIR": "Technical Controller",
    "TECHNICAL SPECIALIST - SINGLE": "Technical Specialist",
    "TECHNICAL SPECIALIST - PAIR": "Technical Specialist",
    "TECHNICAL CONTROLLER": "Technical Controller",
    "TECHNICAL SPECIALIST": "Technical Specialist",
}


@dataclass(frozen=True)
class IsuOfficialRow:
    federation_code: str
    federation_name: str
    full_name: str
    first_name: str
    last_name: str
    name_normalized: str
    season: str
    communication_ref: str
    disciplines: str
    appointment_types: str
    levels: str


@dataclass(frozen=True)
class NameEntry:
    full_name: str
    discipline: str
    appointment_type: str
    level: str


def normalize_name(value: str | None) -> str:
    return " ".join((value or "").lower().split()).strip()


def _clean_line(line: str) -> str:
    line = line.replace("\u00a0", " ")
    return " ".join(line.split())


def _strip_header_prefixes(segment: str) -> str:
    out = _clean_line(segment)
    changed = True
    while changed:
        changed = False
        upper = out.upper()
        for prefix in HEADER_PREFIXES:
            if upper == prefix:
                return ""
            if upper.startswith(prefix + " "):
                out = out[len(prefix) :].strip()
                changed = True
                break
    return out


def _split_federation_header(line: str) -> tuple[str, str, str] | None:
    match = FEDERATION_RE.match(line)
    if not match:
        return None
    code = match.group(1)
    rest = match.group(2).strip()
    tail = ""
    for idx, ch in enumerate(rest):
        if ch.isalpha() and ch.islower():
            start = rest.rfind(" ", 0, idx) + 1
            tail = rest[start:].strip()
            break
    federation_name = rest[: len(rest) - len(tail)].strip() if tail else rest
    return code, federation_name, tail


def _looks_like_name(value: str) -> bool:
    if not value or len(value) < 3:
        return False
    if HEADER_WORD_RE.search(value):
        return False
    letters = [ch for ch in value if ch.isalpha()]
    if len(letters) < 3:
        return False
    return any(ch.islower() for ch in letters)


def split_surname_first_name(raw_name: str) -> tuple[str, str]:
    """Best-effort split for ISU roster order, usually ``Family Given``."""
    name = _clean_line(raw_name)
    if "," in name:
        last, first = [part.strip() for part in name.split(",", 1)]
        return first, last

    tokens = name.split()
    if len(tokens) < 2:
        return "", name

    # Keep common lowercase family-name particles with the surname.
    split_idx = 2 if tokens[0][0].islower() and len(tokens) > 2 else 1
    while split_idx < len(tokens) - 1 and tokens[split_idx][0].islower():
        split_idx += 1
    last = " ".join(tokens[:split_idx])
    first = " ".join(tokens[split_idx:])
    return first, last


def _role_prefix(segment: str) -> tuple[str, str, str]:
    candidate = _clean_line(segment)
    upper = candidate.upper()
    for header, appointment_type, level in ROLE_HEADERS:
        if upper == header:
            return "", appointment_type, level
        if upper.startswith(header + " "):
            return candidate[len(header) :].strip(), appointment_type, level
    return candidate, "", ""


def _line_discipline(line: str) -> str:
    upper = line.upper()
    for header, discipline in DISCIPLINE_HEADERS.items():
        if header in upper:
            return discipline
    return ""


def _line_appointment_group(line: str) -> str:
    upper = line.upper()
    for header, appointment_type in APPOINTMENT_GROUP_HEADERS.items():
        if header in upper:
            return appointment_type
    return ""


def _extract_name_entries_from_line(
    line: str,
    *,
    discipline: str = "",
    appointment_type: str = "",
    level: str = "",
) -> list[NameEntry]:
    line = _clean_line(line)
    names: list[NameEntry] = []
    prev_end = 0
    for match in TITLE_RE.finditer(line):
        segment = line[prev_end : match.start()]
        prev_end = match.end()
        candidate, role_appointment_type, role_level = _role_prefix(segment)
        candidate = _strip_header_prefixes(candidate)
        candidate = re.sub(r"\s*\(deceased\)\s*$", "", candidate, flags=re.IGNORECASE)
        if _looks_like_name(candidate):
            names.append(
                NameEntry(
                    full_name=candidate,
                    discipline=discipline,
                    appointment_type=role_appointment_type or appointment_type,
                    level=role_level or level,
                )
            )
    return names


def _extract_names_from_line(line: str) -> list[str]:
    return [entry.full_name for entry in _extract_name_entries_from_line(line)]


def _append_unique(values: list[str], value: str) -> None:
    clean = (value or "").strip()
    if clean and clean not in values:
        values.append(clean)


def parse_isu_official_text(
    text: str, *, season: str, communication_ref: str
) -> list[IsuOfficialRow]:
    parsed: dict[tuple[str, str, str], dict[str, object]] = {}
    federation_code = ""
    federation_name = ""
    current_discipline = ""
    current_appointment_type = ""
    current_level = ""

    for raw_line in text.splitlines():
        line = _clean_line(raw_line)
        if not line:
            continue

        fed_match = _split_federation_header(line)
        if fed_match:
            federation_code, federation_name, tail = fed_match
            current_discipline = ""
            current_appointment_type = ""
            current_level = ""
            line = tail
            if not line:
                continue

        if not federation_code:
            continue

        line_discipline = _line_discipline(line)
        if line_discipline:
            current_discipline = line_discipline
        line_appointment_type = _line_appointment_group(line)
        if line_appointment_type:
            current_appointment_type = line_appointment_type
        role_line_text, role_appointment_type, role_level = _role_prefix(line)
        if not role_line_text and (role_appointment_type or role_level):
            current_appointment_type = role_appointment_type or current_appointment_type
            current_level = role_level or current_level
            continue

        entries = _extract_name_entries_from_line(
            line,
            discipline=current_discipline,
            appointment_type=current_appointment_type,
            level=current_level,
        )
        for entry in entries:
            normalized = normalize_name(entry.full_name)
            key = (federation_code, normalized, season)
            if key not in parsed:
                first, last = split_surname_first_name(entry.full_name)
                parsed[key] = {
                    "federation_code": federation_code,
                    "federation_name": federation_name,
                    "full_name": entry.full_name,
                    "first_name": first,
                    "last_name": last,
                    "name_normalized": normalized,
                    "season": season,
                    "communication_ref": communication_ref,
                    "disciplines": [],
                    "appointment_types": [],
                    "levels": [],
                }
            row_data = parsed[key]
            _append_unique(row_data["disciplines"], entry.discipline)  # type: ignore[arg-type]
            _append_unique(
                row_data["appointment_types"], entry.appointment_type  # type: ignore[arg-type]
            )
            _append_unique(row_data["levels"], entry.level)  # type: ignore[arg-type]

    rows: list[IsuOfficialRow] = []
    for row_data in parsed.values():
        rows.append(
            IsuOfficialRow(
                federation_code=str(row_data["federation_code"]),
                federation_name=str(row_data["federation_name"]),
                full_name=str(row_data["full_name"]),
                first_name=str(row_data["first_name"]),
                last_name=str(row_data["last_name"]),
                name_normalized=str(row_data["name_normalized"]),
                season=str(row_data["season"]),
                communication_ref=str(row_data["communication_ref"]),
                disciplines=",".join(row_data["disciplines"]),  # type: ignore[arg-type]
                appointment_types=",".join(row_data["appointment_types"]),  # type: ignore[arg-type]
                levels=",".join(row_data["levels"]),  # type: ignore[arg-type]
            )
        )
    return rows


def read_pdf_bytes(source: str) -> bytes:
    if source.startswith(("http://", "https://")):
        r = requests.get(source, timeout=60)
        r.raise_for_status()
        return r.content
    return Path(source).read_bytes()


def pdf_text(pdf_bytes: bytes) -> str:
    pages: list[str] = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text(x_tolerance=1, y_tolerance=3) or "")
    return "\n".join(pages)


def infer_communication_ref(source: str) -> str:
    name = Path(urlparse(source).path).name or source
    match = re.search(r"(?:Communication[-_ ]*)?(\d{4})", name, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def write_csv(path: str, rows: list[IsuOfficialRow]) -> None:
    fieldnames = [
        "federation_code",
        "federation_name",
        "full_name",
        "first_name",
        "last_name",
        "name_normalized",
        "season",
        "communication_ref",
        "disciplines",
        "appointment_types",
        "levels",
    ]
    if path == "-":
        out = sys.stdout
        close = False
    else:
        out = open(path, "w", newline="", encoding="utf-8")
        close = True
    try:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)
        out.flush()
    finally:
        if close:
            out.close()


def merge_csv_values(existing: str | None, new_value: str | None) -> str:
    """Append ``new_value`` to a comma-separated text field if it is not already present."""
    values: list[str] = []
    for raw in (existing or "").split(","):
        value = raw.strip()
        if value and value not in values:
            values.append(value)
    new_clean = (new_value or "").strip()
    if new_clean and new_clean not in values:
        values.append(new_clean)
    return ",".join(values)


def _repoint_duplicate_isu_official_refs(conn, *, keeper_id: int, duplicate_ids: list[int]) -> None:
    """Move FK references from duplicate ISU official rows to the canonical row."""
    has_segment_isu_official_id = bool(
        conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'segment_official'
                      AND column_name = 'isu_official_id'
                )
                """
            )
        ).scalar()
    )
    for duplicate_id in duplicate_ids:
        conn.execute(
            text(
                """
                UPDATE public.judge_isu_official_link
                SET isu_official_id = :keeper_id
                WHERE isu_official_id = :duplicate_id
                """
            ),
            {"keeper_id": keeper_id, "duplicate_id": duplicate_id},
        )
        conn.execute(
            text(
                """
                UPDATE public.isu_official_name_alias
                SET isu_official_id = :keeper_id
                WHERE isu_official_id = :duplicate_id
                """
            ),
            {"keeper_id": keeper_id, "duplicate_id": duplicate_id},
        )
        if has_segment_isu_official_id:
            conn.execute(
                text(
                    """
                    UPDATE public.segment_official
                    SET isu_official_id = :keeper_id
                    WHERE isu_official_id = :duplicate_id
                    """
                ),
                {"keeper_id": keeper_id, "duplicate_id": duplicate_id},
            )
        conn.execute(
            text("DELETE FROM officials_analysis.isu_official WHERE id = :duplicate_id"),
            {"duplicate_id": duplicate_id},
        )


def load_rows(
    rows: list[IsuOfficialRow], *, dry_run: bool, engine: Engine | None = None
) -> int:
    import judge_official_link_core as core

    if dry_run:
        for row in rows:
            print(f"DRY RUN {row.federation_code} | {row.full_name} | {row.season}")
        return 0

    engine = engine or core.make_engine()
    core.ensure_table(engine)
    count = 0
    with engine.begin() as conn:
        for row in rows:
            existing = conn.execute(
                text(
                    """
                    SELECT
                        id,
                        season,
                        communication_ref,
                        disciplines,
                        appointment_types,
                        levels
                    FROM officials_analysis.isu_official
                    WHERE federation_code = :federation_code
                      AND name_normalized = :name_normalized
                    ORDER BY id
                    """
                ),
                row.__dict__,
            ).mappings().all()
            if existing:
                keeper = existing[0]
                duplicate_ids = [int(r["id"]) for r in existing[1:]]
                if duplicate_ids:
                    _repoint_duplicate_isu_official_refs(
                        conn, keeper_id=int(keeper["id"]), duplicate_ids=duplicate_ids
                    )
                conn.execute(
                    text(
                        """
                        UPDATE officials_analysis.isu_official
                        SET federation_name = :federation_name,
                            full_name = :full_name,
                            first_name = :first_name,
                            last_name = :last_name,
                            season = :season,
                            communication_ref = :communication_ref,
                            disciplines = :disciplines,
                            appointment_types = :appointment_types,
                            levels = :levels,
                            last_modified = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        **row.__dict__,
                        "id": int(keeper["id"]),
                        "season": merge_csv_values(keeper["season"], row.season),
                        "communication_ref": merge_csv_values(
                            keeper["communication_ref"], row.communication_ref
                        ),
                        "disciplines": merge_csv_values(
                            keeper["disciplines"], row.disciplines
                        ),
                        "appointment_types": merge_csv_values(
                            keeper["appointment_types"], row.appointment_types
                        ),
                        "levels": merge_csv_values(keeper["levels"], row.levels),
                    },
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO officials_analysis.isu_official (
                            federation_code,
                            federation_name,
                            full_name,
                            first_name,
                            last_name,
                            name_normalized,
                            season,
                            communication_ref,
                            disciplines,
                            appointment_types,
                            levels
                        )
                        VALUES (
                            :federation_code,
                            :federation_name,
                            :full_name,
                            :first_name,
                            :last_name,
                            :name_normalized,
                            :season,
                            :communication_ref,
                            :disciplines,
                            :appointment_types,
                            :levels
                        )
                        """
                    ),
                    row.__dict__,
                )
            count += 1
    return count


def parse_pdf_source(
    source: str,
    *,
    season: str,
    communication_ref: str = "",
    limit: int | None = None,
) -> list[IsuOfficialRow]:
    rows = parse_isu_official_text(
        pdf_text(read_pdf_bytes(source)),
        season=season,
        communication_ref=communication_ref or infer_communication_ref(source),
    )
    if limit is not None:
        return rows[:limit]
    return rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("source", help="ISU officials PDF URL or local PDF path")
    p.add_argument("--season", required=True, help="Roster season, e.g. 2526 or 2025/2026")
    p.add_argument("--communication-ref", default="", help="Communication number, e.g. 2735")
    p.add_argument("-o", "--output", default="", help="Optional CSV output path")
    p.add_argument("--load", action="store_true", help="Upsert rows into the database")
    p.add_argument("--dry-run", action="store_true", help="Print planned DB rows; do not write")
    p.add_argument("--limit", type=int, default=None, help="Limit parsed rows for testing")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = parse_pdf_source(
        args.source,
        season=args.season,
        communication_ref=args.communication_ref,
        limit=args.limit,
    )
    if args.output:
        write_csv(args.output, rows)
    if args.load or args.dry_run:
        loaded = load_rows(rows, dry_run=args.dry_run)
        action = "planned" if args.dry_run else "loaded"
        print(f"{action} {loaded if not args.dry_run else len(rows)} ISU officials")
    elif not args.output:
        write_csv("-", rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
