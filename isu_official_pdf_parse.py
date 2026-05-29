"""
Parse ISU Communication PDF text into roster rows for ``officials_analysis.isu_official``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from isu_official_names import normalize_person_name, parse_isu_list_name

_COUNTRY_RE = re.compile(r"^([A-Z]{2,3})\s*-\s*(.+)$")
_NAME_LINE_RE = re.compile(r",\s*(Mr\.|Ms\.|Mrs\.)\s*$", re.IGNORECASE)
_ROLE_HINTS = (
    "REFEREE",
    "JUDGE",
    "TECHNICAL CONTROLLER",
    "TECHNICAL SPECIALIST",
    "DATA & REPLAY",
    "DATA OPERATOR",
    "REPLAY OPERATOR",
)
_DISCIPLINE_HINTS = (
    "SINGLE & PAIR",
    "ICE DANCE",
    "SYNCHRONIZED",
)


@dataclass(frozen=True)
class IsuRosterRow:
    federation_code: str
    raw_name: str
    full_name: str
    first_name: str | None
    last_name: str | None
    name_normalized: str
    discipline_context: str | None
    role_context: str | None


def _is_role_line(line: str) -> bool:
    u = line.upper()
    return any(h in u for h in _ROLE_HINTS) and not _NAME_LINE_RE.search(line)


def _is_discipline_line(line: str) -> bool:
    u = line.upper()
    return any(h in u for h in _DISCIPLINE_HINTS) and not _is_role_line(line)


def parse_isu_official_pdf_lines(lines: list[str]) -> list[IsuRosterRow]:
    """
    Parse line-oriented text extracted from an ISU officials Communication PDF.
    """
    federation: str | None = None
    discipline_ctx: str | None = None
    role_ctx: str | None = None
    seen: set[tuple[str, str]] = set()
    out: list[IsuRosterRow] = []

    for raw in lines:
        line = (raw or "").strip()
        if not line or line.startswith("Communication No."):
            continue
        if line.startswith("List of ") or line.startswith("Following nominations"):
            continue
        if line.startswith("ISU Members must"):
            continue

        m_country = _COUNTRY_RE.match(line)
        if m_country and not _NAME_LINE_RE.search(line):
            federation = m_country.group(1).strip().upper()
            discipline_ctx = None
            role_ctx = None
            continue

        if federation is None:
            continue

        if _NAME_LINE_RE.search(line):
            full_name, first, last = parse_isu_list_name(line)
            norm = normalize_person_name(full_name)
            if not norm:
                continue
            key = (federation, norm)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                IsuRosterRow(
                    federation_code=federation,
                    raw_name=line,
                    full_name=full_name,
                    first_name=first,
                    last_name=last,
                    name_normalized=norm,
                    discipline_context=discipline_ctx,
                    role_context=role_ctx,
                )
            )
            continue

        if _is_discipline_line(line):
            discipline_ctx = line
            role_ctx = None
            continue

        if _is_role_line(line):
            role_ctx = line
            continue

    return out


def extract_lines_from_pdf(pdf_path: str) -> list[str]:
    import pdfplumber

    lines: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
    return lines
