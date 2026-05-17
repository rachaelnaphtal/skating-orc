"""
Load and lightly normalize USFS **qualifying-season availability** workbooks
(typically a Google Form export with sheet ``original``).

These spreadsheets are **wide**: one row per official, columns for member number,
name, self-reported **form role** checkboxes (Judge–SP, Referee–Dance, etc.),
one column per **competition** availability (e.g. “2026 Eastern Sectional Singles | … | …?”),
plus **conflicts**, ethics / COI, chief interest, international, uploads, etc.

**Eligibility (who may be assigned)** should come from the **directory**, not from the
form role columns: join ``member_number`` → ``officials_analysis.officials.mbr_number`` and
use ``officials_analysis.appointments`` (``appointment_type_id``, ``discipline_id``,
``level_id``, ``active``) according to rules you configure per competition. Form role
columns are optional (QA, display, or cross-check vs appointments).

**Retaining sheet context:** use :func:`build_respondent_supplemental_snapshot` to keep
all non–competition-prompt columns (conflicts, disclosures, chief/international questions,
self-reported roles, etc.) for optional columns in assigning reports or DB JSON / side tables.

**Response completion:** Google Form exports often include *Completion status* or a column
named **Status**. By default :func:`load_original_sheet` keeps only rows marked **Complete** so partial /
abandoned form sessions are not loaded. Use ``only_complete_responses=False`` or
``allow_missing_completion_status=True`` when the export has no such column.

This module only handles **ingest and column discovery**; per-event eligibility rules,
appointments SQL, and UI live in your reporting layer.
"""

from __future__ import annotations

import re

import pandas as pd

DEFAULT_SHEET = "original"

# Availability columns look like event titles with a year and a pipe separator.
_COMPETITION_COL_HINT = re.compile(r"20\d{2}.+\|.+\|")


def find_completion_status_column(df: pd.DataFrame) -> str | None:
    """
    Detect the “completion status” column (Google Forms: *Completion status*).

    Returns the first string column whose header matches common patterns.
    """
    for c in df.columns:
        if not isinstance(c, str):
            continue
        cl = c.strip().lower()
        if "completion" in cl and "status" in cl:
            return str(c)
        if cl in ("form status", "response status", "survey status"):
            return str(c)
        if cl == "status":
            return str(c)
    return None


def is_complete_response_status(value: object) -> bool:
    """True when the cell indicates a fully submitted form (not partial / incomplete)."""
    if pd.isna(value):
        return False
    s = str(value).strip().lower()
    if not s:
        return False
    if s == "complete":
        return True
    # “Complete (…)” or similar
    if s.startswith("complete") and not s.startswith("incomplete"):
        return True
    return False


def filter_complete_responses(
    df: pd.DataFrame,
    *,
    status_column: str | None = None,
    allow_missing_status_column: bool = False,
) -> pd.DataFrame:
    """
    Keep only rows whose completion status is :func:`is_complete_response_status`.

    If no status column is found: raises ``ValueError`` unless
    ``allow_missing_status_column`` is True (then returns ``df`` unchanged).
    """
    col = status_column or find_completion_status_column(df)
    if not col:
        if allow_missing_status_column:
            return df
        raise ValueError(
            "No completion status column found (expected headers like 'Status' or 'Completion status'). "
            "Pass allow_missing_status_column=True to load all rows, or status_column='...'."
        )
    if col not in df.columns:
        raise ValueError(f"Completion status column not in dataframe: {col!r}")
    mask = df[col].map(is_complete_response_status)
    return df.loc[mask].copy()


def load_original_sheet(
    path: str,
    *,
    sheet_name: str = DEFAULT_SHEET,
    only_complete_responses: bool = True,
    completion_status_column: str | None = None,
    allow_missing_completion_status: bool = False,
) -> pd.DataFrame:
    """
    Read the main sheet with headers in row 0 (Excel row 1).

    When ``only_complete_responses`` is True (default), drops rows that are not
    *Complete* per :func:`filter_complete_responses`.
    """
    df = pd.read_excel(path, sheet_name=sheet_name, header=0)
    if only_complete_responses:
        df = filter_complete_responses(
            df,
            status_column=completion_status_column,
            allow_missing_status_column=allow_missing_completion_status,
        )
    return df


def find_member_number_column(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if not isinstance(c, str):
            continue
        if "Member Number" in c or "mbr" in c.lower():
            return str(c)
    return None


def find_email_column(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if not isinstance(c, str):
            continue
        if "email" in c.lower():
            return str(c)
    return None


def competition_availability_columns(df: pd.DataFrame) -> list[str]:
    """Columns that appear to be per-competition availability prompts."""
    out: list[str] = []
    for c in df.columns:
        if not isinstance(c, str):
            continue
        if _COMPETITION_COL_HINT.search(c):
            out.append(c)
    return out


def form_self_reported_role_columns(df: pd.DataFrame) -> list[str]:
    """
    Columns where the respondent **checked** capacity on the form (e.g.
    “Judge - Singles and Pairs”, “Referee - Dance”).

    **Not** the source of truth for eligibility — use ``appointments`` for that.
    Useful for optional display or sanity checks against the directory.
    """
    skip_prefixes = (
        "status",
        "timestamp",
        "member",
        "first name",
        "last name",
        "email",
        "city",
        "state",
        "section",
        "region",
        "airport",
        "additional information",
        "comments",
        "upload",
        "conflict",
        "please select",
        "are you",
        "do you",
        "during ",
        "all officials",
        "if you",
        "prior to",
        "list of",
        "i am available",
        "date",
        "start time",
        "finish time",
    )
    out: list[str] = []
    for c in df.columns:
        if not isinstance(c, str) or not c.strip():
            continue
        cl = c.strip().lower()
        if _COMPETITION_COL_HINT.search(c):
            continue
        if any(cl.startswith(p) for p in skip_prefixes):
            continue
        if "|" in c and "20" in c:
            continue
        # Form role / appointment multi-select style
        if c.startswith("Judge ") or c.startswith("Referee ") or c.startswith("Technical "):
            out.append(c)
            continue
        if "Data Entry" in c or c in ("Scoring Official", "SST", "Music Coordinator", "Music Technician", "Announcer"):
            out.append(c)
    return out


def role_appointment_columns(df: pd.DataFrame) -> list[str]:
    """Backward-compatible alias for :func:`form_self_reported_role_columns`."""
    return form_self_reported_role_columns(df)


def respondent_supplemental_columns(df: pd.DataFrame) -> list[str]:
    """
    Every column **except** per-competition availability prompts — identity, form roles,
    conflicts, ethics, chief / international, uploads, comments, etc.

    Use this to know which headers to persist alongside availability when loading into a DB.
    """
    comp = set(competition_availability_columns(df))
    return [c for c in df.columns if isinstance(c, str) and c not in comp]


def conflicts_ethics_related_columns(df: pd.DataFrame) -> list[str]:
    """
    Subset of supplemental columns likely tied to **conflicts / COI / ethics / coaching /
    competing** (substring heuristic). Tune tokens if a new form wording ships.
    """
    hints = (
        "conflict",
        "relative",
        "coach",
        "teach",
        "consult",
        "financial",
        "commercial",
        "venture",
        "compete",
        "disclos",
        "indirect",
        "ethic",
        "list of",
        "upload",
    )
    out: list[str] = []
    for c in respondent_supplemental_columns(df):
        cl = c.lower()
        if any(h in cl for h in hints):
            out.append(c)
    return out


def build_respondent_supplemental_snapshot(
    df: pd.DataFrame,
    *,
    member_col: str | None = None,
    drop_competition_prompts: bool = True,
) -> pd.DataFrame:
    """
    One row per respondent: normalized ``member_number`` plus **all** other columns you
    may want to store or show (conflicts, ethics, chief interest, form self-reported
    roles, comments, uploads, …).

    By default **drops** per-competition availability columns (use
    :func:`melt_competition_availability` for those). Set ``drop_competition_prompts``
    to False to keep the full wide row including every competition column.
    """
    mcol = member_col or find_member_number_column(df)
    if not mcol:
        raise ValueError("Could not resolve member number column.")
    out = df.copy()
    out.insert(0, "member_number", normalize_member_number(out[mcol]))
    out = out.drop(columns=[mcol])
    if drop_competition_prompts:
        drop_c = [c for c in competition_availability_columns(df) if c in out.columns]
        if drop_c:
            out = out.drop(columns=drop_c)
    return out


def normalize_member_number(series: pd.Series) -> pd.Series:
    """Strip whitespace; coerce to string; drop obvious .0 from Excel numeric IDs."""
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.replace({"nan": "", "None": ""})
    return s


def normalize_member_number_value(value: object) -> str:
    """Single-cell version of :func:`normalize_member_number` (for DB lookup keys)."""
    return normalize_member_number(pd.Series([value])).iloc[0]


def normalize_qualifying_availability_cell(value: object) -> str:
    """
    Map a raw spreadsheet cell to a stable code for ingest logic.

    Returns one of: ``available``, ``not_available``, ``no_response``,
    ``does_not_apply`` (prompt N/A for this person), ``unknown``.

    Loaders that only persist explicit **yes** treat every code except ``available``
    the same (no row, or delete existing row).
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "no_response"
    s = str(value).strip()
    if not s:
        return "no_response"
    sl = s.lower().rstrip(".")
    if (
        "does not apply" in sl
        or "doesn't apply" in sl
        or "doesnt apply" in sl
        or "not applicable" in sl
        or sl in ("n/a",)
    ):
        return "does_not_apply"
    if sl in ("yes", "y", "true", "1", "available"):
        return "available"
    if sl in ("no", "n", "false", "0", "unavailable"):
        return "not_available"
    if "i am not available" in sl:
        return "not_available"
    if "i am available" in sl:
        return "available"
    if "not available" in sl or "unavailable" in sl:
        return "not_available"
    if "available" in sl and "not available" not in sl:
        return "available"
    return "unknown"


def melt_competition_availability(
    df: pd.DataFrame,
    *,
    competition_cols: list[str] | None = None,
    member_col: str | None = None,
) -> pd.DataFrame:
    """
    Long format: one row per (member_number, competition_question, raw_cell_value).

    Map values to available / unavailable / missing in your reporting layer
    (forms may use “I am available.”, Yes/No, checkboxes exported as strings, etc.).
    """
    mcol = member_col or find_member_number_column(df)
    if not mcol:
        raise ValueError("Could not resolve member number column.")

    ccols = competition_cols if competition_cols is not None else competition_availability_columns(df)
    if not ccols:
        raise ValueError("No competition availability columns found.")

    slim = df[[mcol] + ccols].copy()
    slim[mcol] = normalize_member_number(slim[mcol])
    long = slim.melt(id_vars=[mcol], var_name="competition_prompt", value_name="raw_availability")
    long.rename(columns={mcol: "member_number"}, inplace=True)
    return long

