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

**Storage:** :func:`activityAnalysis.qualifying_form_store.load_qualifying_form_workbook`
persists full rows in ``qualifying_official_form_response.response_json``, competitions,
and normalized availability. Reporting uses directory ``appointments`` plus per-competition
criteria in ``qualifying_competition_criteria``.

This module handles **ingest and column discovery** only.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

import pandas as pd

DEFAULT_SHEET = "original"

# Availability columns look like event titles with a year and a pipe separator.
_COMPETITION_COL_HINT = re.compile(r"20\d{2}.+\|.+\|")


def resolve_workbook_sheet_name(path: str, sheet_name: str | None = None) -> str:
    """
    Pick the data sheet for a qualifying export.

    Uses ``sheet_name`` when present; else ``original`` (Google Form default);
    else the first workbook sheet (e.g. ``Sheet 1``).
    """
    xl = pd.ExcelFile(path)
    if sheet_name and sheet_name in xl.sheet_names:
        return sheet_name
    if DEFAULT_SHEET in xl.sheet_names:
        return DEFAULT_SHEET
    if not xl.sheet_names:
        raise ValueError(f"No worksheets in {path!r}")
    return xl.sheet_names[0]


def parse_qualifying_competition_prompt(competition_key: str) -> dict[str, str | None]:
    """Split a form column header into title, location, dates, and a 4-digit year."""
    text = str(competition_key).strip().rstrip("?")
    parts = [p.strip() for p in text.split("|")]
    title = parts[0] if parts else text
    location = parts[1] if len(parts) > 1 else None
    dates = parts[2] if len(parts) > 2 else None
    ym = re.search(r"(20\d{2})", text)
    year = ym.group(1) if ym else None
    return {
        "title": title,
        "location": location,
        "dates": dates,
        "year": year,
        "competition_key": text,
    }


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
    if s.startswith("incomplete"):
        return False
    if s == "complete":
        return True
    # “Complete (…)” or similar
    if s.startswith("complete"):
        return True
    return False


def find_completion_status_key(column_names: Iterable[str]) -> str | None:
    """Like :func:`find_completion_status_column` but for an iterable of header names."""
    return find_completion_status_column(pd.DataFrame(columns=list(column_names)))


def _header_is_opt_out_all_qualifying(header: str) -> bool:
    """Column G–style prompt: not available / not interested for all qualifying events."""
    cl = header.strip().lower()
    if not cl:
        return False
    if "check the box" in cl and "not" in cl:
        return True
    if "not interested" in cl and ("qualifying" in cl or "officiating" in cl):
        return True
    if "not available" in cl and (
        "anything" in cl
        or "any of" in cl
        or "all of" in cl
        or ("all" in cl and "qualifying" in cl)
    ):
        return True
    return False


def find_not_interested_all_qualifying_column(df: pd.DataFrame) -> str | None:
    """
    Checkbox in column G (typical): “NOT interested in officiating ALL … qualifying …”.
    """
    str_cols = [c for c in df.columns if isinstance(c, str)]
    for c in str_cols:
        if _header_is_opt_out_all_qualifying(c):
            return str(c)
    # Fallback: Excel column G (0-based index 6) when the header matches opt-out wording.
    if len(str_cols) > 6 and _header_is_opt_out_all_qualifying(str_cols[6]):
        return str_cols[6]
    return None


def find_not_interested_all_qualifying_key(column_names: Iterable[str]) -> str | None:
    return find_not_interested_all_qualifying_column(
        pd.DataFrame(columns=list(column_names))
    )


def is_affirmative_checkbox(value: object) -> bool:
    """True when a yes/no or checkbox cell is checked / answered Yes."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return value != 0
    if isinstance(value, (list, tuple, set)):
        return len(value) > 0
    s = str(value).strip().lower()
    if not s:
        return False
    if s in ("1.0", "1.00"):
        return True
    return s in ("yes", "y", "true", "1", "checked", "x")


def is_opt_out_all_qualifying_value(value: object) -> bool:
    """
    True when the global opt-out checkbox (column G) is checked.

    Google Forms may export TRUE, Yes, or the full question text—not only “yes”.
    """
    if is_affirmative_checkbox(value):
        return True
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    s = str(value).strip().lower()
    if not s or s in ("no", "n", "false", "0", "unchecked"):
        return False
    if _header_is_opt_out_all_qualifying(s):
        return True
    if "not interested" in s and ("qualifying" in s or "all" in s or "officiating" in s):
        return True
    if "not available" in s and (
        "anything" in s or "any of" in s or "all" in s or "qualifying" in s
    ):
        return True
    if "opt out" in s or "opt-out" in s or "optout" in s:
        if "qualifying" in s or "officiating" in s or "all" in s:
            return True
    if "would like to opt out" in s:
        return True
    return False


def response_json_is_complete(response_json: dict[str, Any]) -> bool:
    """False when the stored row has an explicit incomplete *Status* / completion column."""
    key = find_completion_status_key(response_json.keys())
    if not key:
        return True
    return is_complete_response_status(response_json.get(key))


def _header_is_qualifying_notes_column(header: str) -> bool:
    """Supplemental note fields on the qualifying availability form."""
    cl = header.strip().lower()
    if "additional information" in cl and "committee" in cl:
        if "not officiate" in cl or "qualifying season" in cl:
            return True
    if "anything else we should know" in cl and "availability" in cl:
        return True
    if "regrading" in cl and "committee" in cl and "additional" in cl:
        return True
    return False


# Short headers for “rank your roles” grid (not “Judge - Singles and Pairs” checkboxes).
_FORM_ROLE_PRIORITY_HEADERS = frozenset(
    {
        "Judge",
        "Referee",
        "Technical Controller",
        "Technical Specialist",
        "Scoring Official",
        "Scoring Official.1",  # pandas duplicate when the sheet has two “Scoring Official” cols
        "Scoring System Technician",
        "Music Coordinator",
        "Music Technician",
        "Announcer",
    }
)

# Form label → ``appointment_types.name`` in the directory.
_FORM_PRIORITY_TO_APPOINTMENT_NAME: dict[str, str] = {
    "Judge": "Competition Judge",
    "Referee": "Referee",
    "Technical Controller": "Technical Controller",
    "Technical Specialist": "Technical Specialist",
    "Scoring Official": "Scoring Official",
    "Scoring Official.1": "Scoring Official",
    "Scoring System Technician": "Scoring System Technician",
    "Music Coordinator": "Music Coordinator",
    "Music Technician": "Music Technician",
    "Announcer": "Announcer",
}

# Short labels for the report column (comma-separated, priority order).
_FORM_PRIORITY_DISPLAY_LABEL: dict[str, str] = {
    "Judge": "judge",
    "Referee": "referee",
    "Technical Controller": "technical controller",
    "Technical Specialist": "technical specialist",
    "Scoring Official": "scoring official",
    "Scoring Official.1": "scoring official",
    "Scoring System Technician": "scoring system technician",
    "Music Coordinator": "music coordinator",
    "Music Technician": "music technician",
    "Announcer": "announcer",
}


def _header_is_role_priority_column(header: str) -> bool:
    return header.strip() in _FORM_ROLE_PRIORITY_HEADERS


def _parse_role_priority_rank(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        rank = int(float(value))
    except (TypeError, ValueError):
        return None
    if 1 <= rank <= 15:
        return rank
    return None


def extract_qualifying_role_priority(
    response_json: dict[str, Any],
    *,
    held_appointment_type_ids: set[int],
    appointment_name_to_id: dict[str, int],
) -> str:
    """
    Comma-separated role preference order (e.g. ``judge, referee``) from the form rank
    grid. Only includes roles the official holds as an **active** directory appointment.
    """
    ranked: list[tuple[int, str]] = []
    seen_labels: set[str] = set()
    for key, val in response_json.items():
        if not isinstance(key, str) or not _header_is_role_priority_column(key):
            continue
        rank = _parse_role_priority_rank(val)
        if rank is None:
            continue
        appt_name = _FORM_PRIORITY_TO_APPOINTMENT_NAME.get(key.strip())
        if not appt_name:
            continue
        at_id = appointment_name_to_id.get(appt_name.strip().casefold())
        if at_id is None or int(at_id) not in held_appointment_type_ids:
            continue
        label = _FORM_PRIORITY_DISPLAY_LABEL.get(key.strip(), appt_name.casefold())
        if label in seen_labels:
            continue
        seen_labels.add(label)
        ranked.append((rank, label))
    ranked.sort(key=lambda x: (x[0], x[1]))
    return ", ".join(label for _, label in ranked)


def extract_qualifying_form_notes(response_json: dict[str, Any]) -> str:
    """Concatenate committee / availability note columns from a stored form row."""
    parts: list[str] = []
    for key in sorted(response_json.keys()):
        if not isinstance(key, str) or key.startswith("_"):
            continue
        if not _header_is_qualifying_notes_column(key):
            continue
        val = response_json.get(key)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        text = str(val).strip()
        if text:
            parts.append(text)
    return "\n\n".join(parts)


def row_opts_out_all_qualifying(row: pd.Series) -> bool:
    """True if any column in the row is the global opt-out prompt with a Yes/checked value."""
    for col in row.index:
        if isinstance(col, str) and _header_is_opt_out_all_qualifying(col):
            if is_opt_out_all_qualifying_value(row[col]):
                return True
    # Column G (index 6): checked cell even when the header string is truncated oddly.
    str_cols = [c for c in row.index if isinstance(c, str)]
    if len(str_cols) > 6:
        col_g = str_cols[6]
        if not _COMPETITION_COL_HINT.search(col_g):
            if is_opt_out_all_qualifying_value(row.get(col_g)):
                return True
    return False


def response_json_not_interested_all(response_json: dict[str, Any]) -> bool:
    """True when they checked NOT interested / not available for all qualifying."""
    if response_json.get("_not_interested_all_qualifying") is True:
        return True
    for key, val in response_json.items():
        if not isinstance(key, str) or key.startswith("_"):
            continue
        if _header_is_opt_out_all_qualifying(key):
            if is_opt_out_all_qualifying_value(val):
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
    resolved = resolve_workbook_sheet_name(path, sheet_name)
    df = pd.read_excel(path, sheet_name=resolved, header=0)
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


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    """Google Form exports usually include a *Timestamp* column."""
    exact = {
        "timestamp",
        "submitted",
        "submission time",
        "response timestamp",
        "date submitted",
    }
    for c in df.columns:
        if not isinstance(c, str):
            continue
        if c.strip().lower() in exact:
            return str(c)
    for c in df.columns:
        if not isinstance(c, str):
            continue
        if "timestamp" in c.lower():
            return str(c)
    return None


def dedupe_form_responses_by_member(
    df: pd.DataFrame,
    *,
    member_column: str,
    timestamp_column: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    When the same member number appears more than once, keep the **most recent** row.

    Uses ``timestamp_column`` (or autodetected Timestamp) when present; otherwise
    keeps the last row in sheet order (later rows win).
    """
    mcol = member_column
    ts_col = timestamp_column or find_timestamp_column(df)
    keys = df[mcol].map(normalize_member_number_value)
    empty = keys.isna() | (keys == "")

    dup_keys = keys[~empty][keys[~empty].duplicated(keep=False)]
    if dup_keys.empty:
        return df, {
            "duplicate_rows_dropped": 0,
            "duplicate_member_numbers": [],
            "dedupe_by": None,
        }

    empty_df = df.loc[empty]
    valid = df.loc[~empty].copy()
    valid["_mbr_key"] = keys[~empty]

    dedupe_by = "sheet_order"
    if ts_col and ts_col in valid.columns:
        valid["_sort_ts"] = pd.to_datetime(valid[ts_col], errors="coerce")
        valid = valid.sort_values(["_mbr_key", "_sort_ts"], kind="mergesort")
        dedupe_by = "timestamp"
    else:
        valid = valid.reset_index().rename(columns={"index": "_orig_idx"})
        valid = valid.sort_values(["_mbr_key", "_orig_idx"], kind="mergesort")

    before = len(valid)
    deduped_valid = valid.drop_duplicates(subset=["_mbr_key"], keep="last")
    dropped = before - len(deduped_valid)

    drop_cols = [c for c in ("_mbr_key", "_sort_ts", "_orig_idx") if c in deduped_valid.columns]
    deduped_valid = deduped_valid.drop(columns=drop_cols)

    parts = [deduped_valid]
    if not empty_df.empty:
        parts.append(empty_df)
    out = pd.concat(parts, ignore_index=True)

    dup_members = sorted(
        {str(k) for k in dup_keys.unique() if k and not pd.isna(k)}
    )
    return out, {
        "duplicate_rows_dropped": dropped,
        "duplicate_member_numbers": dup_members,
        "dedupe_by": dedupe_by,
    }


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

