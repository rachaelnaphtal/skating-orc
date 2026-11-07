"""
Load a competition from results.usfigureskating.org.

The schedule and officials pages are public. Judges-scores PDFs are not.
Fetch the final segments and their panels, then match uploaded PDFs by the
filename published on each segment.
"""

from __future__ import annotations

import json
import re
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import requests

from database_loader import normalize_scraped_judge_name
from ijs_results_urls import usfs_results_event_url
from officials_competition_types import (
    OFFICIALS_COMPETITION_TYPE_ID_ADULT_CHAMPIONSHIPS,
    OFFICIALS_COMPETITION_TYPE_ID_ADULT_SECTIONAL,
    OFFICIALS_COMPETITION_TYPE_ID_COLLEGIATE_CHAMPIONSHIPS,
)

_NUXT_WRAPPERS = frozenset(
    {
        "ShallowReactive",
        "Reactive",
        "Ref",
        "ShallowRef",
        "EmptyRef",
        "EmptyShallowRef",
    }
)
_FINAL_STATUSES = frozenset({"final"})
_HTTP_TIMEOUT = 45
_USER_AGENT = "JudgingAnalysis/usfs-results"


@dataclass
class UsfsSegmentSlot:
    category_name: str
    category_id: str
    segment_name: str
    segment_code: str
    level: str
    discipline: str
    date: str
    time: str
    pdf_filename: str
    judges: list[str]
    officials: list[dict[str, str]]
    officials_error: str = ""

    @property
    def label(self) -> str:
        when = self.date or ""
        return f"{self.category_name} — {self.segment_name}" + (
            f" ({when})" if when else ""
        )


@dataclass
class UsfsEventChecklist:
    event_url: str
    name: str
    location: str
    start_date: str
    end_date: str
    season_code: str
    suggested_type_id: int | None
    segments: list[UsfsSegmentSlot] = field(default_factory=list)


@dataclass
class SegmentImportOutcome:
    label: str
    status: str
    detail: str


def season_code_from_iso_date(iso_date: str) -> str:
    """USFS season code (``2627``) from an ISO start date. Season turns over in July."""
    year_s, month_s, _day = (iso_date or "").split("-")
    year = int(year_s)
    month = int(month_s)
    start = year if month >= 7 else year - 1
    return f"{start % 100:02d}{(start + 1) % 100:02d}"


def suggest_officials_competition_type_id(name: str, slug: str) -> int | None:
    """Best officials-analysis type id from the event name and URL slug."""
    text = f"{slug} {name}".lower().replace("_", " ").replace("-", " ")
    synchro = "synchro" in text
    if "sectional" in text:
        if "adult" in text:
            return OFFICIALS_COMPETITION_TYPE_ID_ADULT_SECTIONAL
        if "eastern" in text:
            return 5 if synchro else 1
        if "midwestern" in text:
            return 6 if synchro else 2
        if "pacific" in text:
            return 7 if synchro else 3
        return None
    if "collegiate" in text:
        return OFFICIALS_COMPETITION_TYPE_ID_COLLEGIATE_CHAMPIONSHIPS
    if "adult" in text and "championship" in text:
        return OFFICIALS_COMPETITION_TYPE_ID_ADULT_CHAMPIONSHIPS
    if synchro and "championship" in text:
        return 8
    if "championship" in text:
        return 4
    return None


def expected_segment_label(category_name: str, segment_name: str) -> str:
    """
    Label comparable to the protocol PDF header.

    Categories sometimes end in ``FS`` or ``SP`` while the segment name already
    says Free Skating or Short Program.
    """
    category = " ".join((category_name or "").split())
    segment = " ".join((segment_name or "").split())
    category_key = category.lower()
    segment_key = segment.lower()
    if segment_key.startswith("free") and category_key.endswith(" fs"):
        category = category[:-3].strip()
    elif segment_key.startswith("short") and category_key.endswith(" sp"):
        category = category[:-3].strip()
    return f"{category} {segment}".strip()


def resolve_nuxt_payload(payload: list) -> Any:
    """Resolve a Nuxt ``__NUXT_DATA__`` array (integer indexes are references)."""

    def resolve(idx: int, seen: set[int]) -> Any:
        if not isinstance(idx, int) or idx < 0 or idx >= len(payload):
            return idx
        if idx in seen:
            return None
        node = payload[idx]
        if isinstance(node, (str, int, float, bool)) or node is None:
            return node
        seen.add(idx)
        if isinstance(node, dict):
            return {
                key: resolve(value, seen) if isinstance(value, int) else value
                for key, value in node.items()
            }
        if isinstance(node, list):
            if (
                len(node) == 2
                and isinstance(node[0], str)
                and node[0] in _NUXT_WRAPPERS
                and isinstance(node[1], int)
            ):
                return resolve(node[1], seen)
            return [
                resolve(item, seen) if isinstance(item, int) else item for item in node
            ]
        return node

    return resolve(0, set())


def nuxt_data_map(html: str) -> dict[str, Any]:
    match = re.search(
        r'id="__NUXT_DATA__"[^>]*>(.*?)</script>',
        html or "",
        re.DOTALL,
    )
    if not match:
        raise ValueError("Page has no results payload.")
    payload = json.loads(match.group(1))
    root = resolve_nuxt_payload(payload)
    data = root.get("data") if isinstance(root, dict) else None
    if not isinstance(data, dict):
        raise ValueError("Results payload has no data map.")
    return data


def _event_record(data_map: dict[str, Any]) -> dict[str, Any]:
    for key, value in data_map.items():
        if (
            isinstance(key, str)
            and key.startswith("event:")
            and isinstance(value, dict)
            and isinstance(value.get("event"), dict)
            and isinstance(value.get("categories"), list)
        ):
            return value
    raise ValueError("Schedule page has no event.")


def _iter_protocol_segments(node: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            officials = value.get("officials")
            if value.get("code") and isinstance(officials, list):
                found.append(value)
            for child_key, child in value.items():
                if child_key == "performances":
                    continue
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(node)
    return found


def _pdf_filename(category_id: str, segment_code: str, protocol: Any) -> str:
    href = ""
    if isinstance(protocol, dict):
        href = str(protocol.get("href") or "").strip()
    if href:
        return Path(href.split("?", 1)[0]).name
    code = (segment_code or "").strip().lower()
    return f"{category_id}-{code}-judges-scores.pdf"


def _judge_number(role: str) -> int:
    match = re.search(r"(\d+)", role or "")
    return int(match.group(1)) if match else 999


def _is_panel_judge(role: str) -> bool:
    return (role or "").strip().lower().startswith("judge")


def _disambiguate_officials(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: dict[str, int] = {}
    out: list[dict[str, str]] = []
    for row in rows:
        base = (row.get("role") or "").strip()
        name = normalize_scraped_judge_name(row.get("name"))
        if not base or not name:
            continue
        count = seen.get(base, 0) + 1
        seen[base] = count
        role = f"{base} ({count})" if count > 1 else base
        out.append({"role": role, "name": name})
    return out


def _officials_from_segment(segment: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in segment.get("officials") or []:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "role": str(row.get("role") or "").strip(),
                "name": str(row.get("name") or "").strip(),
            }
        )
    return _disambiguate_officials(rows)


def _panel_judges(officials: list[dict[str, str]]) -> list[str]:
    judges = [row for row in officials if _is_panel_judge(row["role"])]
    judges.sort(key=lambda row: _judge_number(row["role"]))
    return [row["name"] for row in judges]


def checklist_from_records(
    event_url: str,
    event_record: dict[str, Any],
    officials_by_category: dict[str, list[dict[str, Any]]] | None = None,
    officials_errors: dict[str, str] | None = None,
) -> UsfsEventChecklist:
    event = event_record.get("event") or {}
    name = str(event.get("name") or "").strip()
    slug = str(event.get("slug") or "")
    city = str(event.get("city") or "").strip()
    region = str(event.get("region") or "").strip()
    location = ", ".join(part for part in (city, region) if part)
    start_date = str(event.get("startDate") or "")[:10]
    end_date = str(event.get("endDate") or "")[:10]
    season = season_code_from_iso_date(start_date) if start_date else ""
    officials_by_category = officials_by_category or {}
    officials_errors = officials_errors or {}
    segments: list[UsfsSegmentSlot] = []
    for category in event_record.get("categories") or []:
        if not isinstance(category, dict):
            continue
        category_id = str(category.get("id") or "").strip()
        category_name = str(category.get("name") or "").strip()
        details = {
            str(item.get("code") or "").upper(): item
            for item in officials_by_category.get(category_id, [])
            if isinstance(item, dict)
        }
        for segment in category.get("segments") or []:
            if not isinstance(segment, dict):
                continue
            if str(segment.get("status") or "").lower() not in _FINAL_STATUSES:
                continue
            code = str(segment.get("code") or "").upper()
            detail = details.get(code) or {}
            officials = _officials_from_segment(detail) if detail else []
            segments.append(
                UsfsSegmentSlot(
                    category_name=category_name,
                    category_id=category_id,
                    segment_name=str(segment.get("name") or "").strip(),
                    segment_code=code,
                    level=str(category.get("level") or "").strip(),
                    discipline=str(category.get("discipline") or "").strip(),
                    date=str(segment.get("date") or "")[:10],
                    time=str(segment.get("time") or "").strip(),
                    pdf_filename=_pdf_filename(
                        category_id, code, detail.get("protocol")
                    ),
                    judges=_panel_judges(officials),
                    officials=officials,
                    officials_error=officials_errors.get(category_id, ""),
                )
            )
    segments.sort(key=lambda slot: (slot.date, slot.time, slot.category_name, slot.segment_code))
    return UsfsEventChecklist(
        event_url=event_url,
        name=name,
        location=location,
        start_date=start_date,
        end_date=end_date,
        season_code=season,
        suggested_type_id=suggest_officials_competition_type_id(name, slug),
        segments=segments,
    )


def _fetch_text(url: str, session: requests.Session) -> str:
    response = session.get(
        url,
        timeout=_HTTP_TIMEOUT,
        headers={"User-Agent": _USER_AGENT},
    )
    response.raise_for_status()
    return response.text


def fetch_usfs_event_checklist(
    url: str,
    session: requests.Session | None = None,
) -> UsfsEventChecklist:
    """Read final segments and panels for one results.usfigureskating.org event."""
    event_url = usfs_results_event_url(url)
    if not event_url:
        raise ValueError(
            "Enter a results.usfigureskating.org event, schedule, or category URL."
        )
    own_session = session is None
    http = session or requests.Session()
    try:
        schedule_html = _fetch_text(f"{event_url}/schedule", http)
        event_record = _event_record(nuxt_data_map(schedule_html))
        category_ids: list[str] = []
        for category in event_record.get("categories") or []:
            if not isinstance(category, dict):
                continue
            category_id = str(category.get("id") or "").strip()
            if not category_id:
                continue
            if any(
                str(segment.get("status") or "").lower() in _FINAL_STATUSES
                for segment in (category.get("segments") or [])
                if isinstance(segment, dict)
            ):
                category_ids.append(category_id)
        officials_by_category: dict[str, list[dict[str, Any]]] = {}
        officials_errors: dict[str, str] = {}
        for category_id in category_ids:
            page = f"{event_url}/{category_id}/officials"
            try:
                html = _fetch_text(page, http)
                data_map = nuxt_data_map(html)
                details: list[dict[str, Any]] = []
                for value in data_map.values():
                    details.extend(_iter_protocol_segments(value))
                officials_by_category[category_id] = details
            except Exception as exc:
                officials_errors[category_id] = str(exc)
        return checklist_from_records(
            event_url,
            event_record,
            officials_by_category,
            officials_errors,
        )
    finally:
        if own_session:
            http.close()


def match_uploaded_pdfs(
    segments: list[UsfsSegmentSlot],
    filenames: list[str],
) -> tuple[dict[str, UsfsSegmentSlot], list[str]]:
    """
    Map uploaded basenames to final segments.

    Returns ``(matches by uploaded filename, unrecognized filenames)``.
    The first file wins when two uploads share a basename.
    """
    by_pdf: dict[str, UsfsSegmentSlot] = {}
    for segment in segments:
        by_pdf.setdefault(segment.pdf_filename.lower(), segment)
    matched: dict[str, UsfsSegmentSlot] = {}
    unmatched: list[str] = []
    seen_basenames: set[str] = set()
    for filename in filenames:
        base = Path(filename).name
        key = base.lower()
        slot = by_pdf.get(key)
        if slot is None or key in seen_basenames:
            unmatched.append(base)
            continue
        seen_basenames.add(key)
        matched[base] = slot
    return matched, unmatched


def _parsed_name_matches(event_name: str, slot: UsfsSegmentSlot) -> bool:
    from judgingParsing import segment_name_match_key

    parsed = segment_name_match_key(event_name)
    expected = segment_name_match_key(
        expected_segment_label(slot.category_name, slot.segment_name)
    )
    if not parsed or not expected:
        return False
    if parsed == expected:
        return True
    # Category names sometimes keep an FS/SP token that the PDF spells out
    # as Free Skating / Short Program, or the reverse.
    drop = {"FS", "SP"}

    def _core(key: str) -> list[str]:
        return [token for token in key.split("_") if token not in drop]

    return _core(parsed) == _core(expected)


def stored_segment_names(event_url: str) -> list[str]:
    """Segment names already stored for this results URL."""
    from database import get_db_session
    from database_loader import DatabaseLoader

    session = get_db_session()
    try:
        return DatabaseLoader(session).getSegmentNamesForCompetition(event_url)
    finally:
        session.close()


def already_imported_keys(
    segments: list[UsfsSegmentSlot],
    stored_names: list[str],
) -> set[tuple[str, str]]:
    """``(category_id, segment_code)`` for slots whose protocol is already loaded."""
    found: set[tuple[str, str]] = set()
    for slot in segments:
        if any(_parsed_name_matches(name, slot) for name in stored_names):
            found.add((slot.category_id, slot.segment_code))
    return found


def import_usfs_segments(
    checklist: UsfsEventChecklist,
    uploads: list[tuple[str, bytes]],
    *,
    competition_name: str,
    season_code: str,
    officials_type_id: int,
) -> list[SegmentImportOutcome]:
    """
    Write matched judges-scores PDFs for one event.

    Segments already stored for this event keep their scores. Officials are
    replaced from the public panel. A PDF whose header is a different segment
    is skipped.
    """
    from datetime import date

    import openpyxl

    from database import get_db_session
    from database_loader import DatabaseLoader
    from downloadResults import handleEventResults
    from judgingParsing import extract_judge_scores
    from models import Competition
    from officials_competition_types import competition_load_flags_from_officials_type_id

    name = " ".join((competition_name or "").split())
    if not name:
        raise ValueError("Competition name is required.")
    year = (season_code or "").strip()
    if not re.fullmatch(r"\d{4}", year):
        raise ValueError("Season year code must be four digits, such as 2627.")

    filenames = [filename for filename, _data in uploads]
    matched, _unmatched = match_uploaded_pdfs(checklist.segments, filenames)
    by_base: dict[str, bytes] = {}
    for filename, data in uploads:
        by_base.setdefault(Path(filename).name.lower(), data)
    outcomes: list[SegmentImportOutcome] = []
    if not matched:
        return outcomes

    qualifying, nqs, international = competition_load_flags_from_officials_type_id(
        int(officials_type_id)
    )
    start = checklist.start_date or None
    end = checklist.end_date or None
    session = get_db_session()
    try:
        loader = DatabaseLoader(session)
        competition_id = loader.insert_competition(
            name,
            checklist.event_url,
            year,
            qualifying=qualifying,
            nqs=nqs,
            international=international,
            officials_analysis_competition_type_id=int(officials_type_id),
        )
        loader.updateCompetition(
            checklist.event_url,
            checklist.location,
            start,
            end,
            name=name,
            qualifying=qualifying,
            nqs=nqs,
            international=international,
            officials_analysis_competition_type_id=int(officials_type_id),
            update_officials_competition_type=True,
        )
        already = set(loader.getSegmentNamesForCompetition(checklist.event_url))
        competition = session.get(Competition, competition_id)
        start_date = competition.start_date if competition is not None else None
        end_date = competition.end_date if competition is not None else None
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date[:10])
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date[:10])

        ordered = sorted(
            matched.items(),
            key=lambda item: (item[1].date, item[1].time, item[1].category_name, item[1].segment_code),
        )
        for filename, slot in ordered:
            label = slot.label
            if slot.officials_error:
                outcomes.append(
                    SegmentImportOutcome(label, "error", slot.officials_error)
                )
                continue
            if not slot.judges:
                outcomes.append(
                    SegmentImportOutcome(
                        label,
                        "error",
                        "Officials page has no Judge No. rows for this segment.",
                    )
                )
                continue
            pdf_bytes = by_base.get(filename.lower())
            if not pdf_bytes:
                outcomes.append(
                    SegmentImportOutcome(label, "error", f"Missing upload {filename}.")
                )
                continue
            path: str | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=".pdf", delete=False
                ) as handle:
                    handle.write(pdf_bytes)
                    path = handle.name
                (
                    event_name,
                    total_errors,
                    num_starts,
                    allowed_errors,
                    rule_errors,
                    all_element_dict,
                    all_pcs_dict,
                ) = extract_judge_scores(
                    workbook=openpyxl.Workbook(),
                    pdf_path=path,
                    base_excel_path="",
                    judges=list(slot.judges),
                    pdf_number=0,
                    event_regex=".*",
                    only_rule_errors=False,
                    use_gcp=False,
                    create_thrown_out_analysis=True,
                    judge_filter="",
                    isFSM=True,
                    write_excel=False,
                    competition_start_date=start_date,
                    competition_end_date=end_date,
                    competition_year=year,
                )
                if not event_name or total_errors is None or not num_starts:
                    outcomes.append(
                        SegmentImportOutcome(
                            label,
                            "error",
                            "The PDF did not parse as a judges-scores protocol.",
                        )
                    )
                    continue
                if not _parsed_name_matches(str(event_name), slot):
                    outcomes.append(
                        SegmentImportOutcome(
                            label,
                            "error",
                            f"PDF header is {event_name}, expected "
                            f"{expected_segment_label(slot.category_name, slot.segment_name)}.",
                        )
                    )
                    continue
                was_present = str(event_name) in already
                handleEventResults(
                    name,
                    True,
                    "",
                    None,
                    None,
                    loader,
                    competition_id,
                    list(already),
                    {},
                    {},
                    [],
                    0,
                    list(slot.judges),
                    event_name,
                    total_errors,
                    num_starts,
                    allowed_errors,
                    rule_errors,
                    all_element_dict,
                    all_pcs_dict,
                    segment_official_rows=slot.officials,
                    segment_db_key=event_name,
                )
                already.add(str(event_name))
                if was_present:
                    outcomes.append(
                        SegmentImportOutcome(
                            label,
                            "skipped",
                            "Already loaded. Scores were left in place and the panel was refreshed.",
                        )
                    )
                else:
                    outcomes.append(
                        SegmentImportOutcome(
                            label,
                            "imported",
                            f"{num_starts} skaters, {len(slot.judges)} judges.",
                        )
                    )
            except Exception as exc:
                outcomes.append(SegmentImportOutcome(label, "error", str(exc)))
            finally:
                if path:
                    Path(path).unlink(missing_ok=True)
        return outcomes
    finally:
        session.close()


def render_usfs_results_site_loader() -> None:
    """Streamlit form: schedule URL, PDF drop, import matched finals."""
    import pandas as pd
    import streamlit as st

    from analytics_connection import get_analytics_safe, release_analytics_db_connection
    from officials_competition_types import (
        OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING,
        format_officials_competition_type_select_label,
    )

    st.write(
        "Paste the event or schedule URL. Finished segments and their judge panels "
        "come from the public site. Drop the judges-scores PDFs you downloaded; "
        "each file is matched by the filename on that segment."
    )
    url = st.text_input(
        "Event URL",
        placeholder=(
            "https://results.usfigureskating.org/events/2026/"
            "36414-eastern-sectional-singles-us-pairs-final/schedule"
        ),
        key="usfs_event_url",
    )
    if st.button("Load schedule", key="usfs_load_schedule"):
        try:
            with st.spinner("Reading the schedule and officials…"):
                checklist = fetch_usfs_event_checklist(url)
        except Exception as exc:
            st.error(f"Could not read that event: {exc}")
        else:
            st.session_state["usfs_checklist"] = asdict(checklist)
            st.session_state["usfs_comp_name"] = checklist.name
            st.session_state["usfs_season"] = checklist.season_code
            if checklist.suggested_type_id is not None:
                st.session_state["usfs_oa_type"] = int(checklist.suggested_type_id)
            st.rerun()

    raw = st.session_state.get("usfs_checklist")
    if not raw:
        return
    checklist = UsfsEventChecklist(
        event_url=raw["event_url"],
        name=raw["name"],
        location=raw["location"],
        start_date=raw["start_date"],
        end_date=raw["end_date"],
        season_code=raw["season_code"],
        suggested_type_id=raw.get("suggested_type_id"),
        segments=[UsfsSegmentSlot(**segment) for segment in raw.get("segments") or []],
    )
    where = checklist.location or "location not listed"
    when = "–".join(
        part for part in (checklist.start_date, checklist.end_date) if part
    )
    st.caption(f"{checklist.name} · {when} · {where}")

    st.text_input("Competition name", key="usfs_comp_name")
    st.text_input(
        "Season year code",
        key="usfs_season",
        help="For example 2627 for the 2026–27 season. Filled from the event start date.",
    )
    analytics = get_analytics_safe()
    oa_types = analytics.get_officials_analysis_competition_types()
    if not oa_types:
        st.error(
            "No officials competition types are loaded. Import activity or directory data first."
        )
        return
    oa_ids = sorted(tid for tid, _name in oa_types)
    if st.session_state.get("usfs_oa_type") not in oa_ids:
        st.session_state["usfs_oa_type"] = (
            OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING
            if OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING in oa_ids
            else oa_ids[0]
        )

    def _fmt_type(opt_id: int) -> str:
        type_name = next((n for tid, n in oa_types if tid == opt_id), "")
        return format_officials_competition_type_select_label(opt_id, type_name or None)

    st.selectbox(
        "Officials competition type",
        options=oa_ids,
        format_func=_fmt_type,
        key="usfs_oa_type",
    )

    uploads = st.file_uploader(
        "Judges scores PDFs",
        type=["pdf"],
        accept_multiple_files=True,
        key="usfs_pdfs",
        help="Drop every judges-scores PDF for this event. Names must match the expected filename.",
    )
    filenames = [item.name for item in uploads] if uploads else []
    matched, unmatched = match_uploaded_pdfs(checklist.segments, filenames)
    matched_files = {slot.pdf_filename.lower(): filename for filename, slot in matched.items()}
    release_analytics_db_connection()
    try:
        loaded_keys = already_imported_keys(
            checklist.segments,
            stored_segment_names(checklist.event_url),
        )
    except Exception:
        loaded_keys = set()
        st.caption("Could not check which segments are already in the database.")

    rows = []
    for slot in checklist.segments:
        uploaded = matched_files.get(slot.pdf_filename.lower(), "")
        loaded = (slot.category_id, slot.segment_code) in loaded_keys
        if slot.officials_error:
            state = "Officials page failed"
        elif not slot.judges:
            state = "No judges"
        elif uploaded:
            state = "Matched"
        elif loaded:
            state = "Already imported"
        else:
            state = "Need PDF"
        rows.append(
            {
                "Status": state,
                "Segment": slot.label,
                "Level": slot.level,
                "Discipline": slot.discipline,
                "Judges": ", ".join(
                    f"J{index} {name}" for index, name in enumerate(slot.judges, start=1)
                ),
                "Expected PDF": slot.pdf_filename,
                "Uploaded file": uploaded,
            }
        )
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    ready = [
        slot
        for slot in checklist.segments
        if slot.pdf_filename.lower() in matched_files and slot.judges and not slot.officials_error
    ]
    n_loaded = sum(
        1
        for slot in checklist.segments
        if (slot.category_id, slot.segment_code) in loaded_keys
    )
    n_need = sum(1 for row in rows if row["Status"] == "Need PDF")
    st.caption(
        f"{len(checklist.segments)} final segments, {n_loaded} already imported, "
        f"{len(ready)} ready to import, {n_need} still need a PDF."
    )
    if unmatched:
        st.warning(
            "These files do not match a final segment filename: "
            + ", ".join(unmatched)
        )

    if st.button("Import matched PDFs", type="primary", key="usfs_import"):
        if not ready:
            st.error("No matched PDFs with a judge panel to import.")
            return
        payload = [
            (item.name, item.getvalue())
            for item in uploads
            if Path(item.name).name.lower() in matched_files
        ]
        release_analytics_db_connection()
        try:
            with st.spinner("Importing…"):
                outcomes = import_usfs_segments(
                    checklist,
                    payload,
                    competition_name=st.session_state.get("usfs_comp_name") or "",
                    season_code=st.session_state.get("usfs_season") or "",
                    officials_type_id=int(st.session_state.get("usfs_oa_type")),
                )
        except Exception as exc:
            st.error(f"Import failed: {exc}")
            return
        finally:
            release_analytics_db_connection()
        imported = [row for row in outcomes if row.status == "imported"]
        skipped = [row for row in outcomes if row.status == "skipped"]
        errors = [row for row in outcomes if row.status == "error"]
        if imported:
            st.success(
                "Imported " + ", ".join(row.label for row in imported) + "."
            )
            st.cache_data.clear()
        if skipped:
            st.info(
                "Already in the database (scores kept, panel refreshed): "
                + ", ".join(row.label for row in skipped)
                + "."
            )
        for row in errors:
            st.error(f"{row.label}: {row.detail}")
        if imported or skipped:
            st.caption(
                "Analytics caches were not rebuilt. Run the precompute scripts when you want "
                "rankings and PCS quality to include this competition."
            )
