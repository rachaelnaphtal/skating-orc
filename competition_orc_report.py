"""
Competition ORC workbook from scores already stored on a competition.

Same sheets as the URL scrape in the standalone report app (per-segment anomaly
sheets, summary, optional thrown-out analysis), without fetching protocols again.
"""

from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from database_loader import judge_person_match_key, normalize_scraped_judge_name
from models import (
    Competition,
    Element,
    ElementScorePerJudge,
    Judge,
    PcsScorePerJudge,
    PcsType,
    Segment,
    SegmentOfficial,
    Skater,
    SkaterSegment,
)

_JUDGE_ROLE_RE = re.compile(r"^judge\s+no\.?\s*(\d+)\s*$", re.IGNORECASE)


@dataclass
class CompetitionOrcReport:
    filename: str
    workbook: bytes
    additional_filename: str | None = None
    additional_workbook: bytes | None = None
    key_filename: str | None = None
    key_workbook: bytes | None = None
    segment_names: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def safe_report_filename(name: str) -> str:
    """File stem for a generated workbook. Rejects blank names and path characters."""
    cleaned = " ".join((name or "").split())
    if not cleaned:
        raise ValueError("Report name is required.")
    if re.search(r'[\\/:*?"<>|]', cleaned):
        raise ValueError("Report name cannot include \\ / : * ? \" < > |.")
    return cleaned


def segment_matches_event_regex(name: str, event_regex: str) -> bool:
    """Same rule as the scrape: a blank pattern includes every segment."""
    pattern = event_regex or ""
    if not pattern:
        return True
    return re.match(pattern, name) is not None


def compile_event_regex(event_regex: str) -> str:
    """Return ``event_regex`` after checking that it compiles. Blank is allowed."""
    pattern = (event_regex or "").strip()
    if pattern:
        re.compile(pattern)
    return pattern


def panel_judge_slots(
    officials: list[tuple[str, str]],
    score_judges: dict[int, str],
) -> tuple[list[tuple[int | None, str]], list[str]]:
    """
    Panel order for one segment.

    ``Judge No.N`` rows set J1..Jn. Names are matched to scoring judges
    case-insensitively. Scoring judges who are not on that panel are appended.
    When the segment has no Judge No. rows, scoring judges are listed
    alphabetically and a warning is returned.
    """
    warnings: list[str] = []
    by_key: dict[str, list[tuple[int, str]]] = {}
    for judge_id, judge_name in score_judges.items():
        by_key.setdefault(judge_person_match_key(judge_name), []).append(
            (int(judge_id), judge_name)
        )

    numbered: list[tuple[int, str]] = []
    for role, official_name in officials:
        match = _JUDGE_ROLE_RE.match((role or "").strip())
        if not match:
            continue
        numbered.append((int(match.group(1)), official_name or ""))
    numbered.sort(key=lambda item: item[0])

    used_ids: set[int] = set()
    slots: list[tuple[int | None, str]] = []
    if numbered:
        width = max(number for number, _name in numbered)
        slots = [(None, "")] * width
        for number, official_name in numbered:
            key = judge_person_match_key(official_name)
            choices = [pair for pair in by_key.get(key, []) if pair[0] not in used_ids]
            if choices:
                judge_id, judge_name = choices[0]
                used_ids.add(judge_id)
                slots[number - 1] = (judge_id, judge_name)
            else:
                label = normalize_scraped_judge_name(official_name) or official_name
                slots[number - 1] = (None, label or f"Judge {number}")
        for index, (judge_id, judge_name) in enumerate(slots):
            if judge_id is None and not judge_name:
                slots[index] = (None, f"Judge {index + 1}")
    extras = [
        (int(judge_id), judge_name)
        for judge_id, judge_name in score_judges.items()
        if int(judge_id) not in used_ids
    ]
    extras.sort(key=lambda pair: pair[1].lower())
    if numbered and extras:
        names = ", ".join(name for _jid, name in extras)
        warnings.append(
            f"Scoring judges not listed as Judge No. were added after the panel: {names}."
        )
    elif not numbered and extras:
        warnings.append(
            "No Judge No. officials on this segment; judges are listed alphabetically."
        )
    slots.extend(extras)
    return slots, warnings


def build_segment_protocol(
    officials: list[tuple[str, str]],
    element_rows: list[dict],
    pcs_rows: list[dict],
) -> tuple[list[str], dict, dict, list[str]]:
    """
    Rebuild the structures ``findElementDeviations`` / ``findPCSDeviations`` expect.

    Score lists are aligned to panel order. A judge with no stored mark is ``None``.
    """
    score_judges: dict[int, str] = {}
    for row in element_rows:
        score_judges[int(row["judge_id"])] = str(row["judge_name"])
    for row in pcs_rows:
        score_judges.setdefault(int(row["judge_id"]), str(row["judge_name"]))

    slots, warnings = panel_judge_slots(officials, score_judges)
    judges = [name for _judge_id, name in slots]
    index_by_id = {
        int(judge_id): index
        for index, (judge_id, _name) in enumerate(slots)
        if judge_id is not None
    }

    skaters: dict[int, tuple] = {}
    elements: dict[int, dict] = {}
    for row in element_rows:
        ssid = int(row["skater_segment_id"])
        skaters[ssid] = _skater_sort_key(row)
        element_id = int(row["element_id"])
        element = elements.get(element_id)
        if element is None:
            element = {
                "skater_segment_id": ssid,
                "element_id": element_id,
                "element_number": _optional_int(row.get("element_number")),
                "Element": str(row["element"]),
                "Notes": _notes(row.get("notes")),
                "scores": [None] * len(slots),
            }
            elements[element_id] = element
        _place_score(element["scores"], index_by_id, int(row["judge_id"]), row.get("score"))

    pcs_by_key: dict[tuple[int, int], dict] = {}
    for row in pcs_rows:
        ssid = int(row["skater_segment_id"])
        skaters.setdefault(ssid, _skater_sort_key(row))
        key = (ssid, int(row["pcs_type_id"]))
        component = pcs_by_key.get(key)
        if component is None:
            component = {
                "skater_segment_id": ssid,
                "pcs_type_id": int(row["pcs_type_id"]),
                "Component": str(row["component"]),
                "scores": [None] * len(slots),
            }
            pcs_by_key[key] = component
        _place_score(component["scores"], index_by_id, int(row["judge_id"]), row.get("score"))

    elements_per_skater: dict[str, list] = {}
    pcs_per_skater: dict[str, list] = {}
    for ssid, _sort_key in sorted(skaters.items(), key=lambda item: item[1]):
        skater_name = _sort_key[2]
        skater_elements = [
            element
            for element in elements.values()
            if element["skater_segment_id"] == ssid
        ]
        skater_elements.sort(
            key=lambda element: (
                element["element_number"] is None,
                element["element_number"] if element["element_number"] is not None else 0,
                element["element_id"],
            )
        )
        if skater_elements:
            elements_per_skater[skater_name] = [
                {
                    "Element": element["Element"],
                    "Notes": element["Notes"],
                    "Number": element["element_number"],
                    "Scores": list(element["scores"]),
                }
                for element in skater_elements
            ]
        skater_pcs = [
            component
            for component in pcs_by_key.values()
            if component["skater_segment_id"] == ssid
        ]
        skater_pcs.sort(key=lambda component: component["pcs_type_id"])
        if skater_pcs:
            pcs_per_skater[skater_name] = [
                {"Component": component["Component"], "Scores": list(component["scores"])}
                for component in skater_pcs
            ]
    return judges, elements_per_skater, pcs_per_skater, warnings


def build_orc_workbook(
    report_name: str,
    segments: list[tuple[str, list[str], dict, dict]],
    *,
    judge_filter: str = "",
    only_rule_errors: bool = False,
    add_additional_analysis: bool = False,
    competition_start_date=None,
    competition_end_date=None,
    competition_year=None,
) -> tuple[bytes, bytes | None]:
    """
    Build the ORC workbook for segments already in protocol shape.

    Each segment is ``(name, judges, elements_per_skater, pcs_per_skater)``.
    """
    import openpyxl

    from downloadResults import (
        create_additional_analysis_sheet,
        handleEventResults,
        make_competition_summary_page,
    )
    from judgingParsing import (
        count_total_errors_per_judge,
        create_all_element_dict,
        create_all_pcs_dict,
        detect_element_rule_errors,
        exclude_anomalies_already_flagged_as_rule_errors,
        findElementDeviations,
        findPCSDeviations,
        get_allowed_errors,
        printToExcel,
    )
    from pcs_fall_rule_errors import detect_pcs_fall_rule_errors

    filename = safe_report_filename(report_name)
    workbook = openpyxl.Workbook()
    judge_errors: dict = {}
    event_details: dict = {}
    detailed_rule_errors: list = []
    agg_elements = None
    agg_pcs = None

    for index, (event_name, judges, elements_per_skater, pcs_per_skater) in enumerate(segments):
        element_errors = detect_element_rule_errors(
            elements_per_skater,
            judges,
            event_name,
            judge_filter=judge_filter,
            competition_start_date=competition_start_date,
            competition_end_date=competition_end_date,
        )
        element_errors = element_errors + detect_pcs_fall_rule_errors(
            elements_per_skater,
            pcs_per_skater,
            judges,
            event_name,
            judge_filter=judge_filter,
            competition_year=competition_year,
            competition_start_date=competition_start_date,
            competition_end_date=competition_end_date,
        )
        element_deviations = []
        pcs_errors = []
        if not only_rule_errors:
            element_deviations = findElementDeviations(
                elements_per_skater,
                judges,
                judge_filter=judge_filter,
                event_name=event_name,
            )
            element_deviations = exclude_anomalies_already_flagged_as_rule_errors(
                element_errors, element_deviations
            )
            pcs_errors = findPCSDeviations(
                pcs_per_skater,
                judges,
                judge_filter=judge_filter,
                event_name=event_name,
            )
        total_errors = count_total_errors_per_judge(
            judges, element_errors, element_deviations, pcs_errors
        )
        printToExcel(
            workbook,
            event_name,
            judges,
            element_errors,
            element_deviations,
            pcs_errors,
            index,
        )
        skater_names = list(dict.fromkeys([*elements_per_skater, *pcs_per_skater]))
        all_element_dict = {}
        all_pcs_dict = {}
        if add_additional_analysis:
            all_element_dict = create_all_element_dict(
                judges,
                elements_per_skater,
                event_name,
                pcs_per_skater=pcs_per_skater,
            )
            all_pcs_dict = create_all_pcs_dict(judges, pcs_per_skater, event_name)
        agg_elements, agg_pcs = handleEventResults(
            filename,
            False,
            judge_filter,
            agg_elements,
            agg_pcs,
            None,
            0,
            [],
            judge_errors,
            event_details,
            detailed_rule_errors,
            index,
            judges,
            event_name,
            total_errors,
            len(skater_names),
            get_allowed_errors(len(skater_names)),
            element_errors,
            all_element_dict,
            all_pcs_dict,
        )

    if "Sheet" in workbook.sheetnames and len(workbook.sheetnames) > 1:
        del workbook["Sheet"]
    workbook._sheets.sort(key=lambda ws: ws.title)
    if event_details:
        make_competition_summary_page(workbook, filename, event_details, judge_errors)

    payload = BytesIO()
    workbook.save(payload)
    additional = None
    if add_additional_analysis and _frame_has_rows(agg_elements):
        import pandas as pd

        pcs_frame = agg_pcs
        if pcs_frame is None or getattr(pcs_frame, "empty", True):
            pcs_frame = pd.DataFrame(
                columns=["Judge Name", "Component", "Thrown out", "High"]
            )
        with tempfile.TemporaryDirectory() as tmp:
            folder = str(Path(tmp)) + "/"
            create_additional_analysis_sheet(
                agg_elements,
                pcs_frame,
                folder,
                filename,
                use_gcp=False,
            )
            extra_path = Path(tmp) / f"{filename}_Additional_Analysis.xlsx"
            additional = extra_path.read_bytes()
    return payload.getvalue(), additional


def generate_competition_orc_report(
    session: Session,
    competition_id: int,
    report_name: str,
    *,
    event_regex: str = "",
    judge_filter: str = "",
    only_rule_errors: bool = False,
    add_additional_analysis: bool = False,
    anonymize_judges: bool = False,
) -> CompetitionOrcReport:
    """Build an ORC workbook for one competition from stored element and PCS scores."""
    pattern = compile_event_regex(event_regex)
    filename = safe_report_filename(report_name)
    competition = session.get(Competition, int(competition_id))
    if competition is None:
        raise ValueError(f"No competition with id {competition_id}.")

    segment_rows = session.execute(
        select(Segment.id, Segment.name)
        .where(Segment.competition_id == competition.id)
        .order_by(Segment.id)
    ).all()
    if not segment_rows:
        raise ValueError("This competition has no segments.")

    selected = [
        (int(segment_id), str(name))
        for segment_id, name in segment_rows
        if segment_matches_event_regex(str(name), pattern)
    ]
    if not selected:
        raise ValueError("No stored segments match the event filter.")

    segment_ids = [segment_id for segment_id, _name in selected]
    officials = _officials_by_segment(session, segment_ids)
    elements = _element_rows_by_segment(session, segment_ids)
    pcs = _pcs_rows_by_segment(session, segment_ids)

    prepared = []
    warnings: list[str] = []
    included: list[str] = []
    for segment_id, name in selected:
        element_rows = elements.get(segment_id, [])
        pcs_rows = pcs.get(segment_id, [])
        if not element_rows and not pcs_rows:
            warnings.append(f"{name}: no stored scores, skipped.")
            continue
        judges, elements_per_skater, pcs_per_skater, panel_warnings = build_segment_protocol(
            officials.get(segment_id, []),
            element_rows,
            pcs_rows,
        )
        for note in panel_warnings:
            warnings.append(f"{name}: {note}")
        if not judges:
            warnings.append(f"{name}: no judges, skipped.")
            continue
        prepared.append((name, judges, elements_per_skater, pcs_per_skater))
        included.append(name)

    if not prepared:
        raise ValueError("No stored scores to report for the selected segments.")

    workbook, additional = build_orc_workbook(
        filename,
        prepared,
        judge_filter=judge_filter or "",
        only_rule_errors=only_rule_errors,
        add_additional_analysis=add_additional_analysis,
        competition_start_date=competition.start_date,
        competition_end_date=competition.end_date,
        competition_year=competition.year,
    )
    key_filename = None
    key_workbook = None
    if anonymize_judges:
        from orc_anonymize import anonymize_report_workbooks, relabel_official_names

        panel_names = [name for _event, judges, _elements, _pcs in prepared for name in judges]
        workbook, additional, key_workbook, mapping = anonymize_report_workbooks(
            workbook,
            additional,
            panel_names,
        )
        key_filename = f"{filename}_Judge_Key.xlsx"
        warnings = [relabel_official_names(note, mapping) for note in warnings]
    return CompetitionOrcReport(
        filename=f"{filename}.xlsx",
        workbook=workbook,
        additional_filename=(
            f"{filename}_Additional_Analysis.xlsx" if additional else None
        ),
        additional_workbook=additional,
        key_filename=key_filename,
        key_workbook=key_workbook,
        segment_names=included,
        warnings=warnings,
    )


def competition_segment_names(session: Session, competition_id: int) -> list[str]:
    rows = session.execute(
        select(Segment.name)
        .where(Segment.competition_id == int(competition_id))
        .order_by(Segment.id)
    ).all()
    return [str(row[0]) for row in rows]


def competition_judge_names(session: Session, competition_id: int) -> list[str]:
    """Stored judge names that have an element or PCS score in this competition."""
    element_names = session.execute(
        select(Judge.name)
        .join(ElementScorePerJudge, ElementScorePerJudge.judge_id == Judge.id)
        .join(Element, Element.id == ElementScorePerJudge.element_id)
        .join(SkaterSegment, SkaterSegment.id == Element.skater_segment_id)
        .join(Segment, Segment.id == SkaterSegment.segment_id)
        .where(Segment.competition_id == int(competition_id))
        .distinct()
    ).all()
    pcs_names = session.execute(
        select(Judge.name)
        .join(PcsScorePerJudge, PcsScorePerJudge.judge_id == Judge.id)
        .join(SkaterSegment, SkaterSegment.id == PcsScorePerJudge.skater_segment_id)
        .join(Segment, Segment.id == SkaterSegment.segment_id)
        .where(Segment.competition_id == int(competition_id))
        .distinct()
    ).all()
    names = {str(row[0]) for row in element_names}
    names.update(str(row[0]) for row in pcs_names)
    return sorted(names, key=str.lower)


def _officials_by_segment(
    session: Session, segment_ids: list[int]
) -> dict[int, list[tuple[str, str]]]:
    rows = session.execute(
        select(
            SegmentOfficial.segment_id,
            SegmentOfficial.role,
            SegmentOfficial.official_name,
        ).where(SegmentOfficial.segment_id.in_(segment_ids))
    ).all()
    grouped: dict[int, list[tuple[str, str]]] = {}
    for segment_id, role, official_name in rows:
        grouped.setdefault(int(segment_id), []).append((str(role), str(official_name)))
    return grouped


def _element_rows_by_segment(session: Session, segment_ids: list[int]) -> dict[int, list[dict]]:
    rows = session.execute(
        select(
            SkaterSegment.segment_id,
            SkaterSegment.id,
            SkaterSegment.start_number,
            Skater.name,
            Element.id,
            Element.element_number,
            Element.name,
            Element.notes,
            ElementScorePerJudge.judge_id,
            Judge.name,
            ElementScorePerJudge.judge_score,
        )
        .join(Element, Element.skater_segment_id == SkaterSegment.id)
        .join(ElementScorePerJudge, ElementScorePerJudge.element_id == Element.id)
        .join(Skater, Skater.id == SkaterSegment.skater_id)
        .join(Judge, Judge.id == ElementScorePerJudge.judge_id)
        .where(SkaterSegment.segment_id.in_(segment_ids))
    ).all()
    grouped: dict[int, list[dict]] = {}
    for row in rows:
        grouped.setdefault(int(row[0]), []).append(
            {
                "skater_segment_id": int(row[1]),
                "start_number": row[2],
                "skater": str(row[3]),
                "element_id": int(row[4]),
                "element_number": row[5],
                "element": str(row[6]),
                "notes": row[7],
                "judge_id": int(row[8]),
                "judge_name": str(row[9]),
                "score": row[10],
            }
        )
    return grouped


def _pcs_rows_by_segment(session: Session, segment_ids: list[int]) -> dict[int, list[dict]]:
    rows = session.execute(
        select(
            SkaterSegment.segment_id,
            SkaterSegment.id,
            SkaterSegment.start_number,
            Skater.name,
            PcsType.id,
            PcsType.name,
            PcsScorePerJudge.judge_id,
            Judge.name,
            PcsScorePerJudge.judge_score,
        )
        .join(PcsScorePerJudge, PcsScorePerJudge.skater_segment_id == SkaterSegment.id)
        .join(PcsType, PcsType.id == PcsScorePerJudge.pcs_type_id)
        .join(Skater, Skater.id == SkaterSegment.skater_id)
        .join(Judge, Judge.id == PcsScorePerJudge.judge_id)
        .where(SkaterSegment.segment_id.in_(segment_ids))
    ).all()
    grouped: dict[int, list[dict]] = {}
    for row in rows:
        grouped.setdefault(int(row[0]), []).append(
            {
                "skater_segment_id": int(row[1]),
                "start_number": row[2],
                "skater": str(row[3]),
                "pcs_type_id": int(row[4]),
                "component": str(row[5]),
                "judge_id": int(row[6]),
                "judge_name": str(row[7]),
                "score": row[8],
            }
        )
    return grouped


def _skater_sort_key(row: dict) -> tuple:
    start = row.get("start_number")
    start_number = int(start) if start is not None else 10**9
    return (start_number, int(row["skater_segment_id"]), str(row["skater"]))


def _optional_int(value) -> int | None:
    if value is None:
        return None
    return int(value)


def _notes(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _place_score(scores: list, index_by_id: dict[int, int], judge_id: int, score) -> None:
    index = index_by_id.get(judge_id)
    if index is None or score is None:
        return
    scores[index] = float(score)


def _frame_has_rows(frame) -> bool:
    return frame is not None and not getattr(frame, "empty", True)
