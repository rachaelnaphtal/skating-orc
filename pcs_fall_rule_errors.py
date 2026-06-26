"""PCS caps after falls (program component scores)."""

from __future__ import annotations

import re
from decimal import Decimal

from rule_errors_policy import (
    segment_supports_pcs_fall_rule_errors,
    should_flag_pcs_fall_rule_errors,
)

PCS_MAX_ONE_FALL = Decimal("9.5")
PCS_MAX_TWO_OR_MORE_FALLS = Decimal("8.75")

_FX_IN_NOTES_RE = re.compile(r"Fx", re.IGNORECASE)


def _judge_name_allowed_by_filter(judge_name: str, judge_filter: str) -> bool:
    if not judge_filter or not str(judge_filter).strip():
        return True
    allowed = {p.strip() for p in str(judge_filter).split(",") if p.strip()}
    return judge_name in allowed


def falls_from_element_notes(notes: str | None) -> int:
    """
    Fall count contributed by one element's info column.

    ``Fx`` means multiple falls on that element (counts as 2). ``F`` without ``x``
    means one fall.
    """
    if not notes:
        return 0
    text = str(notes)
    if _FX_IN_NOTES_RE.search(text):
        return 2
    if "F" in text:
        return 1
    return 0


def program_fall_count_from_elements(elements: list[dict]) -> int:
    """Total falls in a program from parsed element rows or DB element rows."""
    total = 0
    for element in elements:
        notes = element.get("Notes")
        if notes is None and "notes" in element:
            notes = element.get("notes")
        total += falls_from_element_notes(notes)
    return total


def max_pcs_for_fall_count(fall_count: int) -> Decimal | None:
    """Maximum allowed PCS component score for a program fall count, or None if no cap."""
    if fall_count <= 0:
        return None
    if fall_count == 1:
        return PCS_MAX_ONE_FALL
    return PCS_MAX_TWO_OR_MORE_FALLS


def pcs_score_exceeds_fall_limit(score, max_allowed: Decimal) -> bool:
    if score is None:
        return False
    try:
        return Decimal(str(score)) > max_allowed
    except Exception:
        return False


def detect_pcs_fall_rule_errors(
    elements_per_skater: dict,
    pcs_per_skater: dict,
    judges: list,
    event_name: str,
    *,
    judge_filter: str = "",
    competition_year=None,
    competition_start_date=None,
    competition_end_date=None,
) -> list[dict]:
    """Flag PCS component scores above the fall-count cap for each skater."""
    if not should_flag_pcs_fall_rule_errors(
        competition_year,
        competition_start_date,
        competition_end_date,
    ):
        return []
    if not segment_supports_pcs_fall_rule_errors(event_name):
        return []

    errors: list[dict] = []
    for skater, elements in elements_per_skater.items():
        fall_count = program_fall_count_from_elements(elements)
        max_pcs = max_pcs_for_fall_count(fall_count)
        if max_pcs is None:
            continue
        for component in pcs_per_skater.get(skater, []):
            all_scores = component.get("Scores") or []
            component_name = str(component.get("Component", ""))
            for judge_number in range(1, len(all_scores) + 1):
                score = all_scores[judge_number - 1]
                if score is None:
                    continue
                if judge_number > len(judges):
                    continue
                judge_name = judges[judge_number - 1]
                if not _judge_name_allowed_by_filter(judge_name, judge_filter):
                    continue
                if not pcs_score_exceeds_fall_limit(score, max_pcs):
                    continue
                errors.append(
                    make_pcs_fall_rule_error(
                        skater,
                        component_name,
                        judge_number,
                        judge_name,
                        score,
                        fall_count=fall_count,
                        max_pcs_allowed=max_pcs,
                    )
                )
    return errors


def make_pcs_fall_rule_error(
    skater: str,
    component: str,
    judge_number: int,
    judge_name: str,
    judge_score,
    *,
    fall_count: int,
    max_pcs_allowed: Decimal,
) -> dict:
    fall_label = "fall" if fall_count == 1 else "falls"
    return {
        "Skater": skater,
        "Element": "",
        "Component": component,
        "Judge Number": judge_number,
        "Judge Name": judge_name,
        "Judge Score": judge_score,
        "Panel Average": None,
        "Deviation": "Rule Error",
        "Description": (
            f"Max PCS with {fall_count} {fall_label} is {max_pcs_allowed}"
        ),
        "Max PCS Allowed": float(max_pcs_allowed),
        "Fall Count": fall_count,
    }
