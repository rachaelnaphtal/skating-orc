"""
Store and load international judge availability workbooks in PostgreSQL.

Reloading the same **label** replaces responses and normalized event availability
while reusing/updating event column metadata.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

try:
    from activityAnalysis.international_availability import (
        availability_cell_code,
        event_column_short_label,
        load_international_availability_workbook,
        parse_workbook_structure,
        supplemental_column_short_label,
        _normalize_email,
        _normalize_person_name,
    )
    from activityAnalysis.load_activity_data import _json_safe_qualifying_value, get_engine
    from activityAnalysis.officials_analysis_models import (
        InternationalAvailabilityEvent,
        InternationalAvailabilityForm,
        InternationalFormResponse,
        InternationalResponseEventAvailability,
    )
except ModuleNotFoundError:
    from international_availability import (
        availability_cell_code,
        event_column_short_label,
        load_international_availability_workbook,
        parse_workbook_structure,
        supplemental_column_short_label,
        _normalize_email,
        _normalize_person_name,
    )
    from load_activity_data import _json_safe_qualifying_value, get_engine
    from officials_analysis_models import (
        InternationalAvailabilityEvent,
        InternationalAvailabilityForm,
        InternationalFormResponse,
        InternationalResponseEventAvailability,
    )

DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL = "2026-27 International Judge Availability"


def response_lookup_key(*, email: object, first_name: object, last_name: object) -> str:
    """Stable key for deduplicating form rows within a workbook."""
    normalized_email = _normalize_email(email)
    if normalized_email:
        return f"email:{normalized_email}"
    name_key = _normalize_person_name(first_name, last_name)
    if name_key:
        return f"name:{name_key}"
    return ""


def _row_to_response_json(row: pd.Series) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in row.index:
        if not isinstance(col, str):
            continue
        out[col] = _json_safe_qualifying_value(row[col])
    return out


def list_international_availability_forms(*, engine=None) -> pd.DataFrame:
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        rows = session.execute(
            select(
                InternationalAvailabilityForm.id,
                InternationalAvailabilityForm.label,
                InternationalAvailabilityForm.source_filename,
                InternationalAvailabilityForm.loaded_at,
            ).order_by(InternationalAvailabilityForm.loaded_at.desc())
        ).all()
    return pd.DataFrame(
        rows,
        columns=["form_id", "label", "source_filename", "loaded_at"],
    )


def get_international_availability_form(
    *,
    form_id: int | None = None,
    label: str | None = None,
    engine=None,
) -> InternationalAvailabilityForm | None:
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        if form_id is not None:
            return session.get(InternationalAvailabilityForm, int(form_id))
        if label:
            return session.scalar(
                select(InternationalAvailabilityForm).where(
                    InternationalAvailabilityForm.label == label
                )
            )
        return session.scalar(
            select(InternationalAvailabilityForm).order_by(
                InternationalAvailabilityForm.loaded_at.desc()
            ).limit(1)
        )


def layout_from_stored_events(events: list[InternationalAvailabilityEvent]) -> dict[str, Any]:
    """Rebuild workbook layout metadata from stored event rows."""
    event_cols: list[str] = []
    supplemental_cols: list[str] = []
    for event in sorted(events, key=lambda e: (e.sort_order, e.id)):
        if event.column_kind == "supplemental":
            supplemental_cols.append(event.prompt_key)
        else:
            event_cols.append(event.prompt_key)
    layout = {
        "first_name_col": "First Name",
        "last_name_col": "Last Name",
        "email_col": "Email Address",
        "sp_level_col": "International Judging Level - Singles/Pairs",
        "dance_level_col": "International Judging Level - Ice Dance",
        "event_cols": event_cols,
        "supplemental_cols": supplemental_cols,
        "event_short_labels": {
            col: event_column_short_label(col) for col in event_cols
        },
        "supplemental_short_labels": {
            col: supplemental_column_short_label(col) for col in supplemental_cols
        },
    }
    return layout


def international_form_to_dataframe(
    form_id: int,
    *,
    engine=None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Reconstruct the workbook dataframe and layout from stored responses."""
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        events = session.scalars(
            select(InternationalAvailabilityEvent)
            .where(InternationalAvailabilityEvent.form_id == int(form_id))
            .order_by(InternationalAvailabilityEvent.sort_order.asc())
        ).all()
        responses = session.scalars(
            select(InternationalFormResponse)
            .where(InternationalFormResponse.form_id == int(form_id))
            .order_by(
                InternationalFormResponse.last_name.asc().nulls_last(),
                InternationalFormResponse.first_name.asc().nulls_last(),
            )
        ).all()

    layout = layout_from_stored_events(list(events))
    if not responses:
        columns = list(
            dict.fromkeys(
                [
                    layout.get("first_name_col"),
                    layout.get("last_name_col"),
                    layout.get("email_col"),
                    layout.get("sp_level_col"),
                    layout.get("dance_level_col"),
                    *layout["event_cols"],
                    *layout["supplemental_cols"],
                ]
            )
        )
        columns = [c for c in columns if c]
        return pd.DataFrame(columns=columns), layout

    rows = [resp.response_json for resp in responses]
    df = pd.DataFrame(rows)
    for col in layout["event_cols"] + layout["supplemental_cols"]:
        if col not in df.columns:
            df[col] = pd.NA
    return df, layout


def load_international_availability_form_workbook(
    path: str,
    *,
    label: str | None = None,
    sheet_name: str | None = None,
    replace_existing_label: bool = True,
    commit: bool = True,
    engine=None,
) -> dict[str, Any]:
    """
    Ingest an international availability workbook into ``international_*`` tables.

    When ``replace_existing_label`` is true, reloading the same label deletes prior
    responses and availability rows for that form before inserting new data.
    """
    db_engine = engine or get_engine()
    df = load_international_availability_workbook(path, sheet_name=sheet_name)
    layout = parse_workbook_structure(df)
    event_cols: list[str] = layout["event_cols"]
    supplemental_cols: list[str] = layout["supplemental_cols"]
    if not event_cols:
        raise ValueError("No event availability columns found in workbook.")

    form_label = (label or "").strip() or DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL
    first_col = layout.get("first_name_col")
    last_col = layout.get("last_name_col")
    email_col = layout.get("email_col")

    result: dict[str, Any] = {
        "form_id": None,
        "form_label": form_label,
        "rows_read": len(df),
        "events": len(event_cols),
        "supplemental_columns": len(supplemental_cols),
        "responses_stored": 0,
        "availability_rows": 0,
        "skipped_blank_identity": 0,
        "duplicate_rows_dropped": 0,
        "events_reused": 0,
        "events_new": 0,
        "events_removed": 0,
    }

    with Session(db_engine) as session:
        existing_form = None
        if replace_existing_label:
            existing_form = session.scalar(
                select(InternationalAvailabilityForm).where(
                    InternationalAvailabilityForm.label == form_label
                )
            )

        event_id_by_prompt: dict[str, int] = {}
        if existing_form is not None:
            form_id = int(existing_form.id)
            result["form_id"] = form_id
            existing_form.source_filename = os.path.basename(path)
            existing_form.loaded_at = datetime.now(timezone.utc)

            session.execute(
                delete(InternationalFormResponse).where(
                    InternationalFormResponse.form_id == form_id
                )
            )

            existing_events = session.scalars(
                select(InternationalAvailabilityEvent).where(
                    InternationalAvailabilityEvent.form_id == form_id
                )
            ).all()
            by_prompt = {e.prompt_key: e for e in existing_events}
            matched_ids: set[int] = set()
            order = 0
            for prompt in event_cols:
                short = layout["event_short_labels"][prompt]
                event = by_prompt.get(prompt)
                if event is None:
                    event = InternationalAvailabilityEvent(
                        form_id=form_id,
                        prompt_key=prompt,
                        short_label=short,
                        column_kind="event",
                        sort_order=order,
                    )
                    session.add(event)
                    session.flush()
                    result["events_new"] += 1
                else:
                    event.short_label = short
                    event.column_kind = "event"
                    event.sort_order = order
                    result["events_reused"] += 1
                event_id_by_prompt[prompt] = int(event.id)
                matched_ids.add(int(event.id))
                order += 1

            for prompt in supplemental_cols:
                short = layout["supplemental_short_labels"][prompt]
                event = by_prompt.get(prompt)
                if event is None:
                    event = InternationalAvailabilityEvent(
                        form_id=form_id,
                        prompt_key=prompt,
                        short_label=short,
                        column_kind="supplemental",
                        sort_order=order,
                    )
                    session.add(event)
                    session.flush()
                    result["events_new"] += 1
                else:
                    event.short_label = short
                    event.column_kind = "supplemental"
                    event.sort_order = order
                    result["events_reused"] += 1
                matched_ids.add(int(event.id))
                order += 1

            for event in existing_events:
                if int(event.id) not in matched_ids:
                    session.delete(event)
                    result["events_removed"] += 1
            session.flush()
        else:
            form = InternationalAvailabilityForm(
                label=form_label,
                source_filename=os.path.basename(path),
            )
            session.add(form)
            session.flush()
            form_id = int(form.id)
            result["form_id"] = form_id

            order = 0
            for prompt in event_cols:
                event = InternationalAvailabilityEvent(
                    form_id=form_id,
                    prompt_key=prompt,
                    short_label=layout["event_short_labels"][prompt],
                    column_kind="event",
                    sort_order=order,
                )
                session.add(event)
                session.flush()
                event_id_by_prompt[prompt] = int(event.id)
                result["events_new"] += 1
                order += 1
            for prompt in supplemental_cols:
                session.add(
                    InternationalAvailabilityEvent(
                        form_id=form_id,
                        prompt_key=prompt,
                        short_label=layout["supplemental_short_labels"][prompt],
                        column_kind="supplemental",
                        sort_order=order,
                    )
                )
                order += 1
                result["events_new"] += 1

        seen_keys: set[str] = set()
        for _, row in df.iterrows():
            lookup = response_lookup_key(
                email=row.get(email_col) if email_col else None,
                first_name=row.get(first_col) if first_col else None,
                last_name=row.get(last_col) if last_col else None,
            )
            if not lookup:
                result["skipped_blank_identity"] += 1
                continue
            if lookup in seen_keys:
                result["duplicate_rows_dropped"] += 1
                continue
            seen_keys.add(lookup)

            payload = _row_to_response_json(row)
            response = InternationalFormResponse(
                form_id=form_id,
                response_key=lookup,
                email=_normalize_email(row.get(email_col)) if email_col else None,
                first_name=str(row.get(first_col)).strip()
                if first_col and row.get(first_col) is not None and not pd.isna(row.get(first_col))
                else None,
                last_name=str(row.get(last_col)).strip()
                if last_col and row.get(last_col) is not None and not pd.isna(row.get(last_col))
                else None,
                response_json=payload,
            )
            session.add(response)
            session.flush()
            result["responses_stored"] += 1

            for prompt, event_id in event_id_by_prompt.items():
                if prompt not in row.index:
                    continue
                raw = row[prompt]
                code = availability_cell_code(raw)
                raw_text = None
                if raw is not None and not pd.isna(raw):
                    raw_text = str(raw).strip() or None
                session.add(
                    InternationalResponseEventAvailability(
                        form_id=form_id,
                        response_id=int(response.id),
                        event_id=int(event_id),
                        availability_code=code,
                        raw_value=raw_text,
                    )
                )
                result["availability_rows"] += 1

        if commit:
            session.commit()

    return result
