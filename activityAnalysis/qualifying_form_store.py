"""
2027-style qualifying availability: store full form rows per official, configure
per-competition directory criteria, report using ``officials_analysis.appointments`` only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional

import pandas as pd
from sqlalchemy import and_, case, delete, func, literal, select, update
from sqlalchemy.orm import Session

try:
    from activityAnalysis.officials_analysis_models import (
        AppointmentTypes,
        Appointments,
        Assignment,
        Competition,
        Disciplines,
        Levels,
        Officials,
        QualifyingAvailabilityCompetition,
        QualifyingAvailabilityForm,
        QualifyingCompetitionCriteria,
        QualifyingOfficialCompetitionAvailability,
        QualifyingOfficialFormResponse,
    )
    from activityAnalysis.qualifying_availability_ingest import (
        competition_availability_columns,
        dedupe_form_responses_by_member,
        extract_qualifying_form_conflicts,
        extract_qualifying_form_notes,
        extract_qualifying_role_priority,
        find_completion_status_column,
        find_member_number_column,
        find_not_interested_all_qualifying_column,
        is_complete_response_status,
        is_opt_out_all_qualifying_value,
        row_opts_out_all_qualifying,
        load_original_sheet,
        normalize_member_number_value,
        normalize_qualifying_availability_cell,
        parse_qualifying_competition_prompt,
        resolve_workbook_sheet_name,
        response_json_is_complete,
        response_json_not_interested_all,
    )
except ModuleNotFoundError:
    from officials_analysis_models import (
        AppointmentTypes,
        Appointments,
        Assignment,
        Competition,
        Disciplines,
        Levels,
        Officials,
        QualifyingAvailabilityCompetition,
        QualifyingAvailabilityForm,
        QualifyingCompetitionCriteria,
        QualifyingOfficialCompetitionAvailability,
        QualifyingOfficialFormResponse,
    )
    from qualifying_availability_ingest import (
        competition_availability_columns,
        dedupe_form_responses_by_member,
        extract_qualifying_form_conflicts,
        extract_qualifying_form_notes,
        extract_qualifying_role_priority,
        find_completion_status_column,
        find_member_number_column,
        find_not_interested_all_qualifying_column,
        is_complete_response_status,
        is_opt_out_all_qualifying_value,
        row_opts_out_all_qualifying,
        load_original_sheet,
        normalize_member_number_value,
        normalize_qualifying_availability_cell,
        parse_qualifying_competition_prompt,
        resolve_workbook_sheet_name,
        response_json_is_complete,
        response_json_not_interested_all,
    )

try:
    from activityAnalysis.load_activity_data import (
        DISC_SINGLES_PAIRS_ID,
        NO_DISCIPLINE_DIRECTORY_ID,
        SINGLES_DISCIPLINE_ID,
        SINGLES_PAIRS_APPT_TYPES,
        _json_safe_qualifying_value,
        _resolve_discipline_ids,
        count_official_segment_competitions_batch,
        get_engine,
        calendar_years_for_usfs_season_codes,
        other_comps_segment_season_year_codes,
        segment_discipline_type_ids_for_directory,
    )
except ModuleNotFoundError:
    from load_activity_data import (
        DISC_SINGLES_PAIRS_ID,
        NO_DISCIPLINE_DIRECTORY_ID,
        SINGLES_DISCIPLINE_ID,
        SINGLES_PAIRS_APPT_TYPES,
        _json_safe_qualifying_value,
        _resolve_discipline_ids,
        count_official_segment_competitions_batch,
        get_engine,
        calendar_years_for_usfs_season_codes,
        other_comps_segment_season_year_codes,
        segment_discipline_type_ids_for_directory,
    )


def _row_to_response_json(row: pd.Series) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in row.index:
        if not isinstance(col, str):
            continue
        out[col] = _json_safe_qualifying_value(row[col])
    return out


# Per-competition ``raw_value`` when global opt-out (column G) — empty, not the long form text.
_OPT_OUT_AVAILABILITY_RAW: str | None = ""


def load_qualifying_form_workbook(
    path: str,
    *,
    label: str | None = None,
    sheet_name: str | None = None,
    only_complete_responses: bool = True,
    allow_missing_completion_status: bool = False,
    completion_status_column: str | None = None,
    replace_existing_label: bool = True,
    commit: bool = True,
    engine=None,
) -> dict[str, Any]:
    """
    Ingest a 2027-style workbook: one ``response_json`` per matched official,
    competitions from pipe-separated column headers, normalized availability rows.
    """
    import os

    db_engine = engine or get_engine()
    resolved_sheet = resolve_workbook_sheet_name(path, sheet_name)
    df = load_original_sheet(
        path,
        sheet_name=resolved_sheet,
        only_complete_responses=only_complete_responses,
        allow_missing_completion_status=allow_missing_completion_status,
        completion_status_column=completion_status_column,
    )
    comp_cols = competition_availability_columns(df)
    if not comp_cols:
        raise ValueError("No competition availability columns found in workbook.")

    form_label = (label or "").strip() or os.path.basename(path)
    mcol = find_member_number_column(df)
    if not mcol:
        raise ValueError("Could not find member number column.")

    rows_before_dedupe = len(df)
    df, dedupe_stats = dedupe_form_responses_by_member(df, member_column=mcol)

    result: dict[str, Any] = {
        "form_id": None,
        "form_label": form_label,
        "sheet": resolved_sheet,
        "rows_read": rows_before_dedupe,
        "rows_after_dedupe": len(df),
        "duplicate_rows_dropped": dedupe_stats["duplicate_rows_dropped"],
        "duplicate_member_numbers": dedupe_stats["duplicate_member_numbers"],
        "dedupe_by": dedupe_stats["dedupe_by"],
        "competitions": len(comp_cols),
        "responses_stored": 0,
        "availability_rows": 0,
        "skipped_empty_member": 0,
        "skipped_incomplete": 0,
        "competitions_reused": 0,
        "competitions_new": 0,
        "competitions_removed": 0,
        "availability_rows_repaired_opt_out": 0,
        "unmatched_member_numbers": [],
    }
    unmatched: set[str] = set()

    with Session(db_engine) as session:
        comp_id_by_prompt: dict[str, int] = {}
        existing_form = None
        if replace_existing_label:
            existing_form = session.scalar(
                select(QualifyingAvailabilityForm).where(
                    QualifyingAvailabilityForm.label == form_label
                )
            )

        if existing_form is not None:
            form_id = int(existing_form.id)
            result["form_id"] = form_id
            existing_form.source_filename = os.path.basename(path)
            existing_form.loaded_at = datetime.now(timezone.utc)

            session.execute(
                delete(QualifyingOfficialFormResponse).where(
                    QualifyingOfficialFormResponse.form_id == form_id
                )
            )
            session.execute(
                delete(QualifyingOfficialCompetitionAvailability).where(
                    QualifyingOfficialCompetitionAvailability.form_id == form_id
                )
            )

            existing_comps = session.scalars(
                select(QualifyingAvailabilityCompetition).where(
                    QualifyingAvailabilityCompetition.form_id == form_id
                )
            ).all()
            by_prompt = {c.prompt_key: c for c in existing_comps}
            by_title: dict[str, QualifyingAvailabilityCompetition] = {}
            for c in existing_comps:
                title_key = (c.title or "").strip()
                if title_key:
                    by_title[title_key] = c

            matched_ids: set[int] = set()
            for order, prompt in enumerate(comp_cols):
                parsed = parse_qualifying_competition_prompt(prompt)
                title = (parsed.get("title") or "").strip() or prompt

                comp = by_prompt.get(prompt)
                if comp is None:
                    comp = by_title.get(title)

                if comp is not None:
                    comp.prompt_key = prompt
                    comp.title = parsed.get("title")
                    comp.location = parsed.get("location")
                    comp.event_dates = parsed.get("dates")
                    comp.season_year = parsed.get("year")
                    comp.sort_order = order
                    cid = int(comp.id)
                    result["competitions_reused"] += 1
                else:
                    comp = QualifyingAvailabilityCompetition(
                        form_id=form_id,
                        prompt_key=prompt,
                        title=parsed.get("title"),
                        location=parsed.get("location"),
                        event_dates=parsed.get("dates"),
                        season_year=parsed.get("year"),
                        sort_order=order,
                    )
                    session.add(comp)
                    session.flush()
                    cid = int(comp.id)
                    result["competitions_new"] += 1

                comp_id_by_prompt[prompt] = cid
                matched_ids.add(cid)

            for comp in existing_comps:
                if int(comp.id) not in matched_ids:
                    session.delete(comp)
                    result["competitions_removed"] += 1
            session.flush()
        else:
            form = QualifyingAvailabilityForm(
                label=form_label,
                source_filename=os.path.basename(path),
            )
            session.add(form)
            session.flush()
            form_id = int(form.id)
            result["form_id"] = form_id

            for order, prompt in enumerate(comp_cols):
                parsed = parse_qualifying_competition_prompt(prompt)
                comp = QualifyingAvailabilityCompetition(
                    form_id=form_id,
                    prompt_key=prompt,
                    title=parsed.get("title"),
                    location=parsed.get("location"),
                    event_dates=parsed.get("dates"),
                    season_year=parsed.get("year"),
                    sort_order=order,
                )
                session.add(comp)
                session.flush()
                comp_id_by_prompt[prompt] = int(comp.id)
                result["competitions_new"] += 1

        mbr_to_id: dict[str, int] = {}
        for oid, mbr in session.execute(
            select(Officials.id, Officials.mbr_number)
        ).all():
            key = normalize_member_number_value(mbr)
            if key:
                mbr_to_id[key] = int(oid)

        status_col = find_completion_status_column(df)

        for _, row in df.iterrows():
            mbr = normalize_member_number_value(row[mcol])
            if not mbr:
                result["skipped_empty_member"] += 1
                continue
            if status_col is not None and not is_complete_response_status(
                row.get(status_col)
            ):
                result["skipped_incomplete"] += 1
                continue
            oid = mbr_to_id.get(mbr)
            if oid is None:
                unmatched.add(mbr)
                continue

            not_interested_all = row_opts_out_all_qualifying(row)
            payload = _row_to_response_json(row)
            payload["_not_interested_all_qualifying"] = not_interested_all

            session.add(
                QualifyingOfficialFormResponse(
                    form_id=form_id,
                    official_id=oid,
                    member_number=mbr,
                    response_json=payload,
                )
            )
            result["responses_stored"] += 1

            for prompt, cid in comp_id_by_prompt.items():
                if prompt not in row.index:
                    continue
                if not_interested_all:
                    code = "not_available"
                    raw_text = _OPT_OUT_AVAILABILITY_RAW
                else:
                    raw = row[prompt]
                    code = normalize_qualifying_availability_cell(raw)
                    raw_text = None
                    if not pd.isna(raw):
                        raw_text = str(raw).strip() or None
                session.add(
                    QualifyingOfficialCompetitionAvailability(
                        form_id=form_id,
                        official_id=oid,
                        competition_id=cid,
                        availability_code=code,
                        raw_value=raw_text,
                    )
                )
                result["availability_rows"] += 1

        result["availability_rows_repaired_opt_out"] = _repair_opt_out_availability_rows(
            session, form_id
        )
        result["unmatched_member_numbers"] = sorted(unmatched)
        if commit:
            session.commit()

    return result


def _repair_opt_out_availability_rows(session: Session, form_id: int) -> int:
    """
    After load, set ``not_available`` on all competition rows for officials who
    opted out globally (column G) but were stored as ``no_response``.
    """
    fixed = 0
    rows = session.execute(
        select(
            QualifyingOfficialFormResponse.official_id,
            QualifyingOfficialFormResponse.response_json,
        ).where(QualifyingOfficialFormResponse.form_id == int(form_id))
    ).all()
    for oid, response_json in rows:
        payload = (
            response_json
            if isinstance(response_json, dict)
            else (dict(response_json) if response_json else {})
        )
        if not response_json_not_interested_all(payload):
            continue
        upd = session.execute(
            update(QualifyingOfficialCompetitionAvailability)
            .where(
                QualifyingOfficialCompetitionAvailability.form_id == int(form_id),
                QualifyingOfficialCompetitionAvailability.official_id == int(oid),
                QualifyingOfficialCompetitionAvailability.availability_code
                != "not_available",
            )
            .values(
                availability_code="not_available",
                raw_value=_OPT_OUT_AVAILABILITY_RAW,
            )
        )
        fixed += int(upd.rowcount or 0)
        if not payload.get("_not_interested_all_qualifying"):
            payload["_not_interested_all_qualifying"] = True
            session.execute(
                update(QualifyingOfficialFormResponse)
                .where(
                    QualifyingOfficialFormResponse.form_id == int(form_id),
                    QualifyingOfficialFormResponse.official_id == int(oid),
                )
                .values(response_json=payload)
            )
    return fixed


def list_qualifying_forms(engine=None) -> pd.DataFrame:
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        rows = session.execute(
            select(
                QualifyingAvailabilityForm.id,
                QualifyingAvailabilityForm.label,
                QualifyingAvailabilityForm.source_filename,
                QualifyingAvailabilityForm.loaded_at,
            ).order_by(QualifyingAvailabilityForm.loaded_at.desc())
        ).all()
    return pd.DataFrame(
        rows,
        columns=["form_id", "label", "source_filename", "loaded_at"],
    )


def list_form_competitions(form_id: int, engine=None) -> pd.DataFrame:
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        rows = session.execute(
            select(
                QualifyingAvailabilityCompetition.id,
                QualifyingAvailabilityCompetition.prompt_key,
                QualifyingAvailabilityCompetition.title,
                QualifyingAvailabilityCompetition.location,
                QualifyingAvailabilityCompetition.event_dates,
                QualifyingAvailabilityCompetition.season_year,
                QualifyingAvailabilityCompetition.competition_group,
            )
            .where(QualifyingAvailabilityCompetition.form_id == int(form_id))
            .order_by(QualifyingAvailabilityCompetition.sort_order)
        ).all()
    return pd.DataFrame(
        rows,
        columns=[
            "competition_id",
            "prompt_key",
            "title",
            "location",
            "event_dates",
            "season_year",
            "competition_group",
        ],
    )


def save_competition_group(
    competition_id: int,
    competition_group: str | None,
    engine=None,
) -> None:
    """Persist S/P/D vs Synchronized for assignment-history columns on the report."""
    raw = (competition_group or "").strip().lower()
    value = raw if raw in _CHAMPIONSHIP_TYPE_IDS_BY_GROUP else None
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        comp = session.get(QualifyingAvailabilityCompetition, int(competition_id))
        if comp is None:
            raise ValueError(f"Unknown competition id {competition_id}")
        comp.competition_group = value
        session.commit()


def _normalize_competition_group(competition_group: str | None) -> str | None:
    raw = (competition_group or "").strip().lower()
    return raw if raw in _CHAMPIONSHIP_TYPE_IDS_BY_GROUP else None


def _appointment_name_to_id_map(session: Session) -> dict[str, int]:
    rows = session.execute(select(AppointmentTypes.id, AppointmentTypes.name)).all()
    return {str(n or "").strip().casefold(): int(i) for i, n in rows}


def _batch_held_appointment_type_ids(
    session: Session,
    official_ids: list[int],
) -> dict[int, set[int]]:
    """Active appointment type ids per official (one query)."""
    out: dict[int, set[int]] = {int(oid): set() for oid in official_ids}
    if not official_ids:
        return out
    rows = session.execute(
        select(Appointments.official_id, Appointments.appointment_type_id)
        .where(
            Appointments.official_id.in_(list(official_ids)),
            Appointments.active.is_(True),
            Appointments.appointment_type_id.isnot(None),
        )
        .distinct()
    ).all()
    for oid, atid in rows:
        out[int(oid)].add(int(atid))
    return out


def _merge_held_types_cache(
    cache: dict[int, set[int]],
    official_ids: list[int],
    fetched: dict[int, set[int]],
) -> None:
    for oid in official_ids:
        if oid not in cache:
            cache[oid] = set(fetched.get(oid, set()))


def _fetch_assignment_years_combined(
    session: Session,
    official_ids: list[int],
    *,
    competition_group: str | None,
    in_role_appointment_type_id: int | None = None,
    in_role_discipline_id: int | None = None,
) -> tuple[
    dict[int, tuple[int | None, int | None]],
    dict[int, tuple[int | None, int | None]],
]:
    """
    One grouped query: per official, max championship/sectional year (overall and in-role).

    Overall uses any assignment in the competition group; in-role applies appointment
    type / discipline filters when those ids are set.
    """
    overall: dict[int, tuple[int | None, int | None]] = {
        int(oid): (None, None) for oid in official_ids
    }
    in_role: dict[int, tuple[int | None, int | None]] = {
        int(oid): (None, None) for oid in official_ids
    }
    if not official_ids:
        return overall, in_role
    grp = _normalize_competition_group(competition_group)
    if grp is None:
        return overall, in_role

    champ_ids = tuple(_CHAMPIONSHIP_TYPE_IDS_BY_GROUP.get(grp) or ())
    sect_ids = tuple(_SECTIONAL_TYPE_IDS_BY_GROUP.get(grp) or ())
    if not champ_ids and not sect_ids:
        return overall, in_role

    in_role_clause = literal(True)
    if in_role_appointment_type_id is not None:
        parts = [Assignment.appointment_type_id == int(in_role_appointment_type_id)]
        if in_role_discipline_id is not None:
            disc_ids = _assignment_discipline_ids_for_report(
                int(in_role_discipline_id), int(in_role_appointment_type_id)
            )
            if disc_ids is not None:
                parts.append(Assignment.discipline_id.in_(disc_ids))
        in_role_clause = and_(*parts)

    def _max_year(ct_ids: tuple[int, ...], extra) -> Any:
        if not ct_ids:
            return literal(None)
        cond: Any = Competition.competition_type_id.in_(ct_ids)
        if extra is not True:
            cond = and_(cond, extra)
        return func.max(case((cond, Competition.year)))

    cols: list[Any] = [Assignment.official_id]
    for suffix, ct_ids in (("champ", champ_ids), ("sect", sect_ids)):
        if not ct_ids:
            continue
        cols.append(_max_year(ct_ids, True).label(f"{suffix}_overall"))
        if in_role_appointment_type_id is not None:
            cols.append(_max_year(ct_ids, in_role_clause).label(f"{suffix}_in_role"))

    stmt = (
        select(*cols)
        .select_from(Assignment)
        .join(Competition, Assignment.competition_id == Competition.id)
        .where(
            Assignment.official_id.in_(list(official_ids)),
            Competition.competition_type_id.in_(tuple(set(champ_ids) | set(sect_ids))),
        )
        .group_by(Assignment.official_id)
    )
    for row in session.execute(stmt).mappings().all():
        oid = int(row["official_id"])
        if champ_ids:
            yr = row.get("champ_overall")
            if yr is not None:
                overall[oid] = (int(yr), overall[oid][1])
            if in_role_appointment_type_id is not None:
                ir = row.get("champ_in_role")
                if ir is not None:
                    in_role[oid] = (int(ir), in_role[oid][1])
        if sect_ids:
            yr = row.get("sect_overall")
            if yr is not None:
                overall[oid] = (overall[oid][0], int(yr))
            if in_role_appointment_type_id is not None:
                ir = row.get("sect_in_role")
                if ir is not None:
                    in_role[oid] = (in_role[oid][0], int(ir))
    return overall, in_role


def _load_competition_criteria_triples(
    session: Session,
    competition_id: int,
) -> list[tuple[int, int, int | None]]:
    rows = session.execute(
        select(
            QualifyingCompetitionCriteria.appointment_type_id,
            QualifyingCompetitionCriteria.discipline_id,
            QualifyingCompetitionCriteria.level_id,
        ).where(QualifyingCompetitionCriteria.competition_id == int(competition_id))
    ).all()
    out: list[tuple[int, int, int | None]] = []
    for atid, did, lid in rows:
        out.append((int(atid), int(did), int(lid) if lid is not None else None))
    return out


def get_directory_filter_options(engine=None) -> dict[str, list[tuple[int, str]]]:
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        appt = session.execute(
            select(AppointmentTypes.id, AppointmentTypes.name).order_by(
                AppointmentTypes.name
            )
        ).all()
        disc = session.execute(
            select(Disciplines.id, Disciplines.name).order_by(Disciplines.name)
        ).all()
        levels = session.execute(
            select(Levels.id, Levels.name).order_by(Levels.name)
        ).all()
    return {
        "appointment_types": [(int(i), str(n or "")) for i, n in appt],
        "disciplines": [(int(i), str(n or "")) for i, n in disc],
        "levels": [(int(i), str(n or "")) for i, n in levels],
    }


def get_active_directory_appointment_combinations(engine=None) -> pd.DataFrame:
    """
    Distinct active ``appointments`` triples (type × discipline × level) in the directory.
    Used to suggest only real combinations when configuring criteria.
    """
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        rows = session.execute(
            select(
                Appointments.appointment_type_id,
                AppointmentTypes.name,
                Appointments.discipline_id,
                Disciplines.name,
                Appointments.level_id,
                Levels.name,
            )
            .select_from(Appointments)
            .join(
                AppointmentTypes,
                AppointmentTypes.id == Appointments.appointment_type_id,
            )
            .join(Disciplines, Disciplines.id == Appointments.discipline_id)
            .outerjoin(Levels, Levels.id == Appointments.level_id)
            .where(Appointments.active.is_(True))
            .distinct()
            .order_by(
                AppointmentTypes.name,
                Disciplines.name,
                Levels.name.asc().nulls_last(),
            )
        ).all()
    return pd.DataFrame(
        rows,
        columns=[
            "appointment_type_id",
            "appointment_type",
            "discipline_id",
            "discipline",
            "level_id",
            "level",
        ],
    )


def delete_competition_criterion(criteria_id: int, *, engine=None, commit: bool = True) -> bool:
    """Remove one criteria row by primary key."""
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        result = session.execute(
            delete(QualifyingCompetitionCriteria).where(
                QualifyingCompetitionCriteria.id == int(criteria_id)
            )
        )
        if commit:
            session.commit()
    return bool(result.rowcount)


def get_competition_criteria(competition_id: int, engine=None) -> pd.DataFrame:
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        rows = session.execute(
            select(
                QualifyingCompetitionCriteria.id,
                QualifyingCompetitionCriteria.appointment_type_id,
                AppointmentTypes.name,
                QualifyingCompetitionCriteria.discipline_id,
                Disciplines.name,
                QualifyingCompetitionCriteria.level_id,
                Levels.name,
            )
            .select_from(QualifyingCompetitionCriteria)
            .join(
                AppointmentTypes,
                AppointmentTypes.id == QualifyingCompetitionCriteria.appointment_type_id,
            )
            .join(
                Disciplines,
                Disciplines.id == QualifyingCompetitionCriteria.discipline_id,
            )
            .outerjoin(Levels, Levels.id == QualifyingCompetitionCriteria.level_id)
            .where(QualifyingCompetitionCriteria.competition_id == int(competition_id))
            .order_by(AppointmentTypes.name, Disciplines.name, Levels.name)
        ).all()
    return pd.DataFrame(
        rows,
        columns=[
            "criteria_id",
            "appointment_type_id",
            "appointment_type",
            "discipline_id",
            "discipline",
            "level_id",
            "level",
        ],
    )


def save_competition_criteria(
    competition_id: int,
    criteria_rows: list[tuple[int, int, int | None]],
    *,
    engine=None,
    commit: bool = True,
) -> int:
    """
    Replace criteria for ``competition_id``. Each row is
    ``(appointment_type_id, discipline_id, level_id or None)``.
    """
    db_engine = engine or get_engine()
    cid = int(competition_id)
    with Session(db_engine) as session:
        session.execute(
            delete(QualifyingCompetitionCriteria).where(
                QualifyingCompetitionCriteria.competition_id == cid
            )
        )
        for atid, did, lid in criteria_rows:
            session.add(
                QualifyingCompetitionCriteria(
                    competition_id=cid,
                    appointment_type_id=int(atid),
                    discipline_id=int(did),
                    level_id=int(lid) if lid is not None else None,
                )
            )
        if commit:
            session.commit()
    return len(criteria_rows)


def _availability_bucket(
    *,
    has_form_response: bool,
    form_is_complete: bool,
    not_interested_all: bool,
    availability_code: str | None,
) -> str | None:
    """
    Classify an official for one competition.

    Returns ``available``, ``unavailable``, ``no_reply``, or None (excluded, e.g. N/A).
    """
    if not has_form_response or not form_is_complete:
        return "no_reply"
    if not_interested_all:
        return "unavailable"
    code = (availability_code or "").strip().lower() or "no_response"
    if code == "available":
        return "available"
    if code == "not_available":
        return "unavailable"
    if code in ("no_response", ""):
        return "no_reply"
    return None


def _appointment_display_line(
    appointment_type_name: object,
    discipline_name: object,
    level_name: object,
    *,
    discipline_id: int | None = None,
) -> str:
    label = str(appointment_type_name or "").strip()
    ds = str(discipline_name or "").strip()
    if discipline_id is not None:
        try:
            if int(discipline_id) == int(NO_DISCIPLINE_DIRECTORY_ID):
                ds = ""
        except (TypeError, ValueError):
            pass
    elif ds.casefold() == "no discipline":
        ds = ""
    if ds:
        label = f"{label} ({ds})" if label else ds
    ls = str(level_name or "").strip()
    if ls:
        label = f"{label} — {ls}" if label else ls
    return label


def _batch_official_active_appointment_lines(
    session: Session,
    official_ids: list[int],
    appointment_type_id: int,
    discipline_id: int,
    level_id: int | None,
) -> dict[int, list[str]]:
    """Active directory appointment display lines per official (one query)."""
    out: dict[int, list[str]] = {int(oid): [] for oid in official_ids}
    if not official_ids:
        return out
    appt_filters = [
        Appointments.official_id.in_(list(official_ids)),
        Appointments.active.is_(True),
        Appointments.appointment_type_id == int(appointment_type_id),
    ]
    disc_ids = _assignment_discipline_ids_for_report(
        int(discipline_id), int(appointment_type_id)
    )
    if disc_ids is not None:
        appt_filters.append(Appointments.discipline_id.in_(disc_ids))
    stmt = (
        select(
            Appointments.official_id,
            AppointmentTypes.name,
            Disciplines.name,
            Appointments.discipline_id,
            Levels.name,
        )
        .select_from(Appointments)
        .join(AppointmentTypes, Appointments.appointment_type_id == AppointmentTypes.id)
        .join(Disciplines, Appointments.discipline_id == Disciplines.id)
        .outerjoin(Levels, Appointments.level_id == Levels.id)
        .where(*appt_filters)
    )
    if level_id is not None:
        stmt = stmt.where(Appointments.level_id == int(level_id))
    for oid, at, disc, disc_id, lvl in session.execute(stmt).all():
        line = _appointment_display_line(at, disc, lvl, discipline_id=disc_id)
        if line:
            out[int(oid)].append(line)
    return out


def _batch_official_in_role_appointment_year(
    session: Session,
    official_ids: list[int],
    appointment_type_id: int,
    discipline_id: int,
) -> dict[int, int | None]:
    """
    Calendar year of the most recent active directory appointment for this role/discipline.

    Uses ``achieved_date`` when set, otherwise ``appointed_date``.
    """
    out: dict[int, int | None] = {int(oid): None for oid in official_ids}
    if not official_ids:
        return out
    appt_filters = [
        Appointments.official_id.in_(list(official_ids)),
        Appointments.active.is_(True),
        Appointments.appointment_type_id == int(appointment_type_id),
    ]
    disc_ids = _assignment_discipline_ids_for_report(
        int(discipline_id), int(appointment_type_id)
    )
    if disc_ids is not None:
        appt_filters.append(Appointments.discipline_id.in_(disc_ids))
    appt_date = func.coalesce(Appointments.achieved_date, Appointments.appointed_date)
    stmt = (
        select(
            Appointments.official_id,
            func.max(appt_date).label("appointment_date"),
        )
        .where(*appt_filters, appt_date.isnot(None))
        .group_by(Appointments.official_id)
    )
    for oid, appt_dt in session.execute(stmt).all():
        if appt_dt is None:
            continue
        if hasattr(appt_dt, "year"):
            out[int(oid)] = int(appt_dt.year)
        else:
            try:
                out[int(oid)] = int(pd.Timestamp(appt_dt).year)
            except (TypeError, ValueError):
                pass
    return out


def _appointment_matches_competition_criterion(
    appointment_type_id: int,
    discipline_id: int | None,
    level_id: int | None,
    *,
    criterion_at: int,
    criterion_disc: int,
    criterion_level: int | None,
) -> bool:
    """True when an active appointment row matches one configured competition criterion."""
    if int(appointment_type_id) != int(criterion_at):
        return False
    disc_ids = _assignment_discipline_ids_for_report(
        int(criterion_disc), int(criterion_at)
    )
    if disc_ids is not None:
        if discipline_id is None:
            return False
        try:
            if int(discipline_id) not in disc_ids:
                return False
        except (TypeError, ValueError):
            return False
    if criterion_level is not None:
        if level_id is None:
            return False
        try:
            if int(level_id) != int(criterion_level):
                return False
        except (TypeError, ValueError):
            return False
    return True


def _batch_competition_directory_appointment_lines(
    session: Session,
    official_ids: list[int],
    criteria_rows: list[tuple[int, int, int | None]],
) -> dict[int, list[str]]:
    """
    Active directory appointment labels per official for this competition.

    Unions appointments matching **any** configured criterion row on the competition
    (not the report UI filters).
    """
    out: dict[int, list[str]] = {int(oid): [] for oid in official_ids}
    if not official_ids:
        return out
    stmt = (
        select(
            Appointments.official_id,
            AppointmentTypes.name,
            Disciplines.name,
            Appointments.discipline_id,
            Levels.name,
            Appointments.appointment_type_id,
            Appointments.level_id,
        )
        .select_from(Appointments)
        .join(AppointmentTypes, Appointments.appointment_type_id == AppointmentTypes.id)
        .join(Disciplines, Appointments.discipline_id == Disciplines.id)
        .outerjoin(Levels, Appointments.level_id == Levels.id)
        .where(
            Appointments.official_id.in_(list(official_ids)),
            Appointments.active.is_(True),
        )
    )
    for (
        oid,
        at_name,
        disc_name,
        disc_id,
        lvl_name,
        at_id,
        lvl_id,
    ) in session.execute(stmt).all():
        oid = int(oid)
        if not criteria_rows:
            line = _appointment_display_line(
                at_name, disc_name, lvl_name, discipline_id=disc_id
            )
            if line and line not in out[oid]:
                out[oid].append(line)
            continue
        for crit_at, crit_disc, crit_lvl in criteria_rows:
            if not _appointment_matches_competition_criterion(
                int(at_id),
                disc_id,
                lvl_id,
                criterion_at=crit_at,
                criterion_disc=crit_disc,
                criterion_level=crit_lvl,
            ):
                continue
            line = _appointment_display_line(
                at_name, disc_name, lvl_name, discipline_id=disc_id
            )
            if line and line not in out[oid]:
                out[oid].append(line)
    for oid in official_ids:
        out[int(oid)] = sorted(out[int(oid)], key=str.casefold)
    return out


def _criteria_allow_filter(
    criteria_rows: list[tuple[int, int, int | None]],
    appointment_type_id: int,
    discipline_id: int,
    level_id: int | None,
) -> bool:
    """True if competition has no criteria yet, or filter matches a criteria row."""
    if not criteria_rows:
        return True
    for atid, did, lid in criteria_rows:
        if int(atid) != int(appointment_type_id):
            continue
        if int(did) != int(discipline_id):
            continue
        if lid is None:
            return True
        if level_id is not None and int(lid) == int(level_id):
            return True
    return False


_BUCKET_LABELS = {
    "available": "Available",
    "unavailable": "Unavailable",
    "no_reply": "Didn't reply",
}

# Keep in sync with app_query_params.QUALIFYING_* (do not import — circular load risk).
QUALIFYING_ALL_LABEL = "(All)"
QUALIFYING_ANY_LEVEL_LABEL = "(Any level)"

# Stored on qualifying_availability_competition.competition_group (migration 009).
QUALIFYING_COMPETITION_GROUP_SPD = "spd"
QUALIFYING_COMPETITION_GROUP_SYNCHRO = "synchronized"
QUALIFYING_COMPETITION_GROUP_OPTIONS: dict[str, str] = {
    "": "(not set)",
    QUALIFYING_COMPETITION_GROUP_SPD: "S/P/D (Singles, Pairs, Dance)",
    QUALIFYING_COMPETITION_GROUP_SYNCHRO: "Synchronized",
}
# Same competition_type ids as activity_tracker_app.SUMMARY_COMPETITION_TYPES.
_CHAMPIONSHIP_TYPE_IDS_BY_GROUP: dict[str, tuple[int, ...]] = {
    QUALIFYING_COMPETITION_GROUP_SPD: (4,),
    QUALIFYING_COMPETITION_GROUP_SYNCHRO: (8,),
}
_SECTIONAL_TYPE_IDS_BY_GROUP: dict[str, tuple[int, ...]] = {
    QUALIFYING_COMPETITION_GROUP_SPD: (1, 2, 3),
    QUALIFYING_COMPETITION_GROUP_SYNCHRO: (5, 6, 7, 9),
}

QUALIFYING_REPORT_COLUMN_ORDER: tuple[str, ...] = (
    "Name",
    "Member #",
    "Region",
    "Email",
    "Status",
    "Appointment year",
    "Last champs (in role)",
    "Last sectionals (in role)",
    "Last champs (overall)",
    "Last sectionals (overall)",
    "Total comps (2 yr)",
    "Total comps (2 yr, in role)",
    "Directory appointments",
    "Notes",
    "Conflicts",
    "Role priority",
    "official_id",
)


def person_assignments_report_query(official_id: int) -> str:
    """Deep link to activity tracker per-person assignments for this official."""
    return f"?report=person&official={int(official_id)}"


def _assignment_discipline_ids_for_report(
    discipline_id: int,
    appointment_type_id: int,
) -> list[int] | None:
    """
    Match championships matrix discipline resolution, including Singles (1) rows
    stored under Singles/Pairs (9) for judge/referee-style roles.
    """
    ids = _resolve_discipline_ids(int(discipline_id), int(appointment_type_id))
    if ids is None:
        return None
    out = list(ids)
    if (
        int(discipline_id) == SINGLES_DISCIPLINE_ID
        and DISC_SINGLES_PAIRS_ID not in out
    ):
        out.append(DISC_SINGLES_PAIRS_ID)
    return out


@dataclass
class _CriterionReportSegment:
    appointment_type_id: int
    discipline_id: int
    level_id: int | None
    pending: list[tuple[Any, str, dict[str, Any], bool]]


def resolve_report_criteria_filters(
    crit_combos: pd.DataFrame,
    filter_at: str,
    filter_disc: str,
    filter_lvl: str,
) -> list[tuple[int, int, int | None]]:
    """
    Map report dropdowns (including ``(All)``) to criterion rows for this competition.
    """
    if crit_combos.empty:
        return []
    df = crit_combos
    if filter_at != QUALIFYING_ALL_LABEL:
        df = df.loc[df["appointment_type"] == filter_at]
    if filter_disc != QUALIFYING_ALL_LABEL:
        df = df.loc[df["discipline"] == filter_disc]
    if filter_lvl == QUALIFYING_ALL_LABEL:
        pass
    elif filter_lvl == QUALIFYING_ANY_LEVEL_LABEL:
        df = df.loc[df["level_id"].isna()]
    else:
        df = df.loc[df["level"] == filter_lvl]
    out: list[tuple[int, int, int | None]] = []
    for _, r in df.iterrows():
        lid = r["level_id"]
        if pd.isna(lid) or lid is None:
            lid = None
        else:
            lid = int(lid)
        out.append((int(r["appointment_type_id"]), int(r["discipline_id"]), lid))
    return out


def _collect_pending_for_criterion(
    session: Session,
    *,
    form_id: int,
    competition_id: int,
    appointment_type_id: int,
    discipline_id: int,
    level_id: int | None,
    include_available: bool,
    include_no_reply: bool,
    include_unavailable: bool,
) -> _CriterionReportSegment | None:
    appt_filters = [
        Appointments.active.is_(True),
        Appointments.appointment_type_id == int(appointment_type_id),
    ]
    disc_ids = _assignment_discipline_ids_for_report(
        int(discipline_id), int(appointment_type_id)
    )
    if disc_ids is not None:
        appt_filters.append(Appointments.discipline_id.in_(disc_ids))
    if level_id is not None:
        appt_filters.append(Appointments.level_id == int(level_id))

    stmt = (
        select(
            Officials.id,
            Officials.full_name,
            Officials.mbr_number,
            Officials.region,
            Officials.state,
            Officials.email,
            QualifyingOfficialCompetitionAvailability.availability_code,
            QualifyingOfficialCompetitionAvailability.raw_value,
            QualifyingOfficialFormResponse.id.label("form_response_id"),
            QualifyingOfficialFormResponse.response_json,
        )
        .select_from(Officials)
        .join(
            Appointments,
            and_(Appointments.official_id == Officials.id, *appt_filters),
        )
        .outerjoin(
            QualifyingOfficialFormResponse,
            and_(
                QualifyingOfficialFormResponse.official_id == Officials.id,
                QualifyingOfficialFormResponse.form_id == int(form_id),
            ),
        )
        .outerjoin(
            QualifyingOfficialCompetitionAvailability,
            and_(
                QualifyingOfficialCompetitionAvailability.official_id == Officials.id,
                QualifyingOfficialCompetitionAvailability.form_id == int(form_id),
                QualifyingOfficialCompetitionAvailability.competition_id
                == int(competition_id),
            ),
        )
        .distinct()
        .order_by(Officials.full_name.asc().nulls_last())
    )
    executed = session.execute(stmt).all()

    pending: list[tuple[Any, str, dict[str, Any], bool]] = []
    seen: set[int] = set()

    for row in executed:
        oid = int(row.id)
        if oid in seen:
            continue
        seen.add(oid)

        response_json = row.response_json if row.form_response_id is not None else {}
        payload = (
            response_json
            if isinstance(response_json, dict)
            else (dict(response_json) if response_json else {})
        )
        form_is_complete = (
            row.form_response_id is not None and response_json_is_complete(payload)
        )
        not_interested_all = (
            form_is_complete and response_json_not_interested_all(payload)
        )
        bucket = _availability_bucket(
            has_form_response=row.form_response_id is not None,
            form_is_complete=form_is_complete,
            not_interested_all=not_interested_all,
            availability_code=row.availability_code,
        )
        if bucket is None:
            continue
        if bucket == "available" and not include_available:
            continue
        if bucket == "no_reply" and not include_no_reply:
            continue
        if bucket == "unavailable" and not include_unavailable:
            continue
        pending.append((row, bucket, payload, form_is_complete))

    if not pending:
        return None
    return _CriterionReportSegment(
        appointment_type_id=int(appointment_type_id),
        discipline_id=int(discipline_id),
        level_id=level_id,
        pending=pending,
    )


def _rows_from_criterion_segment(
    segment: _CriterionReportSegment,
    *,
    overall_years: dict[int, tuple[int | None, int | None]],
    in_role_years: dict[int, tuple[int | None, int | None]],
    other_comp_cache: dict[int, int],
    in_role_other_comp_cache: dict[int, int] | None,
    in_role_appointment_year_cache: dict[int, int | None] | None,
    held_types_cache: dict[int, set[int]],
    competition_appt_lines_by_official: dict[int, list[str]],
    appointment_name_to_id: dict[str, int],
) -> list[dict[str, Any]]:
    rows_out: list[dict[str, Any]] = []
    for row, bucket, payload, form_is_complete in segment.pending:
        oid = int(row.id)

        notes = ""
        conflicts = ""
        role_priority = ""
        if row.form_response_id is not None and form_is_complete:
            notes = extract_qualifying_form_notes(payload)
            conflicts = extract_qualifying_form_conflicts(payload)
            role_priority = extract_qualifying_role_priority(
                payload,
                held_appointment_type_ids=held_types_cache.get(oid, set()),
                appointment_name_to_id=appointment_name_to_id,
            )

        ir_champ, ir_sect = in_role_years.get(oid, (None, None))
        ov_champ, ov_sect = overall_years.get(oid, (None, None))
        other_count = other_comp_cache.get(oid, 0)

        row_out: dict[str, Any] = {
            "_official_id": oid,
            "official_id": oid,
            "Name": (row.full_name or "").strip() or f"Official {oid}",
            "Member #": (row.mbr_number or "").strip(),
            "Region": (row.region or row.state or "").strip(),
            "Email": (row.email or "").strip(),
            "Status": _BUCKET_LABELS.get(bucket, bucket),
            "Role priority": role_priority,
            "Last champs (in role)": ir_champ,
            "Last sectionals (in role)": ir_sect,
            "Last champs (overall)": ov_champ,
            "Last sectionals (overall)": ov_sect,
            "Total comps (2 yr)": other_count,
            "Directory appointments": "; ".join(
                competition_appt_lines_by_official.get(oid, [])
            ),
            "Notes": notes,
            "Conflicts": conflicts,
        }
        if in_role_appointment_year_cache is not None:
            row_out["Appointment year"] = in_role_appointment_year_cache.get(oid)
        if in_role_other_comp_cache is not None:
            row_out["Total comps (2 yr, in role)"] = in_role_other_comp_cache.get(
                oid, 0
            )
        rows_out.append(row_out)
    return rows_out


def _merge_year_cells(a: Any, b: Any) -> Any:
    vals = []
    for v in (a, b):
        if v is None or v == "":
            continue
        try:
            vals.append(int(v))
        except (TypeError, ValueError):
            continue
    return max(vals) if vals else None


def _merge_notes_cells(a: Any, b: Any) -> str:
    parts: list[str] = []
    for text in (str(a or "").strip(), str(b or "").strip()):
        if text and text not in parts:
            parts.append(text)
    return "\n\n".join(parts)


def _merge_report_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per official; union directory appointment labels and merge notes/years."""
    by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        oid = int(row.pop("_official_id"))
        if oid not in by_id:
            by_id[oid] = row
            continue
        existing = by_id[oid]
        parts = {
            p.strip()
            for p in (
                str(existing.get("Directory appointments") or "").split(";")
                + str(row.get("Directory appointments") or "").split(";")
            )
            if p.strip()
        }
        existing["Directory appointments"] = "; ".join(sorted(parts, key=str.casefold))
        if not str(existing.get("Role priority") or "").strip():
            existing["Role priority"] = row.get("Role priority") or ""
        existing["Notes"] = _merge_notes_cells(
            existing.get("Notes"), row.get("Notes")
        )
        existing["Conflicts"] = _merge_notes_cells(
            existing.get("Conflicts"), row.get("Conflicts")
        )
        for col in (
            "Appointment year",
            "Last champs (in role)",
            "Last sectionals (in role)",
            "Last champs (overall)",
            "Last sectionals (overall)",
        ):
            existing[col] = _merge_year_cells(existing.get(col), row.get(col))
        existing["Total comps (2 yr)"] = _merge_year_cells(
            existing.get("Total comps (2 yr)"), row.get("Total comps (2 yr)")
        )
        if "Total comps (2 yr, in role)" in row:
            existing["Total comps (2 yr, in role)"] = _merge_year_cells(
                existing.get("Total comps (2 yr, in role)"),
                row.get("Total comps (2 yr, in role)"),
            )
    return list(by_id.values())


def _order_report_columns(df: pd.DataFrame) -> pd.DataFrame:
    ordered = [c for c in QUALIFYING_REPORT_COLUMN_ORDER if c in df.columns]
    extra = [c for c in df.columns if c not in ordered]
    return df[ordered + extra]


def build_qualifying_availability_report(
    form_id: int,
    competition_id: int,
    *,
    criteria_filters: list[tuple[int, int, int | None]],
    in_role_appointment_type_id: int | None = None,
    in_role_discipline_id: int | None = None,
    include_available: bool = True,
    include_no_reply: bool = False,
    include_unavailable: bool = False,
    engine=None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Officials with matching **active directory** appointments for one form competition.

    ``criteria_filters`` is one or more ``(appointment_type_id, discipline_id, level_id)``
    rows (from configured criteria, including when UI selects ``(All)``).
    """
    db_engine = engine or get_engine()
    meta: dict[str, Any] = {
        "criteria_configured": False,
        "criteria_match": True,
        "criteria_count": len(criteria_filters),
    }

    if not criteria_filters:
        meta["criteria_match"] = False
        return pd.DataFrame(), meta

    if not (include_available or include_no_reply or include_unavailable):
        meta["no_buckets_selected"] = True
        return pd.DataFrame(), meta

    all_rows: list[dict[str, Any]] = []

    with Session(db_engine) as session:
        comp = session.get(QualifyingAvailabilityCompetition, int(competition_id))
        if comp is None or int(comp.form_id) != int(form_id):
            return pd.DataFrame(), meta
        meta["competition_title"] = comp.title
        meta["prompt_key"] = comp.prompt_key
        meta["competition_group"] = comp.competition_group

        n_crit = session.scalar(
            select(QualifyingCompetitionCriteria.id)
            .where(QualifyingCompetitionCriteria.competition_id == int(competition_id))
            .limit(1)
        )
        meta["criteria_configured"] = n_crit is not None

        meta["other_comp_season_codes"] = other_comps_segment_season_year_codes()
        appt_name_to_id = _appointment_name_to_id_map(session)
        criteria_rows = _load_competition_criteria_triples(session, int(competition_id))

        segments: list[_CriterionReportSegment] = []
        for at_id, disc_id, lid in criteria_filters:
            if not _criteria_allow_filter(criteria_rows, at_id, disc_id, lid):
                meta["criteria_match"] = False
                return pd.DataFrame(), meta
            seg = _collect_pending_for_criterion(
                session,
                form_id=int(form_id),
                competition_id=int(competition_id),
                appointment_type_id=at_id,
                discipline_id=disc_id,
                level_id=lid,
                include_available=include_available,
                include_no_reply=include_no_reply,
                include_unavailable=include_unavailable,
            )
            if seg is not None:
                segments.append(seg)

        if not segments:
            return pd.DataFrame(), meta

        report_oids: list[int] = []
        seen_oid: set[int] = set()
        for seg in segments:
            for row, _, _, _ in seg.pending:
                oid = int(row.id)
                if oid not in seen_oid:
                    seen_oid.add(oid)
                    report_oids.append(oid)

        overall_years, in_role_years = _fetch_assignment_years_combined(
            session,
            report_oids,
            competition_group=comp.competition_group,
            in_role_appointment_type_id=in_role_appointment_type_id,
            in_role_discipline_id=in_role_discipline_id,
        )
        season_codes = other_comps_segment_season_year_codes()
        meta["other_comp_calendar_years"] = calendar_years_for_usfs_season_codes(
            season_codes
        )
        other_comp_cache = count_official_segment_competitions_batch(
            report_oids,
            season_year_codes=season_codes,
        )
        in_role_other_comp_cache: dict[int, int] | None = None
        in_role_appointment_year_cache: dict[int, int | None] | None = None
        if (
            in_role_appointment_type_id is not None
            and in_role_discipline_id is not None
        ):
            in_role_appointment_year_cache = _batch_official_in_role_appointment_year(
                session,
                report_oids,
                int(in_role_appointment_type_id),
                int(in_role_discipline_id),
            )
            in_role_other_comp_cache = count_official_segment_competitions_batch(
                report_oids,
                season_year_codes=season_codes,
                appointment_type_id=int(in_role_appointment_type_id),
                segment_discipline_type_ids=segment_discipline_type_ids_for_directory(
                    int(in_role_discipline_id),
                    int(in_role_appointment_type_id),
                ),
            )
            meta["show_in_role_columns"] = True
            meta["show_total_comps_in_role"] = True
        held_types_cache = _batch_held_appointment_type_ids(session, report_oids)
        competition_appt_lines = _batch_competition_directory_appointment_lines(
            session,
            report_oids,
            criteria_rows,
        )

        for seg in segments:
            all_rows.extend(
                _rows_from_criterion_segment(
                    seg,
                    overall_years=overall_years,
                    in_role_years=in_role_years,
                    other_comp_cache=other_comp_cache,
                    in_role_other_comp_cache=in_role_other_comp_cache,
                    in_role_appointment_year_cache=in_role_appointment_year_cache,
                    held_types_cache=held_types_cache,
                    competition_appt_lines_by_official=competition_appt_lines,
                    appointment_name_to_id=appt_name_to_id,
                )
            )

    report_rows = _merge_report_rows(all_rows)
    df = pd.DataFrame(report_rows)
    if not df.empty:
        df = (
            df.sort_values("Name", kind="mergesort")
            .reset_index(drop=True)
            .pipe(_order_report_columns)
        )
    return df, meta


def get_official_form_response(
    form_id: int, official_id: int, engine=None
) -> dict[str, Any] | None:
    db_engine = engine or get_engine()
    with Session(db_engine) as session:
        row = session.scalar(
            select(QualifyingOfficialFormResponse.response_json).where(
                QualifyingOfficialFormResponse.form_id == int(form_id),
                QualifyingOfficialFormResponse.official_id == int(official_id),
            )
        )
    return dict(row) if row else None
