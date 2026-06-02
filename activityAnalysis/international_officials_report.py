"""PDF export for international officials appointment detail reports."""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from typing import Any

import pandas as pd

try:
    from fpdf import FPDF
except ModuleNotFoundError:
    FPDF = None  # type: ignore[misc, assignment]

try:
    from activityAnalysis.international_listing_seasons import format_usfs_season_code
    from activityAnalysis.international_officials_data import (
        _nullable_int_for_sql,
        get_international_official_activity_detail,
        load_international_panel_segments_bulk,
    )
    from activityAnalysis.international_requirements import (
        RequirementEvaluation,
        _batch_isu_listing_keys,
        _international_level_id,
        _isu_level_id,
        directory_listing_tier_for_level,
        evaluate_requirements_for_appointment,
        format_competition_alternatives_detail,
        listing_tier_display_label,
        should_evaluate_promote_requirements,
    )
    from activityAnalysis.load_activity_data import get_engine
except ModuleNotFoundError:
    from international_listing_seasons import format_usfs_season_code
    from international_officials_data import (
        _nullable_int_for_sql,
        get_international_official_activity_detail,
        load_international_panel_segments_bulk,
    )
    from international_requirements import (
        RequirementEvaluation,
        _batch_isu_listing_keys,
        _international_level_id,
        _isu_level_id,
        directory_listing_tier_for_level,
        evaluate_requirements_for_appointment,
        format_competition_alternatives_detail,
        listing_tier_display_label,
        should_evaluate_promote_requirements,
    )
    from load_activity_data import get_engine


def _primary_requirement_evaluation(
    evals: list[RequirementEvaluation],
) -> RequirementEvaluation | None:
    if not evals:
        return None
    applicable = [e for e in evals if not e.not_applicable]
    return applicable[0] if applicable else evals[0]


def _requirement_metric_label(ev: RequirementEvaluation | None) -> str:
    if ev is None:
        return "N/A"
    if ev.not_applicable:
        return "N/A"
    return "Yes" if ev.meets else "No"


@dataclass
class AppointmentDetailContext:
    official_id: int
    appointment_type_id: int
    discipline_id: int | None
    official_name: str
    appointment_type: str
    discipline: str
    appointment_level: str
    listing_season_code: int
    report_season_codes: list[int]
    competition_count: int
    segment_count: int
    maintain_primary: RequirementEvaluation | None
    promote_primary: RequirementEvaluation | None
    listing_tier: str
    show_promote: bool
    promote_note: str | None
    panel_detail: pd.DataFrame


def sanitize_pdf_filename(text: str, *, max_len: int = 80) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", str(text or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_")
    return (cleaned or "report")[:max_len]


def appointment_detail_pdf_filename(ctx: AppointmentDetailContext) -> str:
    parts = [
        sanitize_pdf_filename(ctx.official_name),
        sanitize_pdf_filename(ctx.appointment_type),
    ]
    if ctx.discipline and ctx.discipline != "—":
        parts.append(sanitize_pdf_filename(str(ctx.discipline)))
    return "_".join(parts) + ".pdf"


def bulk_reports_zip_filename(*, listing_season_code: int, report_season_window: int) -> str:
    return (
        f"international_officials_{format_usfs_season_code(listing_season_code)}"
        f"_{report_season_window}seasons.zip"
    )


def _pdf_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("\u2014", "-")
        .replace("\u2013", "-")
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .encode("latin-1", "replace")
        .decode("latin-1")
    )


def build_appointment_detail_context(
    summary_row: pd.Series,
    *,
    listing_season_code: int,
    report_season_codes: list[int],
    active_only: bool,
    panel_bulk: pd.DataFrame | None = None,
) -> AppointmentDetailContext:
    official_id = int(summary_row["official_id"])
    appointment_type_id = int(summary_row["appointment_type_id"])
    discipline_id = _nullable_int_for_sql(summary_row.get("discipline_id"))
    official_name = (summary_row.get("official_name") or "").strip() or f"Official {official_id}"
    appointment_type = str(summary_row.get("appointment_type") or "")
    discipline = str(summary_row.get("discipline") or "—")
    appointment_level = str(summary_row.get("appointment_level") or "")
    appointment_level_id = summary_row.get("appointment_level_id")

    panel = panel_bulk
    if panel is None:
        panel = load_international_panel_segments_bulk(
            [official_id], season_codes=report_season_codes
        )

    from sqlalchemy.orm import Session

    with Session(get_engine()) as session:
        isu_level_id = _isu_level_id(session)
        international_level_id = _international_level_id(session)

    isu_listing_keys = _batch_isu_listing_keys([official_id])
    show_promote = should_evaluate_promote_requirements(
        official_id,
        appointment_type_id,
        discipline_id,
        appointment_level,
        appointment_level_id,
        isu_level_id=isu_level_id,
        isu_listing_keys=isu_listing_keys,
    )

    maintain_evals = evaluate_requirements_for_appointment(
        official_id,
        appointment_type_id,
        discipline_id,
        "maintain",
        listing_season_code=listing_season_code,
        panel_bulk=panel,
        isu_level_id=isu_level_id,
        isu_listing_keys=isu_listing_keys,
    )
    listing_tier = directory_listing_tier_for_level(
        appointment_level,
        level_id=appointment_level_id,
        isu_level_id=isu_level_id,
        international_level_id=international_level_id,
    )
    tier_rules = [e for e in maintain_evals if e.listing_tier == listing_tier]
    maintain_primary = _primary_requirement_evaluation(tier_rules)

    promote_primary: RequirementEvaluation | None = None
    promote_note: str | None = None
    if not show_promote:
        promote_note = (
            "Promotion checks apply to International-level appointments not yet ISU-listed."
        )
    else:
        promote_evals = evaluate_requirements_for_appointment(
            official_id,
            appointment_type_id,
            discipline_id,
            "promote",
            listing_season_code=listing_season_code,
            panel_bulk=panel,
            isu_level_id=isu_level_id,
            isu_listing_keys=isu_listing_keys,
        )
        promote_applicable = [e for e in promote_evals if not e.not_applicable]
        promote_primary = _primary_requirement_evaluation(promote_applicable)

    panel_detail = get_international_official_activity_detail(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_only,
        panel_bulk=panel,
        season_codes=report_season_codes,
    )

    return AppointmentDetailContext(
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_name=official_name,
        appointment_type=appointment_type,
        discipline=discipline,
        appointment_level=appointment_level,
        listing_season_code=listing_season_code,
        report_season_codes=report_season_codes,
        competition_count=int(summary_row.get("competition_count") or 0),
        segment_count=int(summary_row.get("segment_count") or 0),
        maintain_primary=maintain_primary,
        promote_primary=promote_primary,
        listing_tier=listing_tier,
        show_promote=show_promote,
        promote_note=promote_note,
        panel_detail=panel_detail,
    )


class _ReportPDF(FPDF):
    def footer(self) -> None:
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 8, _pdf_text(f"Page {self.page_no()}"), align="C")


def _pdf_write_heading(pdf: _ReportPDF, text: str, *, level: int = 1) -> None:
    sizes = {1: 14, 2: 12, 3: 11}
    pdf.set_font("Helvetica", "B", sizes.get(level, 11))
    pdf.multi_cell(0, 7, _pdf_text(text))
    pdf.ln(1)


def _pdf_write_body(pdf: _ReportPDF, text: str, *, bold: bool = False) -> None:
    pdf.set_font("Helvetica", "B" if bold else "", 10)
    pdf.multi_cell(0, 5, _pdf_text(text))
    pdf.ln(1)


def _pdf_write_qualifying_table(pdf: _ReportPDF, activity: pd.DataFrame | None) -> None:
    if activity is None or activity.empty:
        return
    cols = [
        ("competition_year", "Season"),
        ("competition_name", "Competition"),
        ("competition_scope", "Scope"),
        ("competition_type", "Type"),
        ("panel_roles", "Role(s)"),
    ]
    present = [(src, label) for src, label in cols if src in activity.columns]
    if not present:
        return
    pdf.set_font("Helvetica", "B", 8)
    col_w = pdf.epw / len(present)
    for _, label in present:
        pdf.cell(col_w, 5, _pdf_text(label), border=1)
    pdf.ln()
    pdf.set_font("Helvetica", "", 8)
    for _, row in activity.iterrows():
        for src, _ in present:
            val = row[src]
            if src == "competition_year" and pd.notna(val):
                text = format_usfs_season_code(int(val))
            else:
                text = "" if pd.isna(val) else str(val)
            pdf.cell(col_w, 5, _pdf_text(text)[:40], border=1)
        pdf.ln()


def _pdf_write_requirement_section(
    pdf: _ReportPDF,
    title: str,
    ev: RequirementEvaluation | None,
    *,
    empty_message: str,
) -> None:
    _pdf_write_heading(pdf, title, level=2)
    if ev is None:
        _pdf_write_body(pdf, empty_message)
        return
    if ev.not_applicable:
        _pdf_write_body(pdf, f"{ev.label} ({ev.isu_rule_ref})")
        _pdf_write_body(pdf, ev.not_applicable_reason or "Not applicable.")
        return
    status = "Meets requirements" if ev.meets else "Does not meet requirements"
    _pdf_write_body(pdf, f"{ev.label} ({ev.isu_rule_ref})", bold=True)
    _pdf_write_body(pdf, f"Overall: {status}")
    if ev.summary_note and not ev.meets:
        _pdf_write_body(pdf, ev.summary_note)
    seasons = ", ".join(format_usfs_season_code(c) for c in ev.season_codes)
    _pdf_write_body(pdf, f"Season window: {seasons}")
    for rule in ev.rule_results:
        met = "Yes" if rule.met else "No"
        _pdf_write_body(pdf, f"{rule.display_label} — Met: {met}", bold=True)
        if rule.metric == "competition_alternatives":
            for line in format_competition_alternatives_detail(rule.detail, met=rule.met):
                _pdf_write_body(pdf, f"  • {line}")
        else:
            _pdf_write_body(pdf, f"  Progress: {rule.detail}")
        if (
            rule.qualifying_competitions is not None
            and not rule.qualifying_competitions.empty
            and len(ev.rule_results) > 1
        ):
            _pdf_write_body(pdf, "Competitions for this rule:")
            _pdf_write_qualifying_table(pdf, rule.qualifying_competitions)
    if ev.qualifying_activity is not None and not ev.qualifying_activity.empty:
        _pdf_write_body(pdf, "Qualifying competitions:")
        _pdf_write_qualifying_table(pdf, ev.qualifying_activity)
    pdf.ln(2)


def _pdf_write_panel_table(pdf: _ReportPDF, detail: pd.DataFrame) -> None:
    if detail.empty:
        _pdf_write_body(pdf, "No matching panel segments in the selected seasons.")
        return
    cols = [
        ("competition_year", "Season"),
        ("competition_name", "Competition"),
        ("competition_scope", "Scope"),
        ("competition_type", "Type"),
        ("segment_name", "Segment"),
        ("segment_level", "Level"),
        ("segment_discipline", "Discipline"),
    ]
    present = [(src, label) for src, label in cols if src in detail.columns]
    pdf.set_font("Helvetica", "B", 7)
    col_w = pdf.epw / len(present)
    for _, label in present:
        pdf.cell(col_w, 4, _pdf_text(label), border=1)
    pdf.ln()
    pdf.set_font("Helvetica", "", 7)
    for _, row in detail.iterrows():
        if pdf.get_y() > pdf.eph - 12:
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 7)
            for _, label in present:
                pdf.cell(col_w, 4, _pdf_text(label), border=1)
            pdf.ln()
            pdf.set_font("Helvetica", "", 7)
        for src, _ in present:
            val = row[src]
            if src == "competition_year" and pd.notna(val):
                text = format_usfs_season_code(int(val))
            else:
                text = "" if pd.isna(val) else str(val)
            pdf.cell(col_w, 4, _pdf_text(text)[:35], border=1)
        pdf.ln()


def build_appointment_detail_pdf(ctx: AppointmentDetailContext) -> bytes:
    if FPDF is None:
        raise RuntimeError("fpdf2 is required for PDF export (pip install fpdf2)")

    pdf = _ReportPDF()
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.add_page()

    _pdf_write_heading(pdf, ctx.official_name, level=1)
    _pdf_write_body(
        pdf,
        f"{ctx.appointment_type} · {ctx.discipline} · "
        f"{ctx.appointment_level or 'Level unknown'} · "
        f"Listing {format_usfs_season_code(ctx.listing_season_code)}",
    )
    _pdf_write_body(
        pdf,
        "Activity seasons: "
        + ", ".join(format_usfs_season_code(c) for c in ctx.report_season_codes),
    )
    _pdf_write_body(
        pdf,
        f"Competitions: {ctx.competition_count}   Segments: {ctx.segment_count}   "
        f"Maintain: {_requirement_metric_label(ctx.maintain_primary)}   "
        f"Promote: {_requirement_metric_label(ctx.promote_primary) if ctx.show_promote else '-'}",
        bold=True,
    )
    pdf.ln(2)

    maintain_title = f"Maintain ({listing_tier_display_label(ctx.listing_tier)} listing)"
    _pdf_write_requirement_section(
        pdf,
        maintain_title,
        ctx.maintain_primary,
        empty_message="No matching requirement rules configured.",
    )

    if ctx.promote_note:
        _pdf_write_heading(pdf, "Promote", level=2)
        _pdf_write_body(pdf, ctx.promote_note)
    else:
        _pdf_write_requirement_section(
            pdf,
            "Promote to ISU",
            ctx.promote_primary,
            empty_message="No promotion requirement profile applies.",
        )

    _pdf_write_heading(pdf, "Panel activity (this appointment)", level=2)
    _pdf_write_panel_table(pdf, ctx.panel_detail)

    out = pdf.output()
    return out if isinstance(out, (bytes, bytearray)) else out.encode("latin-1")


def build_bulk_appointment_reports_zip(
    summary: pd.DataFrame,
    *,
    listing_season_code: int,
    report_season_codes: list[int],
    report_season_window: int,
    active_only: bool,
    panel_bulk: pd.DataFrame | None = None,
) -> bytes:
    if summary.empty:
        return b""

    panel = panel_bulk
    if panel is None:
        official_ids = summary["official_id"].astype(int).unique().tolist()
        panel = load_international_panel_segments_bulk(
            official_ids, season_codes=report_season_codes
        )
    panel_by_official: dict[int, pd.DataFrame] | None = None
    if panel is not None and not panel.empty:
        panel_by_official = {
            int(oid): group for oid, group in panel.groupby("official_id", sort=False)
        }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for _, row in summary.iterrows():
            oid = int(row["official_id"])
            sub_panel = None
            if panel_by_official is not None:
                sub_panel = panel_by_official.get(oid)
            ctx = build_appointment_detail_context(
                row,
                listing_season_code=listing_season_code,
                report_season_codes=report_season_codes,
                active_only=active_only,
                panel_bulk=sub_panel,
            )
            pdf_bytes = build_appointment_detail_pdf(ctx)
            zf.writestr(appointment_detail_pdf_filename(ctx), pdf_bytes)
    return buf.getvalue()
