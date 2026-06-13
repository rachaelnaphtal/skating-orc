"""PDF export for international officials appointment detail reports."""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd

try:
    from fpdf import FPDF
except ModuleNotFoundError:
    FPDF = None  # type: ignore[misc, assignment]

try:
    from activityAnalysis.international_listing_seasons import (
        format_listing_reference_july1,
        format_promote_first_eligible_display,
        format_usfs_season_code,
    )
    from activityAnalysis.international_official_demographics import (
        appointment_demographics_row,
        load_grade_dates_for_appointments,
        load_official_birthdates,
        load_official_international_appointment_rows,
    )
    from activityAnalysis.international_officials_data import (
        _nullable_int_for_sql,
        get_international_official_activity_detail,
        get_international_official_activity_other_appointments,
        load_international_panel_segments_bulk,
        split_panel_detail_by_scope,
    )
    from activityAnalysis.international_official_seminars import (
        load_official_seminars_bulk,
        seminars_display_for_appointment,
    )
    from activityAnalysis.international_requirements import (
        RequirementEvaluation,
        RuleCheckResult,
        _appointment_context,
        _appointment_context_from_batch,
        _batch_appointment_contexts,
        _batch_isu_listing_keys,
        _international_level_id,
        _isu_level_id,
        directory_listing_tier_for_level,
        evaluate_requirements_for_appointment,
        first_listing_season_eligible_for_promote_years,
        format_competition_alternatives_detail,
        format_seminar_alternatives_detail,
        listing_tier_display_label,
        should_evaluate_promote_requirements,
    )
    from activityAnalysis.load_activity_data import get_engine
except ModuleNotFoundError:
    from international_listing_seasons import (
        format_listing_reference_july1,
        format_promote_first_eligible_display,
        format_usfs_season_code,
    )
    from international_official_demographics import (
        appointment_demographics_row,
        load_grade_dates_for_appointments,
        load_official_birthdates,
        load_official_international_appointment_rows,
    )
    from international_officials_data import (
        _nullable_int_for_sql,
        get_international_official_activity_detail,
        get_international_official_activity_other_appointments,
        load_international_panel_segments_bulk,
        split_panel_detail_by_scope,
    )
    from international_official_seminars import (
        load_official_seminars_bulk,
        seminars_display_for_appointment,
    )
    from international_requirements import (
        RequirementEvaluation,
        RuleCheckResult,
        _appointment_context,
        _appointment_context_from_batch,
        _batch_appointment_contexts,
        _batch_isu_listing_keys,
        _international_level_id,
        _isu_level_id,
        directory_listing_tier_for_level,
        evaluate_requirements_for_appointment,
        first_listing_season_eligible_for_promote_years,
        format_competition_alternatives_detail,
        format_seminar_alternatives_detail,
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
    competition_count_international: int
    competition_count_national: int
    segment_count_international: int
    segment_count_national: int
    maintain_primary: RequirementEvaluation | None
    promote_primary: RequirementEvaluation | None
    listing_tier: str
    show_promote: bool
    promote_note: str | None
    panel_detail: pd.DataFrame
    panel_international_this: pd.DataFrame | None = None
    panel_national_this: pd.DataFrame | None = None
    panel_international_other: pd.DataFrame | None = None
    age_as_of_listing: int | None = None
    years_in_grade: int | None = None
    grade_date: date | None = None
    promote_first_eligible_listing: int | None = None
    promote_first_eligible_display: str = "—"
    seminars: pd.DataFrame = field(default_factory=pd.DataFrame)


def _demographics_display_value(value: int | None) -> str:
    return "—" if value is None else str(int(value))


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


_PDF_UNICODE_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("\u2265", ">="),  # ≥
    ("\u2264", "<="),  # ≤
    ("\u00d7", "x"),  # ×
    ("\u2014", "-"),  # —
    ("\u2013", "-"),  # –
    ("\u2018", "'"),
    ("\u2019", "'"),
    ("\u201c", '"'),
    ("\u201d", '"'),
    ("\u2022", "*"),  # •
    ("\u2026", "..."),  # …
    ("\u00a0", " "),  # non-breaking space
)


def _pdf_text(value: Any) -> str:
    """Normalize text for Helvetica / Latin-1 PDF output (no ``?`` placeholders)."""
    text = "" if value is None else str(value)
    for old, new in _PDF_UNICODE_REPLACEMENTS:
        text = text.replace(old, new)
    return text.encode("latin-1", "replace").decode("latin-1")


def build_appointment_detail_context(
    summary_row: pd.Series,
    *,
    listing_season_code: int,
    report_season_codes: list[int],
    active_only: bool,
    panel_bulk: pd.DataFrame | None = None,
    seminars_bulk: pd.DataFrame | None = None,
    birthdates: dict[int, Any] | None = None,
    grade_dates: dict[tuple[int, int, int | None], Any] | None = None,
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

    listing_tier = directory_listing_tier_for_level(
        appointment_level,
        level_id=appointment_level_id,
        isu_level_id=isu_level_id,
        international_level_id=international_level_id,
    )

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

    seminars_by_official: dict[int, pd.DataFrame] | None = None
    if seminars_bulk is not None and not seminars_bulk.empty:
        seminars_by_official = {
            int(oid): group for oid, group in seminars_bulk.groupby("official_id", sort=False)
        }

    maintain_evals = evaluate_requirements_for_appointment(
        official_id,
        appointment_type_id,
        discipline_id,
        "maintain",
        listing_season_code=listing_season_code,
        panel_bulk=panel,
        seminars_bulk=seminars_bulk,
        seminars_by_official=seminars_by_official,
        isu_level_id=isu_level_id,
        isu_listing_keys=isu_listing_keys,
        listing_tier=listing_tier,
    )
    maintain_primary = _primary_requirement_evaluation(maintain_evals)

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
            seminars_bulk=seminars_bulk,
            seminars_by_official=seminars_by_official,
            isu_level_id=isu_level_id,
            isu_listing_keys=isu_listing_keys,
            listing_tier=listing_tier,
        )
        promote_primary = _primary_requirement_evaluation(promote_evals)

    panel_detail = get_international_official_activity_detail(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_only,
        panel_bulk=panel,
        season_codes=report_season_codes,
    )
    panel_international_this, panel_national_this = split_panel_detail_by_scope(panel_detail)
    panel_international_other = get_international_official_activity_other_appointments(
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        active_appointments_only=active_only,
        panel_bulk=panel,
        season_codes=report_season_codes,
    )
    if not panel_international_other.empty:
        panel_international_other, _ = split_panel_detail_by_scope(panel_international_other)

    grade_key = (official_id, appointment_type_id, discipline_id)
    if birthdates is None:
        birthdates = load_official_birthdates([official_id])
    if grade_dates is None:
        grade_dates = load_grade_dates_for_appointments([grade_key])
    grade_date = grade_dates.get(grade_key)
    demo = appointment_demographics_row(
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        listing_season_code=listing_season_code,
        birthdates=birthdates,
        grade_dates=grade_dates,
    )

    first_promote: int | None = None
    if show_promote:
        cached = summary_row.get("promote_first_eligible_listing")
        if cached is not None and not pd.isna(cached):
            first_promote = int(cached)
        else:
            appointment_rows = load_official_international_appointment_rows(
                [official_id]
            ).get(official_id, [])
            appointment_contexts = _batch_appointment_contexts([official_id])
            appt_ctx = _appointment_context_from_batch(
                appointment_contexts,
                official_id,
                appointment_type_id,
                summary_row.get("discipline_id"),
            )
            first_promote = first_listing_season_eligible_for_promote_years(
                official_id=official_id,
                appointment_type_id=appointment_type_id,
                directory_discipline_id=summary_row.get("discipline_id"),
                appointment_level=appointment_level,
                appointment_level_id=appointment_level_id,
                appointment_rows=appointment_rows,
                appt_ctx=appt_ctx,
                listing_season_code=listing_season_code,
                rules_df=None,
                international_level_id=international_level_id,
                isu_level_id=isu_level_id,
                isu_listing_keys=isu_listing_keys,
            )
    promote_first_display = format_promote_first_eligible_display(
        first_promote,
        current_listing_season_code=listing_season_code,
    )

    seminars = seminars_display_for_appointment(
        official_id,
        appointment_type_id,
        discipline_id,
        seminars_bulk=seminars_bulk,
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
        competition_count_international=int(
            summary_row.get("competition_count_international") or 0
        ),
        competition_count_national=int(summary_row.get("competition_count_national") or 0),
        segment_count_international=int(summary_row.get("segment_count_international") or 0),
        segment_count_national=int(summary_row.get("segment_count_national") or 0),
        maintain_primary=maintain_primary,
        promote_primary=promote_primary,
        listing_tier=listing_tier,
        show_promote=show_promote,
        promote_note=promote_note,
        panel_detail=panel_detail,
        panel_international_this=panel_international_this,
        panel_national_this=panel_national_this,
        panel_international_other=panel_international_other,
        age_as_of_listing=demo["age_as_of_listing"],
        years_in_grade=demo["years_in_grade"],
        grade_date=grade_date,
        promote_first_eligible_listing=first_promote,
        promote_first_eligible_display=promote_first_display,
        seminars=seminars,
    )


class _ReportPDF(FPDF):
    def footer(self) -> None:
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 8, _pdf_text(f"Page {self.page_no()}"), align="C")


def _pdf_write_wrapped(
    pdf: _ReportPDF,
    text: str,
    *,
    line_h: float = 5,
) -> None:
    """
    Full-width wrapped text.

    fpdf2 leaves the cursor at the end of the last line after ``multi_cell``; callers
    must reset ``x`` before each block (notably after ``cell()`` rows and in loops).
    """
    pdf.set_x(pdf.l_margin)
    pdf.multi_cell(pdf.epw, line_h, _pdf_text(text))


def _pdf_write_heading(pdf: _ReportPDF, text: str, *, level: int = 1) -> None:
    sizes = {1: 14, 2: 12, 3: 11}
    pdf.set_font("Helvetica", "B", sizes.get(level, 11))
    _pdf_write_wrapped(pdf, text, line_h=7)
    pdf.ln(1)


def _pdf_write_body(pdf: _ReportPDF, text: str, *, bold: bool = False) -> None:
    pdf.set_font("Helvetica", "B" if bold else "", 10)
    _pdf_write_wrapped(pdf, text, line_h=5)
    pdf.ln(1)


_PDF_TABLE_LINE_H = 3.5
_PDF_TABLE_FONT_SIZE = 7
_PDF_TABLE_HEADER_FONT_SIZE = 8


def _pdf_short_discipline(value: Any) -> str:
    text = "" if value is None or pd.isna(value) else str(value).strip()
    if not text or text == "-":
        return ""
    lower = text.lower()
    if "synch" in lower:
        return "SYS"
    if "single" in lower:
        return "Singles"
    if "pair" in lower:
        return "Pairs"
    if "dance" in lower:
        return "Dance"
    return text if len(text) <= 14 else text[:12] + "..."


def _pdf_short_appointment_type(value: Any) -> str:
    text = "" if value is None or pd.isna(value) else str(value).strip()
    if not text:
        return ""
    return (
        text.replace("International ", "Intl ")
        .replace("Technical Controller", "TC")
        .replace("Technical Specialist", "TS")
        .replace("Data / Video Operator", "IDVO")
    )


def _pdf_table_cell_text(row: pd.Series, src: str) -> str:
    val = row.get(src)
    if src == "competition_year" and pd.notna(val):
        return format_usfs_season_code(int(val))
    if src in ("segment_discipline", "discipline"):
        return _pdf_short_discipline(val)
    if src == "appointment_type":
        return _pdf_short_appointment_type(val)
    return "" if pd.isna(val) else str(val)


def _pdf_table_widths(pdf: _ReportPDF, weights: list[float]) -> list[float]:
    total_w = float(sum(weights))
    scale = pdf.epw / total_w
    return [w * scale for w in weights]


def _pdf_table_row_height(
    pdf: _ReportPDF,
    texts: list[str],
    widths: list[float],
    *,
    line_h: float = _PDF_TABLE_LINE_H,
    font_size: int = _PDF_TABLE_FONT_SIZE,
) -> float:
    pdf.set_font("Helvetica", "", font_size)
    max_h = line_h
    for text, width in zip(texts, widths):
        lines = pdf.multi_cell(
            width,
            line_h,
            _pdf_text(text),
            dry_run=True,
            output="LINES",
        )
        max_h = max(max_h, len(lines) * line_h)
    return max_h


def _pdf_table_ensure_space(pdf: _ReportPDF, needed_h: float) -> bool:
    """Add a page when ``needed_h`` does not fit; return True if a break was added."""
    if pdf.will_page_break(needed_h):
        pdf.add_page()
        return True
    return False


def _pdf_draw_table_row(
    pdf: _ReportPDF,
    texts: list[str],
    widths: list[float],
    *,
    row_h: float,
    line_h: float = _PDF_TABLE_LINE_H,
    font_size: int = _PDF_TABLE_FONT_SIZE,
    bold: bool = False,
) -> None:
    x0 = pdf.l_margin
    y0 = pdf.get_y()
    style = "B" if bold else ""
    pdf.set_font("Helvetica", style, font_size)
    pad = 0.5
    auto_break = pdf.auto_page_break
    bottom_margin = pdf.b_margin
    pdf.set_auto_page_break(False, bottom_margin)
    try:
        for text, width in zip(texts, widths):
            pdf.rect(x0, y0, width, row_h)
            inner_w = max(width - 2 * pad, 4)
            pdf.set_xy(x0 + pad, y0 + pad)
            pdf.multi_cell(inner_w, line_h, _pdf_text(text), border=0)
            x0 += width
            pdf.set_xy(x0, y0)
    finally:
        pdf.set_auto_page_break(auto_break, bottom_margin)
    pdf.set_xy(pdf.l_margin, y0 + row_h)


def _pdf_write_data_table(
    pdf: _ReportPDF,
    activity: pd.DataFrame | None,
    column_specs: list[tuple[str, str, float]],
    *,
    empty_message: str | None = None,
) -> None:
    if activity is None or activity.empty:
        if empty_message:
            _pdf_write_body(pdf, empty_message)
        return
    present = [(src, label, weight) for src, label, weight in column_specs if src in activity.columns]
    if not present:
        if empty_message:
            _pdf_write_body(pdf, empty_message)
        return

    widths = _pdf_table_widths(pdf, [weight for _, _, weight in present])
    labels = [label for _, label, _ in present]

    header_h = _PDF_TABLE_LINE_H + 1

    def draw_header() -> None:
        _pdf_draw_table_row(
            pdf,
            labels,
            widths,
            row_h=header_h,
            line_h=_PDF_TABLE_LINE_H,
            font_size=_PDF_TABLE_HEADER_FONT_SIZE,
            bold=True,
        )

    _pdf_table_ensure_space(pdf, header_h + 2)
    draw_header()

    pdf.set_font("Helvetica", "", _PDF_TABLE_FONT_SIZE)
    for _, row in activity.iterrows():
        texts = [_pdf_table_cell_text(row, src) for src, _, _ in present]
        row_h = _pdf_table_row_height(pdf, texts, widths) + 1
        if _pdf_table_ensure_space(pdf, row_h + 1):
            draw_header()
        _pdf_draw_table_row(pdf, texts, widths, row_h=row_h)


_PDF_SEMINAR_COLUMNS = [
    ("Date", "Date", 0.12),
    ("Season", "Season", 0.14),
    ("In person", "In person", 0.1),
    ("At event", "At event", 0.1),
    ("Place", "Place", 0.22),
    ("Notes", "Notes", 0.32),
]


def _pdf_write_seminar_table(pdf: _ReportPDF, seminars: pd.DataFrame | None) -> None:
    _pdf_write_data_table(
        pdf,
        seminars,
        _PDF_SEMINAR_COLUMNS,
        empty_message="No seminar attendance recorded for this appointment.",
    )


def _pdf_write_qualifying_table(pdf: _ReportPDF, activity: pd.DataFrame | None) -> None:
    _pdf_write_data_table(
        pdf,
        activity,
        [
            ("competition_year", "Season", 0.1),
            ("competition_name", "Competition", 0.42),
            ("competition_scope", "Scope", 0.12),
            ("competition_type", "Type", 0.14),
            ("panel_roles", "Role(s)", 0.22),
        ],
    )


_PDF_PANEL_THIS_INTL_COLUMNS = [
    ("competition_year", "Season", 0.09),
    ("competition_name", "Competition", 0.44),
    ("competition_type", "Type", 0.11),
    ("segment_name", "Segment", 0.22),
    ("segment_level", "Level", 0.08),
]

_PDF_PANEL_THIS_NATIONAL_COLUMNS = list(_PDF_PANEL_THIS_INTL_COLUMNS)

_PDF_PANEL_OTHER_INTL_COLUMNS = [
    ("competition_year", "Season", 0.08),
    ("competition_name", "Competition", 0.34),
    ("appointment_type", "Appointment", 0.16),
    ("discipline", "Discipline", 0.1),
    ("competition_type", "Type", 0.1),
    ("segment_name", "Segment", 0.22),
]


def _pdf_status_fill(met: bool | None) -> tuple[int, int, int]:
    if met is None:
        return (235, 235, 235)
    if met:
        return (210, 240, 210)
    return (255, 220, 220)


def _pdf_status_label(met: bool | None) -> str:
    if met is None:
        return "N/A"
    return "MET" if met else "NOT MET"


def _pdf_requirement_summary_note(ev: RequirementEvaluation) -> str:
    if ev.not_applicable:
        return ev.not_applicable_reason or "Not applicable"
    if ev.meets:
        return "All rules satisfied"
    if ev.summary_note:
        return ev.summary_note
    unmet = [r.display_label for r in ev.rule_results if not r.met]
    if unmet:
        return "Needs: " + "; ".join(unmet[:4]) + ("..." if len(unmet) > 4 else "")
    return "Does not meet requirements"


def _pdf_write_status_badge(
    pdf: _ReportPDF,
    label: str,
    met: bool | None,
    *,
    width: float = 22,
) -> None:
    r, g, b = _pdf_status_fill(met)
    pdf.set_fill_color(r, g, b)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(width, 6, _pdf_text(_pdf_status_label(met)), border=1, fill=True, align="C")
    pdf.set_fill_color(255, 255, 255)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(4, 6, "", border=0)
    pdf.cell(0, 6, _pdf_text(label), border=0)
    pdf.ln(6)
    pdf.set_x(pdf.l_margin)


def _pdf_write_at_a_glance(pdf: _ReportPDF, ctx: AppointmentDetailContext) -> None:
    _pdf_write_heading(pdf, "At a glance", level=2)
    rows: list[tuple[str, bool | None, str]] = []
    rows.append(
        (
            f"Maintain ({listing_tier_display_label(ctx.listing_tier)})",
            None
            if ctx.maintain_primary is None
            else (None if ctx.maintain_primary.not_applicable else ctx.maintain_primary.meets),
            _pdf_requirement_summary_note(ctx.maintain_primary)
            if ctx.maintain_primary is not None
            else "No rules configured",
        )
    )
    if ctx.promote_note:
        rows.append(("Promote to ISU", None, ctx.promote_note))
    elif ctx.show_promote:
        prom = ctx.promote_primary
        rows.append(
            (
                "Promote to ISU",
                None if prom is None else (None if prom.not_applicable else prom.meets),
                _pdf_requirement_summary_note(prom)
                if prom is not None
                else "No promotion profile",
            )
        )
    for label, met, note in rows:
        _pdf_write_status_badge(pdf, label, met)
        if note:
            pdf.set_font("Helvetica", "", 9)
            _pdf_write_wrapped(pdf, note, line_h=4)
            pdf.ln(1)
    pdf.ln(2)


def _pdf_write_rule_block(pdf: _ReportPDF, rule: RuleCheckResult) -> None:
    _pdf_table_ensure_space(pdf, 18)
    met = rule.met
    r, g, b = _pdf_status_fill(met)
    pdf.set_fill_color(r, g, b)
    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(14, 5, _pdf_text("MET" if met else "NO"), border=1, fill=True, align="C")
    pdf.set_fill_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 8)
    pdf.cell(0, 5, _pdf_text(rule.display_label), border=0)
    pdf.ln(5)
    pdf.set_x(pdf.l_margin)
    pdf.set_font("Helvetica", "", 8)
    if rule.metric == "competition_alternatives":
        for line in format_competition_alternatives_detail(rule.detail, met=rule.met):
            _pdf_write_wrapped(pdf, f"  - {line}", line_h=4)
    elif rule.metric == "seminar_alternatives":
        for line in format_seminar_alternatives_detail(rule.detail, met=rule.met):
            _pdf_write_wrapped(pdf, f"  - {line}", line_h=4)
    else:
        _pdf_write_wrapped(pdf, f"  Progress: {rule.detail}", line_h=4)
    pdf.ln(1)


def _pdf_write_requirement_section(
    pdf: _ReportPDF,
    title: str,
    ev: RequirementEvaluation | None,
    *,
    empty_message: str,
) -> None:
    _pdf_write_heading(pdf, title, level=3)
    if ev is None:
        _pdf_write_body(pdf, empty_message)
        return
    if ev.not_applicable:
        _pdf_write_status_badge(pdf, ev.label, None)
        _pdf_write_body(pdf, ev.not_applicable_reason or "Not applicable.")
        return

    _pdf_write_status_badge(pdf, f"{ev.label} ({ev.isu_rule_ref})", ev.meets)
    if ev.summary_note and not ev.meets:
        pdf.set_font("Helvetica", "I", 9)
        _pdf_write_wrapped(pdf, ev.summary_note, line_h=4)
        pdf.ln(1)
    seasons = ", ".join(format_usfs_season_code(c) for c in ev.season_codes)
    pdf.set_font("Helvetica", "", 8)
    _pdf_write_wrapped(pdf, f"Season window: {seasons}", line_h=4)
    pdf.ln(2)

    if ev.rule_results:
        _pdf_write_body(pdf, "Rules:", bold=True)
        for rule in ev.rule_results:
            _pdf_write_rule_block(pdf, rule)
            if (
                rule.qualifying_competitions is not None
                and not rule.qualifying_competitions.empty
                and len(ev.rule_results) > 1
            ):
                pdf.set_font("Helvetica", "I", 8)
                _pdf_write_wrapped(pdf, "  Competitions for this rule:", line_h=4)
                _pdf_write_qualifying_table(pdf, rule.qualifying_competitions)
    if ev.qualifying_activity is not None and not ev.qualifying_activity.empty:
        _pdf_write_body(pdf, "Qualifying competitions (combined):", bold=True)
        _pdf_write_qualifying_table(pdf, ev.qualifying_activity)
    pdf.ln(2)


def _pdf_write_panel_section(
    pdf: _ReportPDF,
    title: str,
    detail: pd.DataFrame | None,
    column_specs: list[tuple[str, str, float]],
) -> None:
    _pdf_write_heading(pdf, title, level=2)
    _pdf_write_data_table(
        pdf,
        detail,
        column_specs,
        empty_message="No matching panel segments in the selected seasons.",
    )
    pdf.ln(1)


def _binary_output_bytes(out: bytes | bytearray | str) -> bytes:
    """Streamlit download widgets require ``bytes``, not ``bytearray``."""
    if isinstance(out, bytes):
        return out
    if isinstance(out, bytearray):
        return bytes(out)
    return out.encode("latin-1")


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
    as_of = format_listing_reference_july1(ctx.listing_season_code)
    demo_lines = [
        f"Age {as_of}: {_demographics_display_value(ctx.age_as_of_listing)}",
        f"Years in grade {as_of}: {_demographics_display_value(ctx.years_in_grade)}",
    ]
    if ctx.grade_date is not None:
        demo_lines.append(f"Grade date (achieved or appointed): {ctx.grade_date.isoformat()}")
    if ctx.show_promote:
        demo_lines.append(
            f"First promote year (years): {ctx.promote_first_eligible_display}"
        )
    for line in demo_lines:
        _pdf_write_body(pdf, line)
    _pdf_write_body(
        pdf,
        "Panel activity (report window): "
        f"international {ctx.competition_count_international} competition(s), "
        f"{ctx.segment_count_international} segment(s); "
        f"national {ctx.competition_count_national} competition(s), "
        f"{ctx.segment_count_national} segment(s) "
        f"({ctx.competition_count} competition(s), {ctx.segment_count} segment(s) total).",
    )
    pdf.ln(1)

    _pdf_write_at_a_glance(pdf, ctx)
    _pdf_write_heading(pdf, "Requirement details", level=2)

    maintain_title = f"Maintain ({listing_tier_display_label(ctx.listing_tier)} listing)"
    _pdf_write_requirement_section(
        pdf,
        maintain_title,
        ctx.maintain_primary,
        empty_message="No matching requirement rules configured.",
    )

    if ctx.promote_note:
        _pdf_write_heading(pdf, "Promote to ISU", level=3)
        _pdf_write_body(pdf, ctx.promote_note)
    else:
        _pdf_write_requirement_section(
            pdf,
            "Promote to ISU",
            ctx.promote_primary,
            empty_message="No promotion requirement profile applies.",
        )

    _pdf_write_heading(pdf, "ISU seminar attendance", level=2)
    _pdf_write_body(
        pdf,
        "Seminars recorded for this appointment type and discipline "
        "(Pairs seminars count toward Singles TC/TS appointments).",
    )
    _pdf_write_seminar_table(pdf, ctx.seminars)
    pdf.ln(1)

    intl_this = (
        ctx.panel_international_this
        if ctx.panel_international_this is not None
        else split_panel_detail_by_scope(ctx.panel_detail)[0]
    )
    national_this = (
        ctx.panel_national_this
        if ctx.panel_national_this is not None
        else split_panel_detail_by_scope(ctx.panel_detail)[1]
    )
    intl_other = ctx.panel_international_other
    if intl_other is None:
        intl_other = pd.DataFrame()

    _pdf_write_panel_section(
        pdf,
        "International panel activity (this appointment)",
        intl_this,
        _PDF_PANEL_THIS_INTL_COLUMNS,
    )
    _pdf_write_panel_section(
        pdf,
        "National panel activity (this appointment)",
        national_this,
        _PDF_PANEL_THIS_NATIONAL_COLUMNS,
    )
    _pdf_write_panel_section(
        pdf,
        "International panel activity for other appointments",
        intl_other,
        _PDF_PANEL_OTHER_INTL_COLUMNS,
    )

    out = pdf.output()
    return _binary_output_bytes(out)


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

    official_ids = summary["official_id"].astype(int).unique().tolist()
    birthdates = load_official_birthdates(official_ids)
    grade_keys = [
        (
            int(r["official_id"]),
            int(r["appointment_type_id"]),
            _nullable_int_for_sql(r.get("discipline_id")),
        )
        for _, r in summary.iterrows()
    ]
    grade_dates = load_grade_dates_for_appointments(grade_keys)
    seminars_bulk = load_official_seminars_bulk(official_ids)

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
                seminars_bulk=seminars_bulk,
                birthdates=birthdates,
                grade_dates=grade_dates,
            )
            pdf_bytes = build_appointment_detail_pdf(ctx)
            zf.writestr(appointment_detail_pdf_filename(ctx), pdf_bytes)
    return buf.getvalue()
