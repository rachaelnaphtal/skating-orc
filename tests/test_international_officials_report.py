"""Tests for international officials PDF export."""

from __future__ import annotations

import io
import zipfile

import pandas as pd
import pytest

from activityAnalysis.international_officials_report import (
    AppointmentDetailContext,
    _pdf_short_discipline,
    _pdf_text,
    appointment_detail_pdf_filename,
    build_appointment_detail_pdf,
    build_bulk_appointment_reports_zip,
    bulk_reports_zip_filename,
    sanitize_pdf_filename,
)
from activityAnalysis.international_requirements import (
    RequirementEvaluation,
    RuleCheckResult,
)


def _minimal_context(**overrides) -> AppointmentDetailContext:
    base = dict(
        official_id=1,
        appointment_type_id=10,
        discipline_id=3,
        official_name="Jane Doe",
        appointment_type="Judge",
        discipline="Singles",
        appointment_level="International",
        listing_season_code=2526,
        report_season_codes=[2425, 2324],
        competition_count=2,
        segment_count=4,
        competition_count_international=1,
        competition_count_national=1,
        segment_count_international=3,
        segment_count_national=1,
        maintain_primary=None,
        promote_primary=None,
        listing_tier="international",
        show_promote=True,
        promote_note=None,
        panel_detail=pd.DataFrame(
            [
                {
                    "competition_year": 2425,
                    "competition_name": "Test Event",
                    "competition_scope": "International",
                    "competition_type": "ISU",
                    "segment_name": "Free Skate",
                    "segment_level": "Senior",
                    "segment_discipline": "Singles",
                }
            ]
        ),
    )
    base.update(overrides)
    return AppointmentDetailContext(**base)


def test_pdf_short_discipline_uses_sys_for_synchronized():
    assert _pdf_short_discipline("Synchronized") == "SYS"
    assert _pdf_short_discipline("Synchronized Skating") == "SYS"


def test_pdf_text_replaces_unicode_for_latin1():
    assert _pdf_text("≥4 years in grade in this appointment (listing July 1)") == (
        ">=4 years in grade in this appointment (listing July 1)"
    )
    assert "?" not in _pdf_text("≥4 years — Met: No  • option")
    assert _pdf_text("Referee in ≥2 international competitions") == (
        "Referee in >=2 international competitions"
    )


def test_sanitize_pdf_filename():
    assert sanitize_pdf_filename("Smith, John (Jr.)") == "Smith_John_Jr"
    assert sanitize_pdf_filename("") == "report"


def test_appointment_detail_pdf_filename():
    ctx = _minimal_context()
    assert appointment_detail_pdf_filename(ctx) == "Jane_Doe_Judge_Singles.pdf"


def test_bulk_reports_zip_filename():
    assert bulk_reports_zip_filename(listing_season_code=2526, report_season_window=2) == (
        "international_officials_25-26_2seasons.zip"
    )


def test_build_appointment_detail_pdf_magic_bytes():
    pdf = build_appointment_detail_pdf(_minimal_context())
    assert isinstance(pdf, bytes)
    assert not isinstance(pdf, bytearray)
    assert pdf[:4] == b"%PDF"
    assert len(pdf) > 200


def test_build_appointment_detail_pdf_includes_seminar_section():
    ctx = _minimal_context(
        seminars=pd.DataFrame(
            [
                {
                    "Date": "2025-10-01",
                    "Season": "25-26 (2526)",
                    "In person": "Yes",
                    "At event": "No",
                    "Place": "Geneva",
                    "Notes": "Technical seminar",
                }
            ]
        )
    )
    pdf = build_appointment_detail_pdf(ctx)
    assert b"ISU seminar attendance" in pdf
    assert b"Technical seminar" in pdf


def test_build_appointment_detail_pdf_long_competition_name_and_panel_sections():
    long_name = (
        "2025 ISU Challenger Series Nebelhorn Trophy and International "
        "Figure Skating Competition Presented By Example Sponsor"
    )
    ctx = _minimal_context(
        panel_international_this=pd.DataFrame(
            [
                {
                    "competition_year": 2425,
                    "competition_name": long_name,
                    "competition_type": "ISU",
                    "segment_name": "Free Skate",
                    "segment_level": "Senior",
                    "segment_discipline": "Singles",
                }
            ]
        ),
        panel_national_this=pd.DataFrame(
            [
                {
                    "competition_year": 2425,
                    "competition_name": "U.S. Figure Skating Championships",
                    "competition_type": "National",
                    "segment_name": "Short Program",
                    "segment_level": "Senior",
                    "segment_discipline": "Singles",
                }
            ]
        ),
        panel_international_other=pd.DataFrame(
            [
                {
                    "competition_year": 2324,
                    "competition_name": "ISU World Synchronized Skating Championships",
                    "appointment_type": "International Judge",
                    "discipline": "Synchronized",
                    "competition_type": "ISU",
                    "segment_name": "Free Skate",
                    "segment_level": "Senior",
                    "segment_discipline": "Synchronized",
                }
            ]
        ),
    )
    pdf = build_appointment_detail_pdf(ctx)
    assert pdf[:4] == b"%PDF"
    assert len(pdf) > 400


def test_build_appointment_detail_pdf_competition_alternatives_branches():
    """Multiple competition-alternative lines must not exhaust horizontal space (fpdf2 x cursor)."""
    branch_a = (
        "Judge (>=3 International incl. >=1 ISU Championship among those; "
        "2/3 competitions, 1/1 ISU Championship among those)"
    )
    branch_b = (
        "Referee (>=3 International incl. >=1 ISU Championship among those; "
        "1/3 competitions, 0/1 ISU Championship among those)"
    )
    detail = f"Need one of: {branch_a}; {branch_b}"
    promote = RequirementEvaluation(
        rule_set_id=1,
        isu_rule_ref="413.3.c",
        purpose="promote",
        label="ISU Judge — promote (Singles/Pairs)",
        listing_tier="international",
        season_window=4,
        season_codes=[2223, 2324, 2425, 2526],
        meets=False,
        summary_note="Need one of: " + branch_a[:40] + "...",
        rule_results=[
            RuleCheckResult(
                metric="competition_alternatives",
                display_label=">=3 International incl. >=1 ISU Championship",
                required=1,
                actual=0,
                met=False,
                detail=detail,
            ),
            RuleCheckResult(
                metric="years_in_grade",
                display_label=">=4 years in grade in this appointment (listing July 1)",
                required=4,
                actual=2,
                met=False,
                detail="2/4 years (listing Jul 1, 2026)",
            ),
        ],
    )
    ctx = _minimal_context(
        promote_primary=promote,
        maintain_primary=RequirementEvaluation(
            rule_set_id=2,
            isu_rule_ref="411.1",
            purpose="maintain",
            label="Maintain",
            listing_tier="international",
            season_window=2,
            season_codes=[2425, 2526],
            meets=True,
            summary_note="Meets requirements",
            rule_results=[],
        ),
    )
    pdf = build_appointment_detail_pdf(ctx)
    assert pdf[:4] == b"%PDF"
    assert len(pdf) > 400


def test_build_bulk_appointment_reports_zip(monkeypatch):
    summary = pd.DataFrame(
        [
            {
                "official_id": 1,
                "appointment_type_id": 10,
                "discipline_id": 3,
                "official_name": "Jane Doe",
                "appointment_type": "Judge",
                "discipline": "Singles",
                "appointment_level": "International",
                "competition_count": 1,
                "segment_count": 2,
            },
            {
                "official_id": 2,
                "appointment_type_id": 11,
                "discipline_id": None,
                "official_name": "John Smith",
                "appointment_type": "Referee",
                "discipline": "—",
                "appointment_level": "ISU",
                "competition_count": 0,
                "segment_count": 0,
            },
        ]
    )
    import activityAnalysis.international_officials_report as report_mod

    def fake_context(row, **kwargs):
        return _minimal_context(
            official_id=int(row["official_id"]),
            official_name=row["official_name"],
            appointment_type=row["appointment_type"],
            discipline=row["discipline"],
        )

    monkeypatch.setattr(report_mod, "build_appointment_detail_context", fake_context)
    monkeypatch.setattr(report_mod, "build_appointment_detail_pdf", lambda ctx: b"%PDF-bulk")

    zip_bytes = report_mod.build_bulk_appointment_reports_zip(
        summary,
        listing_season_code=2526,
        report_season_codes=[2425],
        report_season_window=1,
        active_only=True,
        panel_bulk=pd.DataFrame({"official_id": [1, 2]}),
    )
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = sorted(zf.namelist())
    assert names == ["Jane_Doe_Judge_Singles.pdf", "John_Smith_Referee.pdf"]
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        assert zf.read(names[0]) == b"%PDF-bulk"


def test_build_appointment_detail_pdf_requires_fpdf(monkeypatch):
    import activityAnalysis.international_officials_report as report_mod

    monkeypatch.setattr(report_mod, "FPDF", None)
    with pytest.raises(RuntimeError, match="fpdf2"):
        report_mod.build_appointment_detail_pdf(_minimal_context())
