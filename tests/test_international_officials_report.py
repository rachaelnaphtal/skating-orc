"""Tests for international officials PDF export."""

from __future__ import annotations

import io
import zipfile

import pandas as pd
import pytest

from activityAnalysis.international_officials_report import (
    AppointmentDetailContext,
    appointment_detail_pdf_filename,
    build_appointment_detail_pdf,
    build_bulk_appointment_reports_zip,
    bulk_reports_zip_filename,
    sanitize_pdf_filename,
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
