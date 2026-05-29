from scripts.load_isu_officials_pdf import (
    PdfLine,
    _extract_names_from_line,
    infer_communication_ref,
    merge_csv_values,
    parse_isu_official_lines,
    parse_isu_official_text,
    split_surname_first_name,
    write_csv,
)


def appointment_tuples(row):
    return {
        (appointment.discipline, appointment.appointment_type, appointment.level)
        for appointment in row.appointments
    }


def test_extract_names_strips_headers_on_same_line():
    assert _extract_names_from_line("REFEREE & JUDGE Andrew Rebecca, Ms.") == [
        "Andrew Rebecca"
    ]
    assert _extract_names_from_line(
        "ISU Judge Alexandre Elizabeth, Ms. International Judge Andrew Rebecca, Ms."
    ) == ["Alexandre Elizabeth", "Andrew Rebecca"]


def test_extract_names_ignores_pure_headers():
    assert _extract_names_from_line("SINGLE & PAIR SKATING") == []
    assert _extract_names_from_line("ISU Judge") == []
    assert _extract_names_from_line("TECHNICAL CONTROLLER - PAIR") == []


def test_parse_text_uses_federation_headers_not_names():
    text = """
Communication No. 2735
AND - ANDORRA
SINGLE & PAIR SKATING
REFEREE & JUDGE
International Judge
Lopez Camara Monica, Ms.
ARM - ARMENIA
SINGLE & PAIR SKATING
SYNCHRONIZED SKATING
REFEREE & JUDGE
REFEREE & JUDGE
International Judge
Vladimirov Vladislav, Mr. ISU Judge
Vladimirov Vladislav, Mr.
"""

    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")

    assert [(r.federation_code, r.full_name) for r in rows] == [
        ("AND", "Camara Monica Lopez"),
        ("ARM", "Vladislav Vladimirov"),
    ]
    assert rows[0].first_name == "Camara Monica"
    assert rows[0].last_name == "Lopez"
    assert rows[0].federation_name == "ANDORRA"
    assert appointment_tuples(rows[0]) == {
        ("Single & Pair Skating", "Judge", "International")
    }
    assert all("JUDGE" not in r.full_name.upper() for r in rows)
    assert all("SKATING" not in r.full_name.upper() for r in rows)


def test_parse_text_handles_name_on_federation_header_line():
    text = "AUT - AUSTRIA Stratieva Aseniya, Ms."

    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")

    assert [(r.federation_code, r.full_name) for r in rows] == [
        ("AUT", "Aseniya Stratieva")
    ]
    assert rows[0].federation_name == "AUSTRIA"


def test_parse_text_keeps_separate_appointment_rows():
    text = """
AUS - AUSTRALIA
SINGLE & PAIR SKATING
ISU Judge
Andrew Rebecca, Ms.
International Referee
Andrew Rebecca, Ms.
"""

    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")

    assert len(rows) == 1
    assert appointment_tuples(rows[0]) == {
        ("Single & Pair Skating", "Judge", "ISU"),
        ("Single & Pair Skating", "Referee", "International"),
    }


def test_write_csv_outputs_one_row_per_appointment(tmp_path):
    text = """
AUS - AUSTRALIA
SINGLE & PAIR SKATING
ISU Judge
Andrew Rebecca, Ms.
International Referee
Andrew Rebecca, Ms.
"""
    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")
    out = tmp_path / "isu.csv"

    write_csv(str(out), rows)

    lines = out.read_text().splitlines()
    assert len(lines) == 3
    assert "discipline,appointment_type,level" in lines[0]
    assert "Single & Pair Skating,Judge,ISU" in lines[1]
    assert "Single & Pair Skating,Referee,International" in lines[2]


def test_geometry_lines_keep_column_context_for_appointments():
    lines = [
        PdfLine(1, 10, 0, "AUS - AUSTRALIA", "AUS", "AUSTRALIA"),
        PdfLine(1, 20, 0, "SINGLE & PAIR SKATING", "AUS", "AUSTRALIA"),
        PdfLine(1, 30, 0, "TECHNICAL SPECIALIST - SINGLE", "AUS", "AUSTRALIA"),
        PdfLine(1, 40, 0, "ISU Technical Specialist", "AUS", "AUSTRALIA"),
        PdfLine(1, 50, 0, "Burley Robyn, Ms.", "AUS", "AUSTRALIA"),
        PdfLine(1, 20, 1, "SYNCHRONIZED SKATING", "AUS", "AUSTRALIA"),
        PdfLine(1, 30, 1, "REFEREE & JUDGE", "AUS", "AUSTRALIA"),
        PdfLine(1, 40, 1, "ISU Referee", "AUS", "AUSTRALIA"),
        PdfLine(1, 50, 1, "Andrew Rebecca, Ms.", "AUS", "AUSTRALIA"),
        PdfLine(1, 60, 1, "ISU Judge", "AUS", "AUSTRALIA"),
        PdfLine(1, 70, 1, "Andrew Rebecca, Ms.", "AUS", "AUSTRALIA"),
    ]

    rows = parse_isu_official_lines(lines, season="2526", communication_ref="2735")
    by_name = {row.full_name: row for row in rows}

    assert appointment_tuples(by_name["Robyn Burley"]) == {
        ("Single & Pair Skating", "Technical Specialist", "ISU")
    }
    assert appointment_tuples(by_name["Rebecca Andrew"]) == {
        ("Synchronized Skating", "Referee", "ISU"),
        ("Synchronized Skating", "Judge", "ISU"),
    }


def test_geometry_same_country_discipline_header_applies_across_columns():
    lines = [
        PdfLine(1, 10, 1, "ICE DANCE", "USA", "UNITED STATES OF AMERICA"),
        PdfLine(2, 72, 0, "SYNCHRONIZED SKATING", "USA", "UNITED STATES OF AMERICA"),
        PdfLine(2, 72, 1, "TECHNICAL CONTROLLER", "USA", "UNITED STATES OF AMERICA"),
        PdfLine(2, 90, 1, "International Technical Controller", "USA", "UNITED STATES OF AMERICA"),
        PdfLine(2, 135, 1, "Sherr Karin, Ms.", "USA", "UNITED STATES OF AMERICA"),
    ]

    rows = parse_isu_official_lines(lines, season="2526", communication_ref="2735")

    assert appointment_tuples(rows[0]) == {
        ("Synchronized Skating", "Technical Controller", "International")
    }


def test_parse_text_extracts_multiple_names_from_one_line():
    text = """
AUS - AUSTRALIA
ISU Judge
Andrew Rebecca, Ms. Caughley Rachel, Ms.
Cunningham, Dion, Mr.
"""

    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")

    assert [r.full_name for r in rows] == [
        "Rebecca Andrew",
        "Rachel Caughley",
        "Dion Cunningham",
    ]
    assert rows[-1].first_name == "Dion"
    assert rows[-1].last_name == "Cunningham"


def test_split_surname_first_name_keeps_lowercase_particles():
    assert split_surname_first_name("du Preez Katherine Evelyn") == (
        "Katherine Evelyn",
        "du Preez",
    )


def test_parsed_rows_use_western_order_full_name():
    rows = parse_isu_official_text(
        "USA - UNITED STATES\nISU Judge\nSherr Karin, Ms.",
        season="2526",
        communication_ref="2735",
    )
    assert rows[0].full_name == "Karin Sherr"
    assert rows[0].first_name == "Karin"
    assert rows[0].last_name == "Sherr"
    assert rows[0].name_normalized == "karin sherr"


def test_infer_communication_ref_from_url():
    assert (
        infer_communication_ref(
            "https://example.test/2735-List-Officials-FS-ID-SYS-2025-26.pdf"
        )
        == "2735"
    )


def test_merge_csv_values_appends_without_duplicates():
    assert merge_csv_values("2526", "2627") == "2526,2627"
    assert merge_csv_values("2526,2627", "2526") == "2526,2627"
    assert merge_csv_values("", "2735") == "2735"


def test_admin_parser_includes_load_isu_pdf_command():
    from scripts import judge_official_admin

    args = judge_official_admin._build_parser().parse_args(
        ["load-isu-pdf", "list-officials.pdf", "--season", "2526", "--dry-run"]
    )

    assert args.command == "load-isu-pdf"
    assert args.source == "list-officials.pdf"
    assert args.season == "2526"
    assert args.dry_run is True


def test_isu_roster_schema_is_canonical_across_seasons():
    import judge_official_link_core

    ddl = judge_official_link_core.DDL_JUDGE_ISU_OFFICIAL_LINK
    assert "UNIQUE (federation_code, name_normalized)" in ddl
    assert "UNIQUE (federation_code, name_normalized, season)" not in ddl
    assert "CREATE TABLE IF NOT EXISTS officials_analysis.isu_official_appointment" in ddl
    assert "UNIQUE (isu_official_id, discipline, appointment_type, level, season)" in ddl