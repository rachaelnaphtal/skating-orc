from scripts.load_isu_officials_pdf import (
    PdfLine,
    _extract_names_from_line,
    infer_communication_ref,
    merge_csv_values,
    parse_isu_official_lines,
    parse_isu_official_text,
    split_surname_first_name,
)


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
        ("AND", "Lopez Camara Monica"),
        ("ARM", "Vladimirov Vladislav"),
    ]
    assert rows[0].federation_name == "ANDORRA"
    assert rows[0].disciplines == "Single & Pair Skating"
    assert rows[0].appointment_types == "Judge"
    assert rows[0].levels == "International"
    assert all("JUDGE" not in r.full_name.upper() for r in rows)
    assert all("SKATING" not in r.full_name.upper() for r in rows)


def test_parse_text_handles_name_on_federation_header_line():
    text = "AUT - AUSTRIA Stratieva Aseniya, Ms."

    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")

    assert [(r.federation_code, r.full_name) for r in rows] == [
        ("AUT", "Stratieva Aseniya")
    ]
    assert rows[0].federation_name == "AUSTRIA"


def test_parse_text_aggregates_levels_and_appointment_types():
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
    assert rows[0].appointment_types == "Judge,Referee"
    assert rows[0].levels == "ISU,International"
    assert rows[0].disciplines == "Single & Pair Skating"


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

    assert by_name["Burley Robyn"].appointment_types == "Technical Specialist"
    assert by_name["Andrew Rebecca"].appointment_types == "Referee,Judge"
    assert "Technical Specialist" not in by_name["Andrew Rebecca"].appointment_types
    assert by_name["Andrew Rebecca"].disciplines == "Synchronized Skating"


def test_parse_text_extracts_multiple_names_from_one_line():
    text = """
AUS - AUSTRALIA
ISU Judge
Andrew Rebecca, Ms. Caughley Rachel, Ms.
Cunningham, Dion, Mr.
"""

    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")

    assert [r.full_name for r in rows] == [
        "Andrew Rebecca",
        "Caughley Rachel",
        "Cunningham, Dion",
    ]
    assert rows[-1].first_name == "Dion"
    assert rows[-1].last_name == "Cunningham"


def test_split_surname_first_name_keeps_lowercase_particles():
    assert split_surname_first_name("du Preez Katherine Evelyn") == (
        "Katherine Evelyn",
        "du Preez",
    )


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
