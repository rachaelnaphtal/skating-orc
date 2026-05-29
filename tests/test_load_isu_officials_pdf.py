from scripts.load_isu_officials_pdf import (
    _extract_names_from_line,
    infer_communication_ref,
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
    assert all("JUDGE" not in r.full_name.upper() for r in rows)
    assert all("SKATING" not in r.full_name.upper() for r in rows)


def test_parse_text_handles_name_on_federation_header_line():
    text = "AUT - AUSTRIA Stratieva Aseniya, Ms."

    rows = parse_isu_official_text(text, season="2526", communication_ref="2735")

    assert [(r.federation_code, r.full_name) for r in rows] == [
        ("AUT", "Stratieva Aseniya")
    ]


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
