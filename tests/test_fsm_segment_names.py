from judgingParsing import fsm_event_label_from_pdf_lines, ijs_event_label_to_db_segment_name

OWG_HEADER = [
    "Milano Ice Skating Arena Figure Skating",
    "Pattinaggio di figura / Patinage artistique",
    "Men Single Skating",
    "Singolo maschile / Patinage individuel hommes",
    "TUE 10 FEB 2026 Short Program",
    "Programma corto / Programme court",
    "Judges Details per Skater",
    "Fogli di punteggio per pattinatore",
]


def test_fsm_event_label_from_owg_bilingual_pdf_header():
    raw = fsm_event_label_from_pdf_lines(OWG_HEADER)
    assert raw == "Men Single Skating - Short Program"
    assert (
        ijs_event_label_to_db_segment_name(raw)
        == "Men_Single_Skating___Short_Program"
    )


TEAM_MEN_HEADER = [
    "Milano Ice Skating Arena Figure Skating",
    "Team Event",
    "SAT 7 FEB 2026 Men Single Skating - Short Program",
    "Judges Details per Skater",
]


def test_fsm_team_event_pdf_header_includes_discipline():
    raw = fsm_event_label_from_pdf_lines(TEAM_MEN_HEADER)
    assert raw == "Team Event - Men Single Skating - Short Program"
    assert (
        ijs_event_label_to_db_segment_name(raw)
        == "Team_Event___Men_Single_Skating___Short_Program"
    )


WC2016_MEN_SP_HEADER = [
    "ISU World Figure Skating Championships 2016",
    "MEN SHORT PROGRAM JUDGES DETAILS PER SKATER",
    "Starting Total Total Total Total",
    "Rank Name Nation Number Segment Element Program Component Deductions",
    "1 Yuzuru HANYU JPN 110.56 61.52 49.04",
    "Program Components Factor",
    "Skating Skills 1.00 9.75 10.00 9.75 9.50 10.00 10.00 10.00 9.75 10.00 9.89",
]


def test_fsm_event_label_from_2016_combined_header():
    raw = fsm_event_label_from_pdf_lines(WC2016_MEN_SP_HEADER)
    assert raw == "Men - Short Program"
    assert ijs_event_label_to_db_segment_name(raw) == "Men___Short_Program"


def test_fsm_event_label_from_2009_ice_dance_compulsory_and_original():
    cd = [
        "ISU World Figure Skating Championships 2009",
        "ICE DANCE COMPULSORY DANCE JUDGES DETAILS PER SKATER",
    ]
    od = [
        "ISU World Figure Skating Championships 2009",
        "ICE DANCE ORIGINAL DANCE JUDGES DETAILS PER SKATER",
    ]
    assert fsm_event_label_from_pdf_lines(cd) == "Ice Dance - Compulsory Dance"
    assert fsm_event_label_from_pdf_lines(od) == "Ice Dance - Original Dance"
    assert (
        ijs_event_label_to_db_segment_name(fsm_event_label_from_pdf_lines(cd))
        == "Ice_Dance___Compulsory_Dance"
    )


def test_fsm_event_label_from_2016_ice_dance_short_dance_header():
    lines = [
        "ISU World Figure Skating Championships 2016",
        "ICE DANCE SHORT DANCE JUDGES DETAILS PER SKATER",
        "1 Gabriella PAPADAKIS / Guillaume CIZERON FRA 76.29 38.46 37.83",
        "Skating Skills 0.80 8.75 9.75 9.25 9.50 9.75 9.25 9.00 9.50 9.75 9.43",
    ]
    raw = fsm_event_label_from_pdf_lines(lines)
    assert raw == "Ice Dance - Short Dance"
    assert ijs_event_label_to_db_segment_name(raw) == "Ice_Dance___Short_Dance"


WTT_INDEX_HTML = """
<table>
<tr>
  <td></td><td></td>
  <td><a href="FSKXTEAM--------------------------_EntryListbyEvent.pdf">Team Entries (pdf)</a></td>
  <td></td><td></td>
</tr>
<tr>
  <td>Men</td><td>Short Program</td>
  <td><a href="SEG001OF.htm">Panel of Judges</a></td>
  <td><a href="SEG001.htm">Starting Order / Detailed Classification</a></td>
  <td><a href="FSKXTEAM--------------QUAL0001MN--_JudgesDetailsperSkater.pdf">Judges Scores (pdf)</a></td>
</tr>
<tr>
  <td>Women</td><td>Free Skating</td>
  <td><a href="SEG004OF.htm">Panel of Judges</a></td>
  <td><a href="SEG004.htm">Starting Order / Detailed Classification</a></td>
  <td><a href="FSKXTEAM--------------FNL-0002LD--_JudgesDetailsperSkater.pdf">Judges Scores (pdf)</a></td>
</tr>
<tr>
  <td>Ice Dance</td><td>Rhythm Dance</td>
  <td><a href="SEG007OF.htm">Panel of Judges</a></td>
  <td><a href="SEG007.htm">Starting Order / Detailed Classification</a></td>
  <td><a href="FSKXTEAM--------------QUAL0004DC--_JudgesDetailsperSkater.pdf">Judges Scores (pdf)</a></td>
</tr>
</table>
"""


def test_iter_fsm_index_cover_labels_world_team_trophy():
    from downloadResults import iter_fsm_leaderboard_panel_href_and_cover_event

    covers = [
        c for _, c in iter_fsm_leaderboard_panel_href_and_cover_event(WTT_INDEX_HTML)
    ]
    assert covers == [
        "Men - Short Program",
        "Women - Free Skating",
        "Ice Dance - Rhythm Dance",
    ]


def test_iter_fsm_index_cover_labels():
    from downloadResults import iter_fsm_leaderboard_panel_href_and_cover_event, get_page_contents

    html = get_page_contents(
        "https://results.isu.org/results/season2526/owg2026/index.htm"
    )
    if not html:
        return
    covers = [c for _, c in iter_fsm_leaderboard_panel_href_and_cover_event(html)]
    assert "Men Single Skating - Short Program" in covers
    assert "Team Event - Men Single Skating - Short Program" in covers
    assert "Team Event - Women Single Skating - Short Program" in covers
    assert "Team Event - Pair Skating - Short Program" in covers
    assert all(" - " in c for c in covers[:4])
