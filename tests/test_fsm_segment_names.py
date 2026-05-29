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
