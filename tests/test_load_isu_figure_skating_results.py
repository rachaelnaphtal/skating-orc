from datetime import date

from scripts.load_isu_figure_skating_results import (
    choose_default_seasons,
    inferred_competition_type_id,
    extract_detailed_results_url,
    is_fsm_results_url,
    is_isu_championship_event,
    is_world_championship_event,
    normalize_results_base_url,
    parse_disciplines_arg,
    parse_event_levels_arg,
    parse_seasons_arg,
    season_year_from_title,
    season_title_from_compact_code,
    seasons_for_calendar_year,
)
from officials_competition_types import competition_load_flags_from_officials_type_id


def test_normalize_results_base_url_strips_index_files():
    assert (
        normalize_results_base_url(
            "https://ijs.usfigureskating.org/leaderboard/results/2025/36369/index.htm"
        )
        == "https://ijs.usfigureskating.org/leaderboard/results/2025/36369"
    )
    assert (
        normalize_results_base_url("https://example.test/results/index.asp")
        == "https://example.test/results"
    )


def test_is_fsm_results_url_only_classic_for_index_asp():
    assert is_fsm_results_url("https://results.isu.org/results/season2526/wc2026/")
    assert is_fsm_results_url("https://example.test/results/index.htm")
    assert not is_fsm_results_url("https://example.test/results/index.asp")


def test_season_year_from_title():
    assert season_year_from_title("2025/2026") == "2526"
    assert season_year_from_title("bad") == ""


def test_season_title_from_compact_code():
    assert season_title_from_compact_code("2526") == "2025/2026"
    assert season_title_from_compact_code("9900") == "1999/2000"
    assert season_title_from_compact_code("2025/2026") == "2025/2026"


def test_parse_seasons_arg_accepts_compact_codes_without_network():
    assert parse_seasons_arg("2526,2425", session=None, timeout=30) == [
        "2025/2026",
        "2024/2025",
    ]


def test_choose_default_seasons_uses_latest_started_seasons():
    seasons = [
        {"title": "2026/2027", "from": "2026-07-01T00:00:00.000Z"},
        {"title": "2025/2026", "from": "2025-07-01T00:00:00.000Z"},
        {"title": "2024/2025", "from": "2024-07-01T00:00:00.000Z"},
        {"title": "2023/2024", "from": "2023-07-01T00:00:00.000Z"},
    ]

    assert choose_default_seasons(seasons, today=date(2026, 5, 29)) == [
        "2025/2026",
        "2024/2025",
    ]


def test_seasons_for_calendar_year_uses_overlapping_isu_seasons():
    seasons = [
        {"title": "2026/2027"},
        {"title": "2025/2026"},
        {"title": "2024/2025"},
        {"title": "2023/2024"},
    ]

    assert seasons_for_calendar_year(seasons, 2025) == [
        "2025/2026",
        "2024/2025",
    ]


def test_parse_disciplines_arg():
    from scripts.load_isu_figure_skating_results import (
        DISCIPLINE_FIGURE_SKATING,
        DISCIPLINE_SYNCHRONIZED_SKATING,
    )

    assert parse_disciplines_arg(None) == (DISCIPLINE_FIGURE_SKATING,)
    assert parse_disciplines_arg("All") == (
        DISCIPLINE_FIGURE_SKATING,
        DISCIPLINE_SYNCHRONIZED_SKATING,
    )
    assert parse_disciplines_arg("synchronized") == (DISCIPLINE_SYNCHRONIZED_SKATING,)
    assert parse_disciplines_arg("figure,synchro") == (
        DISCIPLINE_FIGURE_SKATING,
        DISCIPLINE_SYNCHRONIZED_SKATING,
    )


def test_parse_event_levels_arg():
    assert parse_event_levels_arg(None) == ("ISU",)
    assert parse_event_levels_arg("ISU,International") == ("ISU", "International")
    assert parse_event_levels_arg("All") == ("ISU", "International")


def test_inferred_competition_type_id():
    assert (
        inferred_competition_type_id(
            "International", "Lake Placid International Ice Dance Competition 2025"
        )
        == 17
    )
    assert (
        inferred_competition_type_id("ISU", "ISU Figure Skating World Championships 2026")
        == 15
    )
    assert inferred_competition_type_id("ISU", "ISU Grand Prix Final 2025") == 16
    assert (
        inferred_competition_type_id(
            "ISU",
            "Four Continents Championships 2026",
            "ISU Four Continents Figure Skating Championships",
        )
        == 15
    )
    assert (
        inferred_competition_type_id(
            "ISU",
            "ISU European Figure Skating Championships 2026",
            event_sub_type_name="ISU European Figure Skating Championships",
        )
        == 15
    )
    assert (
        inferred_competition_type_id(
            "ISU",
            "Olympic Winter Games 2026 Figure Skating",
            "Olympic Games",
        )
        == 15
    )


def test_international_competition_type_flags_are_not_domestic_qualifying():
    assert competition_load_flags_from_officials_type_id(15) == (False, False, True)
    assert competition_load_flags_from_officials_type_id(16) == (False, False, True)
    assert competition_load_flags_from_officials_type_id(17) == (False, False, True)


def test_isu_championship_detection_by_subtype():
    assert is_isu_championship_event(
        "Any display name",
        "ISU Four Continents Figure Skating Championships",
    )
    assert is_isu_championship_event(
        "ISU Figure Skating Four Continents Championships 2026",
        "Four Continents Championships",
    )
    assert is_isu_championship_event(
        "ISU Figure Skating World Championships 2026",
        "World Championships",
    )
    assert is_isu_championship_event(
        "ISU Figure Skating European Championships 2026",
        "European Championships",
    )
    assert is_isu_championship_event(
        "ISU Figure Skating Junior World Championships 2026",
        "World Junior Championships",
    )
    assert is_isu_championship_event(
        "Olympic Winter Games 2026 Figure Skating",
        "Olympic Games",
    )
    assert not is_isu_championship_event(
        "ISU Grand Prix Final 2025",
        "ISU Grand Prix",
    )


def test_world_championship_detection_name_fallback():
    assert is_world_championship_event("ISU Figure Skating World Championships 2026")
    assert is_isu_championship_event(
        "ISU Figure Skating World Junior Championships 2026",
        "",
    )


def test_extract_detailed_results_url_from_anchor():
    html = """
    <html><body>
      <a href="https://results.isu.org/results/season2526/jgplat2025/">
        <span>Detailed Results</span>
      </a>
    </body></html>
    """

    assert (
        extract_detailed_results_url(html, "https://isu.org/events/isu-jgp-riga-2025/")
        == "https://results.isu.org/results/season2526/jgplat2025/"
    )


def test_extract_detailed_results_url_from_next_data_fallback():
    html = (
        r'{"detail_result_url":"https:\/\/results.isu.org\/results\/season2526\/wc2026\/",'
        r'"button_1_title":"Watch Highlights"}'
    )

    assert (
        extract_detailed_results_url(html, "https://isu.org/events/isu-world-championships-2026/")
        == "https://results.isu.org/results/season2526/wc2026/"
    )
