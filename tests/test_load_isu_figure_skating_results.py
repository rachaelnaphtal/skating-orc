from datetime import date

from scripts.load_isu_figure_skating_results import (
    choose_default_seasons,
    extract_detailed_results_url,
    is_fsm_results_url,
    normalize_results_base_url,
    season_year_from_title,
)


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
