from ijs_results_urls import (
    competition_index_fetch_url,
    is_legacy_base_results_url,
    results_url_dedupe_key,
    results_url_for_storage,
    results_url_lookup_keys,
    scrape_join_base,
)


def test_fsm_url_gets_index_htm_for_storage():
    assert (
        results_url_for_storage("https://www.isuresults.com/results/wc2007")
        == "https://www.isuresults.com/results/wc2007/index.htm"
    )
    assert (
        results_url_for_storage("https://www.isuresults.com/results/wc2007/index.htm")
        == "https://www.isuresults.com/results/wc2007/index.htm"
    )


def test_classic_usfs_base_gets_index_asp():
    assert (
        results_url_for_storage(
            "https://ijs.usfigureskating.org/leaderboard/results/2025/36369"
        )
        == "https://ijs.usfigureskating.org/leaderboard/results/2025/36369/index.asp"
    )


def test_existing_index_suffixes_unchanged():
    assert (
        results_url_for_storage("https://example.test/results/index.asp")
        == "https://example.test/results/index.asp"
    )
    assert (
        results_url_for_storage(
            "https://ijs.usfigureskating.org/leaderboard/results/2025/36369/index.htm"
        )
        == "https://ijs.usfigureskating.org/leaderboard/results/2025/36369/index.htm"
    )


def test_dedupe_key_matches_with_and_without_fsm_index():
    bare = "https://www.isuresults.com/results/wc2007"
    with_index = "https://www.isuresults.com/results/wc2007/index.htm"
    assert results_url_dedupe_key(bare) == results_url_dedupe_key(with_index)


def test_lookup_keys_include_bare_and_canonical_fsm():
    keys = results_url_lookup_keys("https://www.isuresults.com/results/wc2007")
    assert "https://www.isuresults.com/results/wc2007/index.htm" in keys
    assert "https://www.isuresults.com/results/wc2007" in keys


def test_competition_index_fetch_url_uses_canonical():
    assert (
        competition_index_fetch_url("https://www.isuresults.com/results/wc2007")
        == "https://www.isuresults.com/results/wc2007/index.htm"
    )


def test_scrape_join_base_strips_index_htm():
    stored = "https://www.isuresults.com/results/wc2007/index.htm"
    assert scrape_join_base(stored) == "https://www.isuresults.com/results/wc2007"


def test_is_legacy_base_results_url():
    assert is_legacy_base_results_url(
        "https://ijs.usfigureskating.org/leaderboard/results/2025/36369"
    )
    assert not is_legacy_base_results_url("https://www.isuresults.com/results/wc2007")
