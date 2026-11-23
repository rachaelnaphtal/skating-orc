"""ORC level picks use stored segment levels, not a short name list."""

from competition_orc_report import segment_passes_orc_filters
from event_regex_presets import orc_event_filters
from segment_level import (
    event_name_matches_levels,
    sort_segment_levels,
)


def test_level_picker_uses_skating_order_and_drops_unspecified():
    ordered = sort_segment_levels(
        ["Senior", "Unspecified", "Excel Novice", "", "Juvenile", "Club"]
    )
    assert ordered == ["Juvenile", "Senior", "Excel Novice", "Club"]


def test_custom_regex_replaces_the_level_pick():
    pattern, levels = orc_event_filters("(?i).*MEN.*", ["Juvenile"], ["Singles"])
    assert pattern == "(?i).*MEN.*"
    assert levels == ()


def test_discipline_regex_stays_separate_from_levels():
    pattern, levels = orc_event_filters("", ["Juvenile", "Senior"], ["Pairs"])
    assert levels == ("Juvenile", "Senior")
    assert "Juvenile" not in pattern
    assert "Pair" in pattern


def test_stored_level_is_exact_and_blank_level_uses_the_name():
    assert segment_passes_orc_filters(
        "EXCEL_NOVICE_BOYS_SHORT",
        event_regex="",
        levels=["Novice"],
        stored_level="Excel Novice",
    ) is False
    assert segment_passes_orc_filters(
        "EXCEL_NOVICE_BOYS_SHORT",
        event_regex="",
        levels=["Excel Novice"],
        stored_level="Excel Novice",
    )
    assert segment_passes_orc_filters(
        "CHAMPIONSHIP_MEN_SHORT_PROGRAM",
        event_regex="",
        levels=["Senior"],
    )
    assert segment_passes_orc_filters(
        "CHAMPIONSHIP_JUVENILE_GIRLS_FREE_SKATING",
        event_regex="",
        levels=["Senior"],
    ) is False
    assert segment_passes_orc_filters(
        "CHAMPIONSHIP_PRE_JUVENILE_GIRLS_FS_FREE_SKATING",
        event_regex="",
        levels=["Juvenile"],
    ) is False
    assert segment_passes_orc_filters(
        "JUVENILE_WOMEN_FREE_SKATING",
        event_regex="(?i).*(?:Pairs?).*",
        levels=["Juvenile"],
    ) is False


def test_scrape_level_filter_classifies_the_event_name():
    assert event_name_matches_levels("JUNIOR_MEN_SHORT_PROGRAM", ["Junior"])
    assert not event_name_matches_levels("EXCEL_NOVICE_BOYS_SHORT", ["Novice"])
    assert event_name_matches_levels("ANY_EVENT", [])
