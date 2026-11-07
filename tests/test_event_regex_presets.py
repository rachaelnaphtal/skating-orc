"""Level presets must not treat Championship Juvenile as Senior."""

from event_regex_presets import build_event_regex_from_presets
from competition_orc_report import segment_matches_event_regex


def test_senior_matches_bare_championship_and_not_lower_levels():
    pattern = build_event_regex_from_presets(["Senior"], [])
    assert segment_matches_event_regex("CHAMPIONSHIP_MEN_SHORT_PROGRAM", pattern)
    assert segment_matches_event_regex("CHAMPIONSHIP_ICE_DANCE_FREE_DANCE", pattern)
    assert segment_matches_event_regex("SENIOR_WOMEN_FREE_SKATING", pattern)
    for name in (
        "CHAMPIONSHIP_JUVENILE_GIRLS_FREE_SKATING",
        "CHAMPIONSHIP_INTERMEDIATE_MEN_COMBINED_SHORT_PROGRAM",
        "CHAMPIONSHIP_PRE_JUVENILE_GIRLS_FS_FREE_SKATING",
        "CHAMPIONSHIP_PRELIMINARY_BOYS_FS_FREE_SKATING",
        "CHAMPIONSHIP_JUNIOR_MEN_COMBINED_SHORT_PROGRAM",
        "204_Championship_Pre_Preliminary_Girls_Free_Skate",
        "JUVENILE_PAIRS_FREE_SKATING",
        "INTERMEDIATE_PAIRS_FREE_SKATING",
    ):
        assert not segment_matches_event_regex(name, pattern), name


def test_junior_filter_keeps_only_junior_segments():
    pattern = build_event_regex_from_presets(["Junior"], [])
    assert segment_matches_event_regex(
        "CHAMPIONSHIP_JUNIOR_MEN_COMBINED_SHORT_PROGRAM", pattern
    )
    assert not segment_matches_event_regex(
        "CHAMPIONSHIP_JUVENILE_GIRLS_FREE_SKATING", pattern
    )
