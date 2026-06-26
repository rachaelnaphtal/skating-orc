from rule_errors_policy import (
    should_flag_pcs_fall_rule_errors,
    segment_is_pairs_for_rule_errors,
    segment_supports_element_rule_errors,
    segment_supports_pcs_fall_rule_errors,
)


def test_segment_supports_isu_fsm_men_short_program():
    assert segment_supports_element_rule_errors("MEN_SHORT_PROGRAM")


def test_segment_supports_isu_fsm_women_free_skating():
    assert segment_supports_element_rule_errors("WOMEN_FREE_SKATING")


def test_segment_supports_olympic_men_single_skating():
    assert segment_supports_element_rule_errors("Men_Single_Skating___Short_Program")


def test_segment_supports_pair_skating_not_pairs_plural():
    assert segment_supports_element_rule_errors("Pair_Skating___Short_Program")


def test_segment_supports_team_event_pair():
    assert segment_supports_element_rule_errors(
        "Team_Event___Pair_Skating___Free_Skating"
    )


def test_segment_does_not_support_ice_dance():
    assert not segment_supports_element_rule_errors("Ice_Dance___Rhythm_Dance")
    assert not segment_supports_element_rule_errors("Junior_Ice_Dance___Free_Dance")


def test_segment_does_not_support_synchronized():
    assert not segment_supports_element_rule_errors(
        "Senior_Synchronized_Skating___Short_Program"
    )


def test_segment_is_pairs_for_pair_skating():
    assert segment_is_pairs_for_rule_errors("Pair_Skating___Short_Program")
    assert not segment_is_pairs_for_rule_errors("MEN_SHORT_PROGRAM")
    assert not segment_is_pairs_for_rule_errors(
        "Team_Event___Men_Single_Skating___Short_Program"
    )


def test_should_flag_pcs_fall_rule_errors_by_season_year():
    assert should_flag_pcs_fall_rule_errors("2425")
    assert should_flag_pcs_fall_rule_errors("2526")
    assert not should_flag_pcs_fall_rule_errors("2324")


def test_should_flag_pcs_fall_rule_errors_falls_back_to_dates():
    assert should_flag_pcs_fall_rule_errors(None, "2024-07-01")
    assert not should_flag_pcs_fall_rule_errors(None, "2024-01-01")


def test_segment_supports_pcs_fall_rule_errors_dance_and_synchro():
    assert segment_supports_pcs_fall_rule_errors("Ice_Dance___Rhythm_Dance")
    assert segment_supports_pcs_fall_rule_errors("Junior_Solo_Dance___Free_Dance")
    assert segment_supports_pcs_fall_rule_errors(
        "Senior_Synchronized_Skating___Short_Program"
    )
    assert segment_supports_pcs_fall_rule_errors("MEN_SHORT_PROGRAM")
    assert not segment_supports_pcs_fall_rule_errors(
        "Senior_Artistic___Free_Skating"
    )
