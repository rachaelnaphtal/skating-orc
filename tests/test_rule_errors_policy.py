from rule_errors_policy import (
    segment_is_pairs_for_rule_errors,
    segment_supports_element_rule_errors,
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
