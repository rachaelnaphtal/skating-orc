from judge_official_link_core import (
    normalize_name,
    protocol_person_match_key,
    suggest_matches,
)


def test_protocol_person_match_key_case_insensitive():
    assert protocol_person_match_key("Agita ABELE") == protocol_person_match_key(
        "Agita Abele"
    )


def test_protocol_person_match_key_strips_honorific():
    assert protocol_person_match_key("Ms. Agita ABELE") == "agita abele"


def test_suggest_matches_caps_last_name():
    choices = {42: "Agita Abele"}
    matches = suggest_matches("Agita ABELE", choices, top=1, min_score=88)
    assert matches
    assert matches[0][0] == 42
    assert matches[0][1] >= 92


def test_normalize_name_choices_casefolds_labels():
    norm = {1: normalize_name("Susan LYNCH")}
    assert norm[1] == "susan lynch"
    matches = suggest_matches("Susan Lynch", {1: "Susan LYNCH"}, normalized_choices=norm)
    assert matches[0][1] == 100.0
