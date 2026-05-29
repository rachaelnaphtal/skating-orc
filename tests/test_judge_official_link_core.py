from judge_official_link_core import normalize_name, normalize_name_choices


def test_normalize_name_choices_reuses_normalize_name():
    choices = {1: "Chris  Buchanan", 2: "Pat Smith"}
    norm = normalize_name_choices(choices)
    assert norm[1] == normalize_name("Chris  Buchanan")
    assert norm[2] == "pat smith"
