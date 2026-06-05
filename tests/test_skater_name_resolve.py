from database_loader import (
    _resolve_skater_dict_key,
    _skater_names_equivalent,
)


def test_skater_names_equivalent_case():
    assert _skater_names_equivalent("Rory Beirne", "Rory BEIRNE")


def test_skater_names_equivalent_reordered():
    assert _skater_names_equivalent("Rory Beirne", "Beirne, Rory")


def test_resolve_skater_dict_key():
    skater_dict = {"Rory BEIRNE": 42, "Alex Smith": 7}
    assert _resolve_skater_dict_key(skater_dict, "Rory Beirne") == "Rory BEIRNE"
    assert _resolve_skater_dict_key(skater_dict, "Unknown Skater") is None
