import pytest

from sharedJudgingAnalysis import categorizeElement, strip_element_level_suffix


def test_strip_element_level_suffix_legacy_spin_level():
    assert strip_element_level_suffix("CCoSp3") == "CCoSp"
    assert strip_element_level_suffix("FCSp4") == "FCSp"
    assert strip_element_level_suffix("FCSp4V") == "FCSp"


def test_strip_element_level_suffix_position_level_spin_codes():
    assert strip_element_level_suffix("CoSp3p4") == "CoSp"
    assert strip_element_level_suffix("CCoSp3p4") == "CCoSp"
    assert strip_element_level_suffix("CoSp3pB") == "CoSp"
    assert strip_element_level_suffix("CCoSp3p3") == "CCoSp"
    assert strip_element_level_suffix("CSSp1B") == "CSSp"
    assert strip_element_level_suffix("FCSp4B") == "FCSp"
    assert strip_element_level_suffix("FSSp3V1") == "FSSp"
    assert strip_element_level_suffix("CSSp2V1") == "CSSp"
    assert strip_element_level_suffix("FCSSp4") == "FCSSp"


def test_strip_element_level_suffix_non_spin_codes():
    assert strip_element_level_suffix("StSq4") == "StSq"
    assert strip_element_level_suffix("3Lz") == "3Lz"


def test_categorize_element_new_position_level_spins():
    assert categorizeElement("CoSp3p4") == "Spin"
    assert categorizeElement("CCoSp3p4") == "Spin"
    assert categorizeElement("CCoSp3") == "Spin"
    assert categorizeElement("FCSp4V") == "Spin"
    assert categorizeElement("CSSp1") == "Spin"
    assert categorizeElement("CSSp1B") == "Spin"
    assert categorizeElement("FSSp3V1") == "Spin"
    assert categorizeElement("CSSp2V1") == "Spin"
    assert categorizeElement("FCSSp4") == "Spin"
    # Flying + change of foot (FCCSp / FCUSp / FCLSp) with V/B suffixes
    assert categorizeElement("FCCSp2") == "Spin"
    assert categorizeElement("FCCSp2V") == "Spin"
    assert categorizeElement("FCCSp4B") == "Spin"
    assert categorizeElement("FCCSp3V1") == "Spin"
    assert categorizeElement("FCUSp2V") == "Spin"
    assert categorizeElement("FCLSp3B") == "Spin"
    assert categorizeElement("SSpb") == "Spin"
    assert categorizeElement("CCoSpb") == "Spin"
    assert categorizeElement("SSpB") == "Spin"


def test_categorize_element_legacy_and_other_types_unchanged():
    assert categorizeElement("4T+3T") == "Jump"
    assert categorizeElement("StSq4") == "Step Sequence"
    assert categorizeElement("3Lz") == "Jump"
    assert categorizeElement("PCoSp4") == "Pairs Spin"


def test_categorize_element_jumps_without_revolution_prefix():
    assert categorizeElement("F+1Lo") == "Jump"
    assert categorizeElement("A+2T") == "Jump"
    assert categorizeElement("Eu+1Lo") == "Jump"
    assert categorizeElement("1Eu+1F") == "Jump"
    assert categorizeElement("F+1Lo+2T") == "Jump"
    assert categorizeElement("1Lz+1A+SEQ") == "Jump"
    assert categorizeElement("2A+2T+SEQ") == "Jump"
    assert categorizeElement("3T+1A+SEQ") == "Jump"
    assert categorizeElement("2S+COMBO") == "Jump"
    assert categorizeElement("3T+COMBO") == "Jump"
    assert categorizeElement("2Aq") == "Jump"
    assert categorizeElement("2Fq+REP") == "Jump"
    assert categorizeElement("2S+REP") == "Jump"
    assert categorizeElement("1S+1A*+SEQ+REP") == "Jump"
    assert categorizeElement("2F+1A+2Tq+SEQ") == "Jump"
    assert categorizeElement("2Aqb1+REP") == "Jump"
    assert categorizeElement("2Lz<<+REP") == "Jump"
    assert categorizeElement("1HF") == "Jump"
    assert categorizeElement("1HLze") == "Jump"
    assert categorizeElement("1HLz!") == "Jump"


@pytest.mark.parametrize(
    "element,expected",
    [
        # Ice dance — lifts, pattern, twizzles, steps, choreo, dance spin
        ("SlLi4", "Lift"),
        ("RoLi4", "Lift"),
        ("1RW4+kpYYY", "Pattern dance"),
        ("SyTwW4", "Twizzle"),
        ("SqTwW4", "Twizzle"),
        ("CiStW3", "Step Sequence"),
        ("DiStW2", "Step Sequence"),
        ("PStW3", "Step Sequence"),
        ("MiStW1+MiStM1", "Step Sequence"),
        ("DiStW2+DiStM1", "Step Sequence"),
        ("SyTwW4+SyTwMB", "Twizzle"),
        ("CiStW3+CiStM2", "Step Sequence"),
        ("ChSq1", "ChSq"),
        ("pChSq1", "ChSq"),
        ("DSp4", "Spin"),
        ("DSp3", "Spin"),
        ("OFT2", "Step Sequence"),
        ("OFSt3", "Step Sequence"),
        # Synchronized — blocks, edges, twizzles, intersections
        ("SqTw4", "Twizzle"),
        ("SoSqTw3", "Twizzle"),
        ("ChTw4", "Twizzle"),
        ("ME+fm", "Moves Element"),
        ("TrE", "Travelling Element"),
        ("TE", "Twizzle Element"),
        ("Pa", "Pair Element"),
        ("AL", "Artistic"),
        ("Cr", "Creative"),
        ("GL", "Group Lift"),
        ("Co", "Mixed Element"),
        ("Mi", "Mixed Element"),
        ("PB", "Pivoting Block"),
        ("PiF", "Pivot Figure"),
        ("ChSt", "Choreo Element"),
        ("SpEe", "Edge Element"),
        ("SeEe", "Edge Element"),
        ("CrEe", "Edge Element"),
        ("NtMiSt1", "Step Sequence"),
        ("NtDiSt1", "Step Sequence"),
        ("SeSt2", "Step Sequence"),
        ("DiSt3", "Step Sequence"),
        ("MiSt2", "Step Sequence"),
        ("CiSt2", "Step Sequence"),
        ("SoOFSt", "Step Sequence"),
        ("I", "Intersection"),
        ("NHE1", "No Hold Element"),
        # Pairs (non-spin)
        ("FiDs4", "Death Spiral"),
        ("3Tw4", "Twist"),
        ("3Li4", "Lift"),
        # Level suffix on non-spin codes must not be treated as ``3p4`` spin notation
        ("CSSp4", "Spin"),
        ("LSp3", "Spin"),
    ],
)
def test_categorize_element_dance_and_synchro_regression(element, expected):
    assert categorizeElement(element) == expected
