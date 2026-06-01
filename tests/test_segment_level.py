from segment_level import (
    LEVEL_EXCEL_JUVENILE,
    LEVEL_JUNIOR,
    LEVEL_JUVENILE,
    LEVEL_NOVICE,
    LEVEL_SENIOR,
    LEVEL_UNSPECIFIED,
    SOURCE_COMPETITION_NAME,
    SOURCE_DEFAULT_INTERNATIONAL,
    SOURCE_SEGMENT_TOKEN,
    classify_segment_level,
    normalize_segment_name,
)


def test_segment_token_levels():
    from segment_level import _level_from_segment_tokens

    cases = [
        ("JUNIOR_MEN_SHORT_PROGRAM", LEVEL_JUNIOR),
        ("ADVANCED_NOVICE_WOMEN_FREE_SKATING", "Advanced Novice"),
        ("Men_Single_Skating___Short_Program", None),
        ("115_126_Novice_Women_Grp_B_Short_Program", LEVEL_NOVICE),
        ("Excel_Juvenile_Men_Short_Program", LEVEL_EXCEL_JUVENILE),
        ("Pre_Juvenile_Women_Free", "Pre-Juvenile"),
        (
            "204_Championship_Pre_Preliminary_Girls_Grp_A_Free_Skate",
            "Pre-Preliminary",
        ),
        (
            "213_Excel_Preliminary_Plus_Girls_Grp_A_Free_Skate",
            "Excel Preliminary Plus",
        ),
        (
            "214_Excel_Pre_Juvenile_Plus_Girls_Grp_A_Free_Skate",
            "Excel Pre-Juvenile Plus",
        ),
        ("213_Excel_Preliminary_Girls_Short", "Excel Preliminary"),
        ("Excel_Intermediate_Women_Free", "Excel Intermediate"),
        ("Excel_Novice_Boys_Short", "Excel Novice"),
        ("Excel_Junior_Girls_Free", "Excel Junior"),
        ("Excel_Senior_Men_Free", "Excel Senior"),
        (
            "352___Excel_Pre_Juv._Plus_Free_Skate_Gr_B_Free_Skate",
            "Excel Pre-Juvenile",
        ),
        (
            "9_High_Beginner_Girls_Excel_High_Beginner_Girls_Excel",
            "Excel High Beginner",
        ),
        (
            "07___Excel_Prelimnary_Plus_Free_Skate",
            "Excel Preliminary Plus",
        ),
        (
            "Excel Grp A Beginner Girls Excel Fs",
            "Excel Beginner",
        ),
    ]
    for segment, expected in cases:
        norm = normalize_segment_name(segment)
        assert _level_from_segment_tokens(norm) == expected, segment


def test_international_men_short_grand_prix_is_senior():
    r = classify_segment_level(
        "Men_Single_Skating___Short_Program",
        competition_name="ISU Grand Prix - Cup of China",
        international=True,
    )
    assert r.level == LEVEL_SENIOR
    assert r.source in (SOURCE_COMPETITION_NAME, SOURCE_DEFAULT_INTERNATIONAL)


def test_mixed_age_synchro_not_default_senior():
    r = classify_segment_level(
        "MIXED_AGE_SYNCHRONIZED_SKATING_FREE_SKATING",
        competition_name="Spring Cup 2026",
        international=True,
    )
    assert r.level == "Mixed Age"
    assert r.source == SOURCE_SEGMENT_TOKEN


def test_international_open_competition_defaults_senior():
    r = classify_segment_level(
        "Men_Single_Skating___Short_Program",
        competition_name="Figure Skating Federation Trophy",
        international=True,
    )
    assert r.level == LEVEL_SENIOR
    assert r.source == SOURCE_DEFAULT_INTERNATIONAL


def test_international_jgp_is_junior():
    r = classify_segment_level(
        "Men_Single_Skating___Short_Program",
        competition_name="ISU Junior Grand Prix - Latvia",
        international=True,
    )
    assert r.level == LEVEL_JUNIOR
    assert r.source == SOURCE_COMPETITION_NAME


def test_junior_token_beats_competition():
    r = classify_segment_level(
        "Junior_Women_Free_Skating",
        competition_name="World Championships",
        international=True,
    )
    assert r.level == LEVEL_JUNIOR
    assert r.source == SOURCE_SEGMENT_TOKEN


def test_team_event_olympics_senior():
    r = classify_segment_level(
        "Team_Event___Men_Single_Skating___Short_Program",
        competition_name="Olympic Winter Games 2026",
        international=True,
    )
    assert r.level == LEVEL_SENIOR
    assert r.source == SOURCE_COMPETITION_NAME


def test_elite_aspire_unified_levels():
    from segment_level import _level_from_segment_tokens, normalize_segment_name

    cases = [
        ("13_Elite_12_FS_Free_Skate", LEVEL_SENIOR),
        ("170_Aspire_4_Girls_Free_Skate_Grp_A", "Aspire"),
        ("14_Unified_Teams_Free_Skate", "Unified"),
        ("Skate_United_Teams_Free_Skate", "Unified"),
        ("13_Special_Olympics_Unified_Teams_Level_1_Free_Skate", "Unified"),
        ("1_9___Senior_Elite_12_Teams_Free_Skate", LEVEL_SENIOR),
    ]
    for segment, expected in cases:
        assert _level_from_segment_tokens(normalize_segment_name(segment)) == expected, segment


def test_theatre_on_ice_levels_only_in_theatre_context():
    from segment_level import classify_segment_level

    assert (
        classify_segment_level(
            "12_Open_Free_Skate",
            competition_name="2025 National Theatre On Ice",
        ).level
        == "Open Level"
    )
    assert (
        classify_segment_level(
            "Theatre_Pre_Bronze_Teams_Free_Skate",
            competition_name="Theatre On Ice Classic",
        ).level
        == "Pre Bronze"
    )
    assert (
        classify_segment_level("Theatre_Bronze_Free_Skate", competition_name="").level
        == "Bronze"
    )
    assert (
        classify_segment_level("Theatre_Silver_Free_Skate", competition_name="").level
        == "Silver"
    )
    assert (
        classify_segment_level("Theatre_Pre_Gold_Free_Skate", competition_name="").level
        == "Pre Gold"
    )
    assert (
        classify_segment_level("Theatre_Gold_Free_Skate", competition_name="").level
        == "Gold"
    )
    # Adult bronze is not a theatre metal tier.
    assert classify_segment_level("154_Adult_Bronze_Women_Free_Skate").level == "Adult"
    # Open Level without theatre context → not matched.
    assert (
        classify_segment_level("12_Open_Level_Free_Skate", competition_name="").level
        == "Unspecified"
    )


def test_ice_and_solo_dance_levels():
    from segment_level import (
        DISCIPLINE_TYPE_ID_SOLO_DANCE,
        classify_segment_level,
        _level_from_segment_tokens,
        normalize_segment_name,
    )

    seg = "143_151Bronze_Solo_Pattern_Dance_(SDS)_P2_Ten_Fox_Variation"
    assert classify_segment_level(seg).level == "Bronze"
    assert (
        classify_segment_level(
            "156___Open_Solo_Rocker_Foxtrot_P1",
            discipline_type_id=DISCIPLINE_TYPE_ID_SOLO_DANCE,
        ).level
        == "Open Level"
    )
    assert (
        classify_segment_level(
            "118_122___International_Solo_Pattern_P2_Austrian_Waltz",
            discipline_type_id=DISCIPLINE_TYPE_ID_SOLO_DANCE,
        ).level
        == "International"
    )
    assert (
        classify_segment_level("156___Open_Solo_Rocker_Foxtrot_P1").level == "Open Level"
    )
    assert (
        classify_segment_level(
            "118_122___International_Solo_Pattern_P2_Austrian_Waltz"
        ).level
        == "International"
    )
    assert (
        _level_from_segment_tokens(
            normalize_segment_name("200_210_International_Solo_Pattern_Dance_P1")
        )
        == "International"
    )
    assert (
        _level_from_segment_tokens(
            normalize_segment_name("130_134_Masters_Open_Partnered_Pattern_Dance_P1")
        )
        == "Open Level"
    )
    assert (
        classify_segment_level(
            "Shadow_Dance_Bronze_P1",
            discipline_type_id=DISCIPLINE_TYPE_ID_SOLO_DANCE,
        ).level
        == "Bronze"
    )
    assert (
        classify_segment_level(
            "ICE_DANCE_RHYTHM_DANCE",
            competition_name="ISU Grand Prix",
            international=True,
        ).level
        != "International"
    )
    assert classify_segment_level("12_Open_Collegiate_Teams_Free_Skate").level == "Open Collegiate"


def test_lts_basic_snowplow_sam_beginner():
    from segment_level import _level_from_segment_tokens, classify_segment_level, normalize_segment_name

    assert _level_from_segment_tokens(normalize_segment_name("Snowplow_Sam_1_Free_Skate")) == "LTS"
    assert _level_from_segment_tokens(normalize_segment_name("Basic_Skills_Grp_A")) == "LTS"
    assert _level_from_segment_tokens(normalize_segment_name("Youth_Beginner_Free_Skate")) == "LTS"
    assert (
        classify_segment_level(
            "Grp_A_Free_Skate",
            competition_name="Spring Learn to Skate Snowplow Sam Event",
        ).level
        == "LTS"
    )
    assert _level_from_segment_tokens(normalize_segment_name("BASIC_NOVICE_MEN_FREE_SKATING")) == "Basic Novice"
    assert _level_from_segment_tokens(normalize_segment_name("Excel Grp A Beginner Girls Excel Fs")) == "Excel Beginner"
    assert _level_from_segment_tokens(normalize_segment_name("9_High_Beginner_Girls_Excel_High_Beginner")) == "Excel High Beginner"


def test_domestic_adult_and_open_levels():
    from segment_level import _level_from_segment_tokens, normalize_segment_name

    cases = [
        ("No_Test_FS_Grp_A_Free_Skate", "No Test"),
        ("04___Masters_Teams_Free_Skate", "Masters"),
        ("154_Adult_Bronze_Women_Free_Skate", "Adult"),
        ("09___Collegiate_Teams_Free_Skate", "Collegiate"),
        ("12_Open_Collegiate_Teams_Free_Skate", "Open Collegiate"),
        ("07___Open_Adult_Teams_Free_Skate", "Open Adult"),
        ("11_Open_Masters_Teams_Free_Skate", "Open Masters"),
        ("18_Open_Juvenile_Girls_Free_Skate", "Open Juvenile"),
    ]
    for segment, expected in cases:
        assert _level_from_segment_tokens(normalize_segment_name(segment)) == expected, segment


def test_us_championship_segment_prefix_is_senior():
    r = classify_segment_level(
        "CHAMPIONSHIP_PAIRS_FREE_SKATING",
        competition_name="2025 Prevagen U.S. Figure Skating Championships",
        international=False,
    )
    assert r.level == LEVEL_SENIOR
    assert r.source == SOURCE_SEGMENT_TOKEN


def test_domestic_unspecified_without_token():
    r = classify_segment_level(
        "Some_Local_Event_Short",
        competition_name="Club Competition",
        international=False,
    )
    assert r.level == LEVEL_UNSPECIFIED


def test_pre_preliminary_not_plain_preliminary():
    from segment_level import _level_from_segment_tokens

    norm = normalize_segment_name("204_Championship_Pre_Preliminary_Girls_Free")
    assert _level_from_segment_tokens(norm) == "Pre-Preliminary"
    norm2 = normalize_segment_name("Championship_Preliminary_Girls_Free")
    assert _level_from_segment_tokens(norm2) == "Preliminary"


def test_excel_juvenile_not_plain_juvenile():
    from segment_level import _level_from_segment_tokens

    norm = normalize_segment_name("Excel_Juvenile_Short_Program")
    assert _level_from_segment_tokens(norm) == LEVEL_EXCEL_JUVENILE
    norm2 = normalize_segment_name("Juvenile_Short_Program")
    assert _level_from_segment_tokens(norm2) == LEVEL_JUVENILE


def test_excel_levels_do_not_match_plain_tokens():
    from segment_level import _level_from_segment_tokens, LEVEL_JUNIOR, LEVEL_SENIOR

    assert _level_from_segment_tokens(normalize_segment_name("Excel_Junior_Free")) == "Excel Junior"
    assert _level_from_segment_tokens(normalize_segment_name("Junior_Free")) == LEVEL_JUNIOR
    assert _level_from_segment_tokens(normalize_segment_name("Excel_Senior_Free")) == "Excel Senior"
    assert _level_from_segment_tokens(normalize_segment_name("Senior_Men_Free")) == LEVEL_SENIOR
