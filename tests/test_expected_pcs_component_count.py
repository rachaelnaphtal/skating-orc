from judgingParsing import expected_pcs_component_count


def test_standard_segments_expect_three_pcs():
    assert expected_pcs_component_count("Junior_Women_Short_Program") == 3
    assert expected_pcs_component_count("Senior Men Free Skating") == 3
    assert expected_pcs_component_count("Ice Dance Rhythm Dance") == 3


def test_compulsory_moves_and_athlete_dev_expect_two_pcs():
    assert (
        expected_pcs_component_count(
            "114_Level_3_Compulsory_Moves_Girl_B_Short_Program"
        )
        == 2
    )
    assert expected_pcs_component_count("Level 3 Compulsory Moves") == 2
    assert expected_pcs_component_count("Preliminary Jump Event") == 2
    assert expected_pcs_component_count("Excel Spin Challenge") == 2
    assert expected_pcs_component_count("Athlete Development Free") == 2


def test_compulsory_dance_still_expects_three_pcs():
    assert expected_pcs_component_count("Ice_Dance___Compulsory_Dance") == 3
