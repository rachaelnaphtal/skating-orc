from activityAnalysis.isu_major_event_classification import (
    MAJOR_ISU_EVENT_EUROPEANS,
    MAJOR_ISU_EVENT_FOUR_CONTINENTS,
    MAJOR_ISU_EVENT_GRAND_PRIX_FINAL,
    MAJOR_ISU_EVENT_JUNIOR_WORLDS,
    MAJOR_ISU_EVENT_OLYMPICS,
    MAJOR_ISU_EVENT_WORLDS,
    classify_isu_major_event,
    competition_matches_major_event,
    year_from_competition_name,
)


def test_classify_worlds():
    assert (
        classify_isu_major_event("ISU Figure Skating World Championships 2026")
        == MAJOR_ISU_EVENT_WORLDS
    )
    assert classify_isu_major_event("Worlds") == MAJOR_ISU_EVENT_WORLDS


def test_classify_junior_worlds():
    assert (
        classify_isu_major_event("2023 World Junior Figure Skating Championships")
        == MAJOR_ISU_EVENT_JUNIOR_WORLDS
    )
    assert (
        classify_isu_major_event("ISU Figure Skating Junior World Championships 2026")
        == MAJOR_ISU_EVENT_JUNIOR_WORLDS
    )
    assert (
        classify_isu_major_event("ISU World Junior Figure Skating Championships")
        == MAJOR_ISU_EVENT_JUNIOR_WORLDS
    )
    assert (
        classify_isu_major_event("World Junior Championships")
        == MAJOR_ISU_EVENT_JUNIOR_WORLDS
    )
    assert (
        classify_isu_major_event("Junior World Championships")
        == MAJOR_ISU_EVENT_JUNIOR_WORLDS
    )


def test_classify_four_continents_and_europeans():
    assert (
        classify_isu_major_event("ISU Four Continents Figure Skating Championships")
        == MAJOR_ISU_EVENT_FOUR_CONTINENTS
    )
    assert (
        classify_isu_major_event("ISU European Figure Skating Championships 2026")
        == MAJOR_ISU_EVENT_EUROPEANS
    )


def test_classify_olympics():
    assert (
        classify_isu_major_event("Olympic Winter Games 2026 Figure Skating")
        == MAJOR_ISU_EVENT_OLYMPICS
    )
    assert classify_isu_major_event("2026 Olympic Winter Games") == MAJOR_ISU_EVENT_OLYMPICS
    assert classify_isu_major_event("Olympic Games") is None
    assert classify_isu_major_event("Youth Olympic Games 2024") is None
    assert (
        classify_isu_major_event("European Youth Olympic Festival 2025") is None
    )


def test_classify_grand_prix_final():
    assert (
        classify_isu_major_event("ISU Grand Prix Final 2025")
        == MAJOR_ISU_EVENT_GRAND_PRIX_FINAL
    )


def test_synchro_worlds_not_classified_for_spd():
    assert classify_isu_major_event("ISU World Synchronized Skating Championships") is None
    assert (
        classify_isu_major_event("Synchronized Skating Junior World Championships")
        is None
    )


def test_gp_series_not_classified_as_final():
    assert classify_isu_major_event("ISU Grand Prix - Cup of China") is None


def test_competition_matches_major_event():
    assert competition_matches_major_event(
        "Four Continents Championships 2026",
        MAJOR_ISU_EVENT_FOUR_CONTINENTS,
    )


def test_year_from_competition_name():
    assert year_from_competition_name("2010 World Figure Skating Championships") == 2010
