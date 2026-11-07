from pytest import approx

from judgingParsing import findPCSDeviations


def _pcs(skater, components):
    return {
        skater: [
            {"Component": name, "Scores": scores} for name, scores in components
        ]
    }


def test_orc_pcs_anomaly_is_one_row_when_signed_sum_exceeds_4_5():
    judges = ["J1", "J2", "J3"]
    # J1 = 9.50 vs 7/7 → +1.666... per component, net +5.0
    rows = findPCSDeviations(
        _pcs(
            "Skater A",
            [
                ("Composition", [9.50, 7.00, 7.00]),
                ("Presentation", [9.50, 7.00, 7.00]),
                ("Skating Skills", [9.50, 7.00, 7.00]),
            ],
        ),
        judges,
        event_name="Women Short Program",
    )
    assert len(rows) == 1
    assert rows[0]["Judge Name"] == "J1"
    assert rows[0]["Skater"] == "Skater A"
    assert rows[0]["Component"] == "All Components"
    assert rows[0]["Deviation"] == approx(5.0)


def test_orc_pcs_opposite_component_deviations_cancel():
    judges = ["J1", "J2", "J3"]
    rows = findPCSDeviations(
        _pcs(
            "Skater A",
            [
                ("Composition", [9.50, 7.00, 7.00]),
                ("Presentation", [4.50, 7.00, 7.00]),
                ("Skating Skills", [7.00, 7.00, 7.00]),
            ],
        ),
        judges,
        event_name="Women Short Program",
    )
    assert rows == []


def test_orc_pcs_exactly_4_5_is_not_an_anomaly():
    judges = ["J1", "J2", "J3"]
    # J1 = 9.25 vs 7/7 → +1.5 per component, net +4.5
    rows = findPCSDeviations(
        _pcs(
            "Skater A",
            [
                ("Composition", [9.25, 7.00, 7.00]),
                ("Presentation", [9.25, 7.00, 7.00]),
                ("Skating Skills", [9.25, 7.00, 7.00]),
            ],
        ),
        judges,
        event_name="Women Short Program",
    )
    assert rows == []


def test_orc_pcs_low_side_net_is_also_an_anomaly():
    judges = ["J1", "J2", "J3"]
    rows = findPCSDeviations(
        _pcs(
            "Skater A",
            [
                ("Composition", [4.50, 7.00, 7.00]),
                ("Presentation", [4.50, 7.00, 7.00]),
                ("Skating Skills", [4.50, 7.00, 7.00]),
            ],
        ),
        judges,
        event_name="Women Short Program",
    )
    assert len(rows) == 1
    assert rows[0]["Judge Name"] == "J1"
    assert rows[0]["Deviation"] == approx(-5.0)
