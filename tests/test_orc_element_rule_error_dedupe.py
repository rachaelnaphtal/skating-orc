from judgingParsing import exclude_anomalies_already_flagged_as_rule_errors


def test_rule_error_suppresses_same_element_anomaly():
    rule_errors = [
        {
            "Skater": "Skater A",
            "Element": "3Lz",
            "Judge Number": 1,
            "Description": "Max with errors is 0",
        }
    ]
    deviations = [
        {
            "Skater": "Skater A",
            "Element": "3Lz",
            "Judge Number": 1,
            "Deviation": 2.5,
        },
        {
            "Skater": "Skater A",
            "Element": "3F",
            "Judge Number": 1,
            "Deviation": 2.2,
        },
        {
            "Skater": "Skater A",
            "Element": "3Lz",
            "Judge Number": 2,
            "Deviation": -2.1,
        },
    ]
    kept = exclude_anomalies_already_flagged_as_rule_errors(rule_errors, deviations)
    assert [(r["Element"], r["Judge Number"]) for r in kept] == [
        ("3F", 1),
        ("3Lz", 2),
    ]


def test_pcs_fall_rule_errors_do_not_suppress_goe_anomalies():
    rule_errors = [
        {
            "Skater": "Skater A",
            "Element": "",
            "Component": "Composition",
            "Judge Number": 1,
        }
    ]
    deviations = [
        {
            "Skater": "Skater A",
            "Element": "3Lz",
            "Judge Number": 1,
            "Deviation": 2.5,
        }
    ]
    kept = exclude_anomalies_already_flagged_as_rule_errors(rule_errors, deviations)
    assert kept == deviations
