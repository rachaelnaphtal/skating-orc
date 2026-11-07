"""USFS results-site schedule parsing and PDF filename matching."""

import json

from judgingParsing import segment_name_match_key
from usfs_results_site import (
    UsfsSegmentSlot,
    _parsed_name_matches,
    already_imported_keys,
    checklist_from_records,
    expected_segment_label,
    match_uploaded_pdfs,
    nuxt_data_map,
    resolve_nuxt_payload,
    season_code_from_iso_date,
    suggest_officials_competition_type_id,
)


def test_season_code_turns_over_in_july():
    assert season_code_from_iso_date("2026-11-03") == "2627"
    assert season_code_from_iso_date("2026-03-01") == "2526"


def test_suggest_officials_type_from_event_name():
    assert (
        suggest_officials_competition_type_id(
            "2027 Eastern Sectional Singles & U.S. Pairs Final",
            "eastern-sectional-singles-us-pairs-final",
        )
        == 1
    )
    assert (
        suggest_officials_competition_type_id(
            "2027 U.S. Figure Skating Championships",
            "us-figure-skating-championships",
        )
        == 4
    )
    assert (
        suggest_officials_competition_type_id(
            "2027 Eastern Synchro Sectional",
            "eastern-synchro-sectional",
        )
        == 5
    )


def test_expected_segment_label_drops_trailing_fs_token():
    assert (
        expected_segment_label("Championship Pre-Juvenile Boys FS", "Free Skating")
        == "Championship Pre-Juvenile Boys Free Skating"
    )
    assert (
        segment_name_match_key(
            expected_segment_label(
                "Championship Intermediate Men Combined", "Short Program"
            )
        )
        == segment_name_match_key(
            "CHAMPIONSHIP_INTERMEDIATE_MEN_COMBINED_SHORT_PROGRAM"
        )
    )


def test_resolve_nuxt_payload_unwraps_references():
    payload = [
        ["ShallowReactive", 1],
        {"data": 2},
        {"event:2026/1": 3},
        {"name": 4, "n": 5},
        "Eastern",
        6,
    ]
    assert resolve_nuxt_payload(payload)["data"]["event:2026/1"] == {
        "name": "Eastern",
        "n": 6,
    }


def test_checklist_keeps_final_segments_and_panel_order():
    event_record = {
        "event": {
            "name": "2027 Eastern Sectional Singles & U.S. Pairs Final",
            "slug": "eastern-sectional-singles-us-pairs-final",
            "city": "Norwood",
            "region": "MA",
            "startDate": "2026-11-03",
            "endDate": "2026-11-07",
        },
        "categories": [
            {
                "id": "championship-intermediate-men-combined",
                "name": "Championship Intermediate Men Combined",
                "level": "Intermediate",
                "discipline": "Singles",
                "segments": [
                    {
                        "code": "SP",
                        "name": "Short Program",
                        "status": "final",
                        "date": "2026-11-05",
                        "time": "16:05",
                    },
                    {
                        "code": "FS",
                        "name": "Free Skating",
                        "status": "upcoming",
                        "date": "2026-11-06",
                        "time": "11:25",
                    },
                ],
            }
        ],
    }
    officials = {
        "championship-intermediate-men-combined": [
            {
                "code": "SP",
                "protocol": {
                    "href": (
                        "assets/protocols/2026/36414/"
                        "championship-intermediate-men-combined-sp-judges-scores.pdf"
                    )
                },
                "officials": [
                    {"role": "Referee", "name": "Ms. Lauren O'TOOLE"},
                    {"role": "Judge No.2", "name": "Mr. Sebastien PAYANNET"},
                    {"role": "Judge No.1", "name": "Ms. Danielle HARTSELL MINNIS"},
                    {"role": "Technical Specialist", "name": "Mr. Steven BELANGER"},
                    {"role": "Technical Specialist", "name": "Ms. Sally WATKINS"},
                ],
            }
        ]
    }
    checklist = checklist_from_records(
        "https://results.usfigureskating.org/events/2026/36414-eastern-sectional-singles-us-pairs-final",
        event_record,
        officials,
    )
    assert checklist.season_code == "2627"
    assert checklist.location == "Norwood, MA"
    assert checklist.suggested_type_id == 1
    assert len(checklist.segments) == 1
    slot = checklist.segments[0]
    assert slot.pdf_filename == (
        "championship-intermediate-men-combined-sp-judges-scores.pdf"
    )
    assert slot.judges == ["Danielle HARTSELL MINNIS", "Sebastien PAYANNET"]
    roles = [row["role"] for row in slot.officials]
    assert "Technical Specialist (2)" in roles
    assert slot.officials[0]["name"] == "Lauren O'TOOLE"


def test_missing_protocol_href_uses_category_and_segment_code():
    event_record = {
        "event": {
            "name": "Event",
            "slug": "event",
            "city": "",
            "region": "",
            "startDate": "2026-11-03",
            "endDate": "2026-11-03",
        },
        "categories": [
            {
                "id": "juvenile-pairs",
                "name": "Juvenile Pairs",
                "level": "Juvenile",
                "discipline": "Pairs",
                "segments": [
                    {
                        "code": "FS",
                        "name": "Free Skating",
                        "status": "final",
                        "date": "2026-11-05",
                        "time": "10:55",
                    }
                ],
            }
        ],
    }
    checklist = checklist_from_records(
        "https://results.usfigureskating.org/events/2026/1-event",
        event_record,
        {"juvenile-pairs": [{"code": "FS", "protocol": None, "officials": []}]},
    )
    assert checklist.segments[0].pdf_filename == "juvenile-pairs-fs-judges-scores.pdf"
    assert checklist.segments[0].officials_error == ""
    assert checklist.segments[0].judges == []


def test_parsed_pdf_header_matches_slot_with_extra_fs_token():
    slot = UsfsSegmentSlot(
        category_name="Championship Preliminary Boys FS",
        category_id="championship-preliminary-boys-fs",
        segment_name="Free Skating",
        segment_code="FS",
        level="Preliminary",
        discipline="Singles",
        date="2026-11-05",
        time="09:00",
        pdf_filename="championship-preliminary-boys-fs-fs-judges-scores.pdf",
        judges=["A"],
        officials=[],
    )
    assert _parsed_name_matches(
        "CHAMPIONSHIP_PRELIMINARY_BOYS_FS_FREE_SKATING", slot
    )
    assert _parsed_name_matches(
        "CHAMPIONSHIP_PRELIMINARY_BOYS_FREE_SKATING", slot
    )
    assert not _parsed_name_matches(
        "CHAMPIONSHIP_PRELIMINARY_BOYS_SHORT_PROGRAM", slot
    )


def test_already_imported_keys_match_stored_protocol_names():
    loaded = UsfsSegmentSlot(
        category_name="Championship Intermediate Men Combined",
        category_id="championship-intermediate-men-combined",
        segment_name="Short Program",
        segment_code="SP",
        level="Intermediate",
        discipline="Singles",
        date="2026-11-05",
        time="16:05",
        pdf_filename="championship-intermediate-men-combined-sp-judges-scores.pdf",
        judges=["A"],
        officials=[],
    )
    waiting = UsfsSegmentSlot(
        category_name="Championship Juvenile Girls",
        category_id="championship-juvenile-girls",
        segment_name="Free Skating",
        segment_code="FS",
        level="Juvenile",
        discipline="Singles",
        date="2026-11-05",
        time="09:00",
        pdf_filename="championship-juvenile-girls-fs-judges-scores.pdf",
        judges=["A"],
        officials=[],
    )
    keys = already_imported_keys(
        [loaded, waiting],
        ["CHAMPIONSHIP_INTERMEDIATE_MEN_COMBINED_SHORT_PROGRAM"],
    )
    assert keys == {("championship-intermediate-men-combined", "SP")}


def test_match_uploaded_pdfs_by_basename():
    slot = UsfsSegmentSlot(
        category_name="Championship Juvenile Girls",
        category_id="championship-juvenile-girls",
        segment_name="Free Skating",
        segment_code="FS",
        level="Juvenile",
        discipline="Singles",
        date="2026-11-05",
        time="09:00",
        pdf_filename="championship-juvenile-girls-fs-judges-scores.pdf",
        judges=["Sebastien PAYANNET"],
        officials=[],
    )
    matched, unmatched = match_uploaded_pdfs(
        [slot],
        [
            "folder/championship-juvenile-girls-fs-judges-scores.PDF",
            "notes.pdf",
            "championship-juvenile-girls-fs-judges-scores.pdf",
        ],
    )
    assert list(matched) == ["championship-juvenile-girls-fs-judges-scores.PDF"]
    assert matched["championship-juvenile-girls-fs-judges-scores.PDF"] is slot
    assert unmatched == [
        "notes.pdf",
        "championship-juvenile-girls-fs-judges-scores.pdf",
    ]


def test_nuxt_data_map_reads_embedded_payload():
    payload = [
        ["ShallowReactive", 1],
        {"data": 2, "state": 3},
        {"event:2026/9": 4},
        None,
        {
            "event": 5,
            "categories": 6,
        },
        {"name": 7, "slug": 8, "city": 9, "region": 9, "startDate": 10, "endDate": 10},
        [],
        "Name",
        "slug",
        "",
        "2026-11-03",
    ]
    html = (
        '<html><script type="application/json" id="__NUXT_DATA__">'
        + json.dumps(payload)
        + "</script></html>"
    )
    data = nuxt_data_map(html)
    assert data["event:2026/9"]["event"]["name"] == "Name"
