"""Tests for international availability database store helpers."""

from __future__ import annotations

from activityAnalysis.international_availability_store import (
    layout_from_stored_events,
    response_lookup_key,
)
from activityAnalysis.officials_analysis_models import InternationalAvailabilityEvent


def test_response_lookup_key_prefers_email():
    assert response_lookup_key(
        email="Jane.Doe@Example.com",
        first_name="Jane",
        last_name="Doe",
    ) == "email:jane.doe@example.com"


def test_response_lookup_key_falls_back_to_name():
    assert response_lookup_key(
        email=None,
        first_name="Jane",
        last_name="Doe",
    ) == "name:jane doe"


def test_layout_from_stored_events_preserves_order_and_kinds():
    events = [
        InternationalAvailabilityEvent(
            id=1,
            form_id=1,
            prompt_key="JGP China - August 18-22 (Junior Grand Prix Events)",
            short_label="JGP China - August 18-22",
            column_kind="event",
            sort_order=0,
        ),
        InternationalAvailabilityEvent(
            id=2,
            form_id=1,
            prompt_key="Do you have a valid passport?",
            short_label="Valid passport",
            column_kind="supplemental",
            sort_order=1,
        ),
    ]
    layout = layout_from_stored_events(events)
    assert layout["event_cols"] == [
        "JGP China - August 18-22 (Junior Grand Prix Events)"
    ]
    assert layout["supplemental_cols"] == ["Do you have a valid passport?"]
    assert layout["event_short_labels"][
        "JGP China - August 18-22 (Junior Grand Prix Events)"
    ] == "JGP China - August 18-22"
