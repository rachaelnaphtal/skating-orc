"""Analytics competition scope: ISU events (officials types 15–16)."""

from unittest.mock import MagicMock

from analytics import JudgeAnalytics
from officials_competition_types import (
    COMPETITION_SCOPE_ISU_EVENT,
    OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP,
    OFFICIALS_COMPETITION_TYPE_ID_ISU_COMPETITION,
    OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL_COMPETITION,
)


def test_isu_event_scope_clause_filters_types_15_and_16_only():
    analytics = JudgeAnalytics(MagicMock())
    clause = analytics._competition_scope_clause(COMPETITION_SCOPE_ISU_EVENT)
    assert clause is not None
    compiled = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert str(OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP) in compiled
    assert str(OFFICIALS_COMPETITION_TYPE_ID_ISU_COMPETITION) in compiled
    assert str(OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL_COMPETITION) not in compiled


def test_qualifying_scope_clause_excludes_international_types():
    analytics = JudgeAnalytics(MagicMock())
    from officials_competition_types import (
        COMPETITION_SCOPE_QUALIFYING,
        OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL_COMPETITION,
        OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP,
        OFFICIALS_COMPETITION_TYPE_ID_ISU_COMPETITION,
        OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING,
    )

    clause = analytics._competition_scope_clause(COMPETITION_SCOPE_QUALIFYING)
    assert clause is not None
    compiled = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert str(OFFICIALS_COMPETITION_TYPE_ID_NON_QUALIFYING) in compiled
    assert str(OFFICIALS_COMPETITION_TYPE_ID_ISU_CHAMPIONSHIP) in compiled
    assert str(OFFICIALS_COMPETITION_TYPE_ID_ISU_COMPETITION) in compiled
    assert str(OFFICIALS_COMPETITION_TYPE_ID_INTERNATIONAL_COMPETITION) in compiled


def test_pcs_deviation_scope_labels_include_isu_events():
    from pcs_deviation_analysis import (
        PCS_DEVIATION_COMPETITION_SCOPE_LABELS,
        PCS_DEVIATION_COMPETITION_SCOPES,
        pcs_deviation_competition_scope_key,
    )

    assert "ISU events" in PCS_DEVIATION_COMPETITION_SCOPE_LABELS
    assert COMPETITION_SCOPE_ISU_EVENT in PCS_DEVIATION_COMPETITION_SCOPES
    assert pcs_deviation_competition_scope_key("ISU events") == COMPETITION_SCOPE_ISU_EVENT
