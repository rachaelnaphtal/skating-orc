"""Regression: ISU schema probe must not shadow its own method via instance cache."""

from database_loader import DatabaseLoader


class _FakeResult:
    def first(self):
        return None


class _FakeSession:
    def execute(self, *_args, **_kwargs):
        return _FakeResult()


def test_isu_official_schema_ready_callable_after_cache():
    loader = DatabaseLoader(_FakeSession())
    assert loader._isu_official_schema_ready() is False
    assert loader._isu_official_schema_ready() is False
