"""Judge name deduplication for ``DatabaseLoader._ensure_judges_by_name``."""

from __future__ import annotations

from types import SimpleNamespace

from database_loader import (
    DatabaseLoader,
    judge_person_match_key,
    select_canonical_judge_ids_per_match_key,
)


def test_judge_person_match_key_case_insensitive():
    assert judge_person_match_key("Karen Wolanchuk") == judge_person_match_key(
        "Karen WOLANCHUK"
    )


def test_select_canonical_judge_prefers_most_scores():
    candidates = {"karen wolanchuk": [341, 2784]}
    score_counts = {341: 666, 2784: 0}
    picked = select_canonical_judge_ids_per_match_key(candidates, score_counts)
    assert picked["karen wolanchuk"] == 341


def test_select_canonical_judge_tiebreak_lowest_id():
    candidates = {"jane doe": [10, 20]}
    score_counts = {10: 5, 20: 5}
    picked = select_canonical_judge_ids_per_match_key(candidates, score_counts)
    assert picked["jane doe"] == 10


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, judge_rows: list[tuple[int, str, str]], score_counts: dict[int, int]):
        self.judge_rows = judge_rows
        self.score_counts = score_counts
        self.added: list[object] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM judge" in sql and "element_score_per_judge" not in sql:
            keys = set((params or {}).get("keys") or [])
            rows = [
                SimpleNamespace(id=i, name=n, match_key=mk)
                for i, n, mk in self.judge_rows
                if mk in keys or not keys
            ]
            return _FakeResult(rows)
        ids = (params or {}).get("ids") or []
        rows = []
        for jid in ids:
            total = int(self.score_counts.get(int(jid), 0))
            if total:
                rows.append(SimpleNamespace(judge_id=int(jid), total=total))
        return _FakeResult(rows)

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        return None


def test_ensure_judges_by_name_picks_scored_duplicate():
    session = _FakeSession(
        [(341, "Karen Wolanchuk", "karen wolanchuk"), (2784, "Karen WOLANCHUK", "karen wolanchuk")],
        {341: 100, 2784: 0},
    )
    loader = DatabaseLoader(session)
    by_name = loader._ensure_judges_by_name(["Karen Wolanchuk"])
    assert by_name["Karen Wolanchuk"] == 341
    assert session.added == []
