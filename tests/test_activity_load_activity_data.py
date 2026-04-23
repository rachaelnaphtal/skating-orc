import os
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["DATABASE_URL"] = "sqlite:////tmp/activity_tracker_tests.db"

import activityAnalysis.load_activity_data as load_activity_data


def test_resolve_database_url_defaults_to_local_sqlite(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert (
        load_activity_data._resolve_database_url()  # pylint: disable=protected-access
        == load_activity_data.DEFAULT_ACTIVITY_DB_URL
    )


def test_resolve_database_url_converts_legacy_postgres_prefix(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/dbname")
    assert load_activity_data._resolve_database_url().startswith(  # pylint: disable=protected-access
        "postgresql://"
    )


def test_build_engine_uses_sqlite_schema_translate(monkeypatch):
    captured = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return SimpleNamespace(url=url, kwargs=kwargs)

    monkeypatch.setattr(load_activity_data, "create_engine", fake_create_engine)
    load_activity_data._build_engine("sqlite:///:memory:")  # pylint: disable=protected-access

    assert captured["url"] == "sqlite:///:memory:"
    assert captured["kwargs"]["execution_options"] == {
        "schema_translate_map": {"officials_analysis": None}
    }


def test_build_engine_uses_postgres_search_path(monkeypatch):
    captured = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return SimpleNamespace(url=url, kwargs=kwargs)

    monkeypatch.setattr(load_activity_data, "create_engine", fake_create_engine)
    load_activity_data._build_engine(  # pylint: disable=protected-access
        "postgresql://user:pass@localhost/db"
    )

    assert captured["url"] == "postgresql://user:pass@localhost/db"
    assert captured["kwargs"]["connect_args"] == {
        "options": "-csearch_path=officials_analysis"
    }
