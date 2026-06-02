import os

from scripts.write_streamlit_secrets import _database_url_from_env


def test_database_url_from_env_prefers_pg_db_url(monkeypatch):
    monkeypatch.setenv("PG_DB_URL", "postgresql://u:p@remote.example.com:5432/db")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@heroku.com:5432/db")
    monkeypatch.setenv("DB_HOST", "localhost")
    assert _database_url_from_env() == "postgresql://u:p@remote.example.com:5432/db"


def test_database_url_from_env_falls_back_to_database_url(monkeypatch):
    monkeypatch.delenv("PG_DB_URL", raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@heroku.com:5432/db")
    assert _database_url_from_env() == "postgresql://u:p@heroku.com:5432/db"


def test_database_url_from_env_builds_from_db_components(monkeypatch):
    monkeypatch.delenv("PG_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "db.example.com")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "mydb")
    monkeypatch.setenv("DB_USERNAME", "user")
    monkeypatch.setenv("DB_PASSWORD", "pass")
    assert (
        _database_url_from_env()
        == "postgresql://user:pass@db.example.com:5432/mydb"
    )
