"""
Database session for judging analytics (Streamlit app, scripts, workers).

Resolution order:
1. ``DATABASE_URL`` environment variable — unless ``USE_CLOUD_DATABASE=true`` in
   ``st.secrets`` and the env URL points at localhost (local dev override).
2. ``st.secrets`` (``.streamlit/secrets.toml``) when running under Streamlit.
3. ``PGUSER`` / ``PGPASSWORD`` / ``PGHOST`` / ``PGPORT`` / ``PGDATABASE``.

The engine is created lazily so Streamlit secrets are loaded before connecting.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote_plus, urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from models import Base

_engine: Engine | None = None
_SessionLocal: sessionmaker | None = None
_active_url: str | None = None
_active_source: str = "unknown"


def _normalize_database_url(url: str) -> str:
    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if "localhost" not in url and "127.0.0.1" not in url:
        if "sslmode=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}sslmode=require"
    return url


def _is_local_database_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return "localhost" in url or "127.0.0.1" in url
    return host in ("localhost", "127.0.0.1", "")


def _truthy_secret(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def _url_from_connection_section(section: Any) -> str | None:
    if not section:
        return None
    try:
        host = str(section["host"]).strip()
        port = str(section.get("port", "5432")).strip()
        database = str(section["database"]).strip()
        username = str(section["username"]).strip()
        password = str(section["password"]).strip()
    except (KeyError, TypeError, AttributeError):
        return None
    if not host or not database or not username:
        return None
    user = quote_plus(username)
    pwd = quote_plus(password)
    return _normalize_database_url(
        f"postgresql://{user}:{pwd}@{host}:{port}/{database}"
    )


def _streamlit_secrets_available() -> bool:
    try:
        import streamlit as st

        _ = st.secrets
        return True
    except Exception:
        return False


def _use_cloud_database_from_secrets() -> bool:
    if not _streamlit_secrets_available():
        return False
    import streamlit as st

    return _truthy_secret(st.secrets.get("USE_CLOUD_DATABASE", False))


def _database_url_from_streamlit_secrets() -> tuple[str | None, str]:
    if not _streamlit_secrets_available():
        return None, "none"

    import streamlit as st

    use_cloud = _use_cloud_database_from_secrets()

    if use_cloud:
        cloud_url = st.secrets.get("CLOUD_DATABASE_URL")
        if cloud_url:
            return _normalize_database_url(str(cloud_url)), "secrets:CLOUD_DATABASE_URL"
        cloud_conn = st.secrets.get("connections", {}).get("postgresql")
        built = _url_from_connection_section(cloud_conn)
        if built:
            return built, "secrets:connections.postgresql"

    db_url = st.secrets.get("DATABASE_URL")
    if db_url and not use_cloud:
        return _normalize_database_url(str(db_url)), "secrets:DATABASE_URL"

    if not use_cloud:
        local_conn = st.secrets.get("connections", {}).get("postgresql-local")
        built = _url_from_connection_section(local_conn)
        if built:
            return built, "secrets:connections.postgresql-local"

    if not use_cloud and db_url:
        return _normalize_database_url(str(db_url)), "secrets:DATABASE_URL"

    cloud_url = st.secrets.get("CLOUD_DATABASE_URL")
    if cloud_url:
        return _normalize_database_url(str(cloud_url)), "secrets:CLOUD_DATABASE_URL"

    return None, "none"


def resolve_database_url() -> tuple[str, str]:
    """Return ``(url, source_label)``."""
    env_url = (os.getenv("DATABASE_URL") or "").strip()
    secrets_url, secrets_source = _database_url_from_streamlit_secrets()
    use_cloud = _use_cloud_database_from_secrets()

    if use_cloud and secrets_url:
        if not env_url or _is_local_database_url(env_url):
            return secrets_url, secrets_source

    if env_url:
        return _normalize_database_url(env_url), "environment:DATABASE_URL"

    if secrets_url:
        return secrets_url, secrets_source

    pguser = os.getenv("PGUSER", "postgres")
    pgpassword = os.getenv("PGPASSWORD", "")
    pghost = os.getenv("PGHOST", "localhost")
    pgport = os.getenv("PGPORT", "5432")
    pgdatabase = os.getenv("PGDATABASE", "postgres")
    return (
        _normalize_database_url(
            f"postgresql://{quote_plus(pguser)}:{quote_plus(pgpassword)}"
            f"@{pghost}:{pgport}/{pgdatabase}"
        ),
        "environment:PG*",
    )


def _bind_engine(url: str, source: str) -> None:
    global _engine, _SessionLocal, _active_url, _active_source
    if _engine is not None and _active_url == url:
        _active_source = source
        return
    if _engine is not None:
        _engine.dispose()
    _active_url = url
    _active_source = source
    _engine = create_engine(url, echo=False)
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


def ensure_database_for_streamlit() -> str:
    """Resolve URL from secrets/env and (re)create the engine. Call from Streamlit apps."""
    url, source = resolve_database_url()
    _bind_engine(url, source)
    return url


def get_database_url() -> str:
    ensure_database_for_streamlit()
    return _active_url or resolve_database_url()[0]


# Backward compatibility for code that reads this constant after init.
DATABASE_URL: str = ""


def get_db_session():
    """Create and return a database session."""
    if _SessionLocal is None:
        ensure_database_for_streamlit()
    return _SessionLocal()


def test_connection():
    """Test database connection."""
    ensure_database_for_streamlit()
    try:
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            return True
    except Exception as e:
        return False, str(e)
