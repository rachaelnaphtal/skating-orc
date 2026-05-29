#!/usr/bin/env python3
"""
Write ``~/.streamlit/secrets.toml`` for Heroku (invoked from ``setup.sh``).

Fixes PEM newlines for ``[connections.gcs]``. Prefer ``GCS_SERVICE_ACCOUNT_JSON``.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from gcs_credentials import fix_pem_private_key, service_account_info_from_env


def _toml_quote(value: str) -> str:
    return json.dumps(value)


def _append_gcs_section(lines: list[str]) -> None:
    lines.append("")
    lines.append("[connections.gcs]")
    info = service_account_info_from_env()
    if info:
        fields = (
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
            "auth_uri",
            "token_uri",
            "auth_provider_x509_cert_url",
            "client_x509_cert_url",
        )
        for key in fields:
            if key not in info:
                continue
            val = info[key]
            if key == "private_key":
                pem = fix_pem_private_key(str(val))
                lines.append('private_key = """')
                lines.extend(pem.splitlines())
                lines.append('"""')
            else:
                lines.append(f"{key} = {_toml_quote(str(val))}")
        for key, val in sorted(info.items()):
            if key in fields or val is None:
                continue
            lines.append(f"{key} = {_toml_quote(str(val))}")
        return

    conn = (os.environ.get("GCS_CONNECTION") or "").strip()
    for line in conn.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("private_key"):
            continue
        lines.append(stripped)

    pem = fix_pem_private_key(os.environ.get("GCS_PRIVATE_KEY"))
    if pem:
        lines.append('private_key = """')
        lines.extend(pem.splitlines())
        lines.append('"""')


def main() -> None:
    streamlit_dir = Path.home() / ".streamlit"
    streamlit_dir.mkdir(parents=True, exist_ok=True)
    out = streamlit_dir / "secrets.toml"

    lines: list[str] = []
    pg_url = os.environ.get("PG_DB_URL", "")
    lines.append(f'DATABASE_URL = {_toml_quote(pg_url)}')
    lines.append("")
    lines.append("[connections.postgresql]")
    lines.append(f'dialect = {_toml_quote("postgresql")}')
    lines.append(f'host = {_toml_quote(os.environ.get("DB_HOST", ""))}')
    lines.append(f'port = {_toml_quote(os.environ.get("DB_PORT", ""))}')
    lines.append(f'database = {_toml_quote(os.environ.get("DB_NAME", ""))}')
    lines.append(f'username = {_toml_quote(os.environ.get("DB_USERNAME", ""))}')
    lines.append(f'password = {_toml_quote(os.environ.get("DB_PASSWORD", ""))}')
    _append_gcs_section(lines)
    lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
