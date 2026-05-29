"""
Google Cloud service-account credentials for Heroku and local Streamlit.

Heroku config vars often store PEM keys with literal ``\\n`` instead of newlines.
Set either ``GCS_SERVICE_ACCOUNT_JSON`` (full JSON, recommended) or
``GCS_CONNECTION`` + ``GCS_PRIVATE_KEY``.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

_LOG = logging.getLogger(__name__)

_PRIVATE_KEY_JSON_RE = re.compile(
    r'("private_key"\s*:\s*")(.*?(-----END PRIVATE KEY-----)\s*)(")',
    re.DOTALL | re.IGNORECASE,
)


def fix_pem_private_key(key: str | None) -> str:
    """Restore newlines in a PEM copied from JSON or a one-line env var."""
    k = (key or "").strip()
    if not k:
        return ""
    if "\\n" in k:
        k = k.replace("\\n", "\n")
    return k


def _escape_private_key_field_for_json(raw: str) -> str:
    """
    Heroku config vars are sometimes pasted from the key file with real newlines
    inside ``private_key``, which makes ``json.loads`` fail.
    """
    def _repl(match: re.Match[str]) -> str:
        prefix, body, end_marker, suffix = match.groups()
        if "\\n" in body and "\n" not in body.replace("\\n", ""):
            return match.group(0)
        escaped = (
            body.replace("\\", "\\\\")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
            .replace("\n", "\\n")
        )
        return f"{prefix}{escaped}{end_marker}{suffix}"

    return _PRIVATE_KEY_JSON_RE.sub(_repl, raw)


def parse_service_account_json(raw: str) -> dict[str, Any]:
    """
    Parse ``GCS_SERVICE_ACCOUNT_JSON`` from Heroku or local env.

    Raises ``json.JSONDecodeError`` or ``ValueError`` when the payload cannot be parsed.
    """
    text = (raw or "").strip()
    if not text:
        raise ValueError("empty GCS service account JSON")

    candidates = [text, _escape_private_key_field_for_json(text)]
    last_err: json.JSONDecodeError | None = None
    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_err = exc
            continue
        if not isinstance(obj, dict):
            raise ValueError("GCS_SERVICE_ACCOUNT_JSON must be a JSON object")
        pk = obj.get("private_key")
        if isinstance(pk, str):
            obj["private_key"] = fix_pem_private_key(pk)
        return obj
    assert last_err is not None
    raise last_err


def service_account_info_from_env() -> dict[str, Any] | None:
    """
    Build a service-account dict for gcsfs / google-auth from environment variables.

    Prefer ``GCS_SERVICE_ACCOUNT_JSON`` (entire key file as one JSON string).
    """
    raw = (os.environ.get("GCS_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw:
        try:
            return parse_service_account_json(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            _LOG.warning(
                "Ignoring invalid GCS_SERVICE_ACCOUNT_JSON (%s); "
                "use GCS_CONNECTION + GCS_PRIVATE_KEY or fix the config var",
                exc,
            )
            return None
    return None


def gcs_filesystem_from_env():
    """Return a ``gcsfs.GCSFileSystem`` when env credentials are present, else ``None``."""
    info = service_account_info_from_env()
    if not info:
        return None
    import gcsfs

    return gcsfs.GCSFileSystem(token=info)
