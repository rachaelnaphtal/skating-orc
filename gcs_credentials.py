"""
Google Cloud service-account credentials for Heroku and local Streamlit.

Heroku config vars often store PEM keys with literal ``\\n`` instead of newlines.
Set either ``GCS_SERVICE_ACCOUNT_JSON`` (full JSON, recommended) or
``GCS_CONNECTION`` + ``GCS_PRIVATE_KEY``.
"""

from __future__ import annotations

import json
import os
from typing import Any


def fix_pem_private_key(key: str | None) -> str:
    """Restore newlines in a PEM copied from JSON or a one-line env var."""
    k = (key or "").strip()
    if not k:
        return ""
    if "\\n" in k:
        k = k.replace("\\n", "\n")
    return k


def service_account_info_from_env() -> dict[str, Any] | None:
    """
    Build a service-account dict for gcsfs / google-auth from environment variables.

    Prefer ``GCS_SERVICE_ACCOUNT_JSON`` (entire key file as one JSON string).
    """
    raw = (os.environ.get("GCS_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw:
        info = json.loads(raw)
        if not isinstance(info, dict):
            raise ValueError("GCS_SERVICE_ACCOUNT_JSON must be a JSON object")
        pk = info.get("private_key")
        if isinstance(pk, str):
            info["private_key"] = fix_pem_private_key(pk)
        return info
    return None


def gcs_filesystem_from_env():
    """Return a ``gcsfs.GCSFileSystem`` when env credentials are present, else ``None``."""
    info = service_account_info_from_env()
    if not info:
        return None
    import gcsfs

    return gcsfs.GCSFileSystem(token=info)
