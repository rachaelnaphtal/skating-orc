#!/usr/bin/env python3
"""
Export ``[connections.gcs]`` from ``.streamlit/secrets.toml`` as one-line JSON for Heroku.

Usage (from repo root):

  python scripts/export_gcs_service_account_json.py > /tmp/gcs-sa.json
  python -c "import json; json.load(open('/tmp/gcs-sa.json'))"  # validate

  heroku config:set GCS_SERVICE_ACCOUNT_JSON="$(cat /tmp/gcs-sa.json)" -a YOUR_APP
  heroku config:set USE_GCP=1 -a YOUR_APP

Do not commit the output file; it contains a private key.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]


def _gcs_section_from_secrets(path: Path) -> dict:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    gcs = (data.get("connections") or {}).get("gcs")
    if not gcs or not isinstance(gcs, dict):
        raise SystemExit(f"No [connections.gcs] in {path}")
    return gcs


def main() -> None:
    secrets_path = Path(__file__).resolve().parents[1] / ".streamlit" / "secrets.toml"
    if len(sys.argv) > 1:
        secrets_path = Path(sys.argv[1]).expanduser().resolve()
    if not secrets_path.is_file():
        raise SystemExit(f"Not found: {secrets_path}")

    info = _gcs_section_from_secrets(secrets_path)
    pk = info.get("private_key")
    if isinstance(pk, str) and "\\n" in pk and "\n" not in pk.replace("\\n", ""):
        # TOML often stores PEM as literal \n — JSON needs the same escaping.
        pass
    elif isinstance(pk, str):
        info["private_key"] = pk.replace("\r\n", "\n").replace("\r", "\n")

    # Compact one line for Heroku config vars.
    sys.stdout.write(json.dumps(info, separators=(",", ":")))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
