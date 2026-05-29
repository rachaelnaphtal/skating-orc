import json
import os

import pytest

from gcs_credentials import (
    fix_pem_private_key,
    parse_service_account_json,
    service_account_info_from_env,
)


@pytest.fixture(autouse=True)
def _clear_gcs_env(monkeypatch):
    monkeypatch.delenv("GCS_SERVICE_ACCOUNT_JSON", raising=False)


def test_fix_pem_private_key_literal_backslash_n():
    one_line = "-----BEGIN PRIVATE KEY-----\\nABC\\n-----END PRIVATE KEY-----\\n"
    fixed = fix_pem_private_key(one_line)
    assert "\n" in fixed
    assert "\\n" not in fixed
    assert fixed.startswith("-----BEGIN PRIVATE KEY-----")


def test_parse_service_account_json_with_real_newlines_in_private_key():
    broken = (
        '{"type":"service_account","project_id":"p","private_key":"-----BEGIN PRIVATE KEY-----\n'
        'line\n-----END PRIVATE KEY-----\n","client_email":"a@b.iam.gserviceaccount.com"}'
    )
    out = parse_service_account_json(broken)
    assert "\n" in out["private_key"]
    assert out["project_id"] == "p"


def test_service_account_info_from_env_invalid_json_returns_none(monkeypatch):
    monkeypatch.setenv("GCS_SERVICE_ACCOUNT_JSON", '{"type": broken}')
    assert service_account_info_from_env() is None


def test_service_account_info_from_env_json():
    info = {
        "type": "service_account",
        "project_id": "p",
        "private_key": "-----BEGIN PRIVATE KEY-----\\nline\\n-----END PRIVATE KEY-----\\n",
        "client_email": "a@b.iam.gserviceaccount.com",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    os.environ["GCS_SERVICE_ACCOUNT_JSON"] = json.dumps(info)
    out = service_account_info_from_env()
    assert out is not None
    assert "\n" in out["private_key"]
    assert "\\n" not in out["private_key"]
