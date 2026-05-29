import os

import pytest

from scrape_storage import (
    gcp_results_files_path,
    scrape_storage_kwargs_for_load,
    scrape_use_gcp,
)


@pytest.fixture(autouse=True)
def _clear_scrape_storage_env(monkeypatch):
    for key in ("USE_GCP", "DYNO", "GCP_RESULTS_FILES_PATH"):
        monkeypatch.delenv(key, raising=False)


def test_scrape_use_gcp_from_env():
    os.environ["USE_GCP"] = "1"
    assert scrape_use_gcp() is True


def test_scrape_use_gcp_false_without_env():
    assert scrape_use_gcp() is False


def test_gcp_results_files_path_trailing_slash():
    os.environ["GCP_RESULTS_FILES_PATH"] = "bucket/prefix"
    assert gcp_results_files_path() == "bucket/prefix/"


def test_scrape_storage_kwargs_gcp():
    os.environ["USE_GCP"] = "true"
    os.environ["GCP_RESULTS_FILES_PATH"] = "my-bucket/out/"
    kw = scrape_storage_kwargs_for_load("Nepela Memorial 2025")
    assert kw["use_gcp"] is True
    assert kw["excel_folder"] == "my-bucket/out/"
    assert kw["pdf_folder"] == "my-bucket/out/PDFs/Nepela_Memorial_2025/"


def test_scrape_storage_kwargs_heroku_tmp():
    os.environ["DYNO"] = "web.1"
    kw = scrape_storage_kwargs_for_load("Test Event")
    assert kw["use_gcp"] is False
    assert kw["pdf_folder"] == "/tmp/judging_scrape/Test_Event/"
