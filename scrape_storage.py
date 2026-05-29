"""
Scrape artifact paths for Load Competition and other database loaders.

On Heroku, set ``USE_GCP=1`` (and ``GCS_CONNECTION`` / ``GCS_PRIVATE_KEY`` for
``setup.sh``) so FSM PDF scratch files go to Google Cloud Storage instead of the
ephemeral dyno disk. ``DYNO`` alone uses ``/tmp`` when ``USE_GCP`` is unset.
"""

from __future__ import annotations

import os
import re

DEFAULT_GCP_RESULTS_FILES_PATH = (
    "skating_orc_reports/gs://skating_orc_reports/Generated/"
)


def scrape_use_gcp() -> bool:
    flag = os.environ.get("USE_GCP", "").strip().lower()
    if flag in ("1", "true", "yes"):
        return True
    return False


def gcp_results_files_path() -> str:
    raw = os.environ.get("GCP_RESULTS_FILES_PATH", DEFAULT_GCP_RESULTS_FILES_PATH)
    folder = (raw or DEFAULT_GCP_RESULTS_FILES_PATH).strip()
    return folder if folder.endswith("/") else f"{folder}/"


def _safe_report_dir(report_name: str) -> str:
    name = (report_name or "").strip() or "competition"
    safe = re.sub(r"[^\w\-.]+", "_", name).strip("._")
    return (safe or "competition")[:120]


def scrape_storage_kwargs_for_load(report_name: str) -> dict:
    """
    Extra ``downloadResults.scrape`` keyword arguments for storage layout.

    Returns ``use_gcp``, and when needed ``excel_folder`` / ``pdf_folder``.
    """
    if scrape_use_gcp():
        folder = gcp_results_files_path()
        safe = _safe_report_dir(report_name)
        return {
            "use_gcp": True,
            "excel_folder": folder,
            "pdf_folder": f"{folder}PDFs/{safe}/",
        }
    if os.environ.get("DYNO"):
        tmp = f"/tmp/judging_scrape/{_safe_report_dir(report_name)}/"
        return {
            "use_gcp": False,
            "excel_folder": tmp,
            "pdf_folder": tmp,
        }
    return {"use_gcp": False}


def scrape_storage_summary() -> str:
    """Short label for UI when Load Competition runs."""
    if scrape_use_gcp():
        return f"Google Cloud Storage (`{gcp_results_files_path()}`)"
    if os.environ.get("DYNO"):
        return "ephemeral `/tmp` on this dyno"
    return "local filesystem (default scrape paths)"
