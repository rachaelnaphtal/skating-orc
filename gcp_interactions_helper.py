from datetime import timedelta
from io import BytesIO
import os
import re
import uuid

from gcs_credentials import gcs_filesystem_from_env, service_account_info_from_env

_REPORT_BUCKET = "skating_orc_reports"

os.environ.setdefault("GCLOUD_PROJECT", "skating-orc")


def _streamlit_gcs_connection():
    """Streamlit + st_files_connection (local dev or when env JSON is not set)."""
    import streamlit as st
    from st_files_connection import FilesConnection

    return st, FilesConnection


def _open_gcs_path(file_name: str, mode: str):
    fs = gcs_filesystem_from_env()
    if fs is not None:
        return fs.open(file_name, mode)
    st, FilesConnection = _streamlit_gcs_connection()
    conn = st.connection("gcs", type=FilesConnection)
    return conn.open(file_name, mode=mode, ttl=600)


def read_file_from_gcp(file_name):
    with _open_gcs_path(file_name, "rb") as file:
        return BytesIO(file.read())


def save_gcp_workbook(workbook, file_name):
    virtual_workbook = BytesIO()
    workbook.save(virtual_workbook)
    workbook_bytes = virtual_workbook.getvalue()
    write_file_to_gcp(workbook_bytes, file_name)


def write_file_to_gcp(bytes, file_name):
    with _open_gcs_path(file_name, "wb") as file:
        file.write(bytes)


def _download_object_filename(filename: str) -> str:
    """Object name safe to put in a Content-Disposition header."""
    base = os.path.basename((filename or "").replace("\\", "/")).strip()
    base = re.sub(r'["\r\n]', "", base)
    return base or "report.xlsx"


def signed_gcs_download_url(
    data: bytes,
    filename: str,
    *,
    content_type: str,
    hours: int = 2,
) -> str:
    """
    Upload ``data`` and return a URL any Heroku dyno can hand to the browser.

    Streamlit download buttons keep the bytes in the memory of whichever dyno
    rendered the button. A later ``GET /media/…`` can land on a different dyno
    and 404. This URL points at Cloud Storage instead.
    """
    info = service_account_info_from_env()
    if not info:
        raise RuntimeError("GCS_SERVICE_ACCOUNT_JSON is not set.")
    from google.cloud import storage
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_info(info)
    client = storage.Client(credentials=credentials, project=info.get("project_id"))
    safe = _download_object_filename(filename)
    blob = client.bucket(_REPORT_BUCKET).blob(
        f"Generated/admin-downloads/{uuid.uuid4().hex}/{safe}"
    )
    blob.upload_from_string(data, content_type=content_type)
    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=hours),
        method="GET",
        scheme="https",
        response_disposition=f'attachment; filename="{safe}"',
    )
