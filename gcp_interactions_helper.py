from io import BytesIO
import os

from gcs_credentials import gcs_filesystem_from_env

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
