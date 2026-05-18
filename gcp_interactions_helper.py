from io import BytesIO
import os

os.environ["GCLOUD_PROJECT"] = "skating-orc"


def _streamlit_gcs_connection():
    """Streamlit + st_files_connection are only needed for GCS upload from apps."""
    import streamlit as st
    from st_files_connection import FilesConnection

    return st, FilesConnection


def read_file_from_gcp(file_name):
    st, FilesConnection = _streamlit_gcs_connection()
    conn = st.connection("gcs", type=FilesConnection)
    with conn.open(file_name, mode="rb", ttl=600) as file:
        return BytesIO(file.read())


def save_gcp_workbook(workbook, file_name):
    virtual_workbook = BytesIO()
    workbook.save(virtual_workbook)
    workbook_bytes = virtual_workbook.getvalue()
    write_file_to_gcp(workbook_bytes, file_name)


def write_file_to_gcp(bytes, file_name):
    st, FilesConnection = _streamlit_gcs_connection()
    conn = st.connection("gcs", type=FilesConnection)
    with conn.open(file_name, mode="wb", ttl=600) as file:
        file.write(bytes)
