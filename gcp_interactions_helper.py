from io import BytesIO
import streamlit as st
from st_files_connection import FilesConnection
import os

os.environ["GCLOUD_PROJECT"] = "skating-orc"

def read_file_from_gcp(file_name):
    conn = st.connection('gcs', type=FilesConnection)
    with conn.open(file_name, mode="rb", ttl=600) as file:
        return BytesIO(file.read())
    
def save_gcp_workbook(workbook, file_name):
    # Save the workbook to bytes
    virtual_workbook = BytesIO()
    workbook.save(virtual_workbook)

    # Get the bytes
    workbook_bytes = virtual_workbook.getvalue()
    write_file_to_gcp(workbook_bytes, file_name)

def write_file_to_gcp(bytes, file_name):
    conn = st.connection('gcs', type=FilesConnection)
    with conn.open(file_name, mode="wb", ttl=600) as file:
        bytes= file.write(bytes)