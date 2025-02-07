import streamlit as st
import asyncio
from io import BytesIO

import downloadResults
import openpyxl
from openpyxl import load_workbook
from st_files_connection import FilesConnection
from google.cloud import storage
import os
import gcsfs

GCP_RESULTS_FILES_PATH="gs://skating_orc_reports/Generated/"
LOCAL_RESULTS_FILES_PATH="/Users/rnaphtal/Documents/JudgingAnalysis_Results/Streamlit/"
os.environ["GCLOUD_PROJECT"] = "skating-orc"
USE_GCP=True

def add_download_link_gcp(report_name):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    bucket_name = "skating_orc_reports"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{GCP_RESULTS_FILES_PATH}{report_name}.xlsx")

    # Download the blob into memory as bytes
    contents = blob.download_as_bytes()
    btn = st.download_button(
        label=f"Download Competition Summary Report- {report_name}",
        data=contents,
        file_name=f"{report_name}.xlsx",
        mime="application/vnd.ms-excel"
    )

def make_gui():
    st.title("Judging Analysis Report Generation")
    report_type = st.selectbox(
        "Which type of report do you want?",
        ("Competition ORC Report", "Full Season Report", "Trial Judge Report"), index=None, 
         key='report_type'
        )
    
    if report_type == "Competition ORC Report":
        report_name = st.text_input("Report Name", value="" , key='report_name')
        url_numbers = st.text_input("Numbers at the end of results URL.", help="For example, if the URL is Competition Results Number URL (ending in the number). For example: https://ijs.usfigureskating.org/leaderboard/results/2025/34240 then enter 2025/34240", key='url_numbers')
        event_regex = st.text_input("Event Regex.", value="", help="For example, '.*(Novice|Junior|Senior).*' will only consider results for Novice, Junior and Senior events.", key='event_regex')
        st.button("Generate Report", on_click=generate_full_competition_report)

def generate_full_competition_report():
    print(st.session_state['report_type'])
    url = f"https://ijs.usfigureskating.org/leaderboard/results/{st.session_state['url_numbers']}"
    report_name_value = st.session_state['report_name']
    event_regex = st.session_state['event_regex']
    folder_name = LOCAL_RESULTS_FILES_PATH
    if USE_GCP:
        folder_name = GCP_RESULTS_FILES_PATH
    if event_regex != '':
        downloadResults.scrape(url, report_name_value, event_regex=event_regex, excel_folder=folder_name, pdf_folder=f"{folder_name}PDFs/", use_gcp=USE_GCP)
    else:
        downloadResults.scrape(url, report_name_value, excel_folder=folder_name, pdf_folder=f"{folder_name}PDFs/", use_gcp=USE_GCP)
    
    
    if USE_GCP:
        add_download_link_gcp(report_name_value)
    else:
        full_report_path = f"{LOCAL_RESULTS_FILES_PATH}{report_name_value}.xlsx"
        with open(full_report_path, "rb") as file:
            btn = st.download_button(
                label=f"Download Competition Summary Report- {report_name_value}",
                data=file,
                file_name=f"{report_name_value}.xlsx",
                mime="application/vnd.ms-excel"
            )

if __name__ == '__main__':
    make_gui()