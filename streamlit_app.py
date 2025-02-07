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
from io import BytesIO
import re

# GCP_RESULTS_FILES_PATH="gs://skating_orc_reports/Generated/"
GCP_RESULTS_FILES_PATH="skating_orc_reports/gs://skating_orc_reports/Generated/"
LOCAL_RESULTS_FILES_PATH="/Users/rnaphtal/Documents/JudgingAnalysis_Results/Streamlit/"
os.environ["GCLOUD_PROJECT"] = "skating-orc"
USE_GCP=True

def add_download_link_gcp(report_name, extension="xlsx"):
    full_file_name = f"{GCP_RESULTS_FILES_PATH}{report_name}.{extension}"

    conn = st.connection('gcs', type=FilesConnection)
    with conn.open(full_file_name, mode="rb", ttl=600) as file:
        bytes= file.read()
        btn = st.download_button(
            label=f"Download Competition Summary Report- {report_name}",
            data=bytes,
            file_name=f"{report_name}.{extension}",
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
            createCompetitionReportLayout()
        if report_type == "Full Season Report":
            createFullSeasonReportLayout()


def createCompetitionReportLayout():
    with st.form("options_form", border=False):
        report_name = st.text_input("Report Name (aka the name of the output file)", value="" , key='report_name')
        url = st.text_input("Results URL for competition (for example:https://ijs.usfigureskating.org/leaderboard/results/2025/34240/index.asp).", 
                                            key='url')
        st.text_input("Event Regex. (Optional)", value="", help="For example, '.*(Novice|Junior|Senior).*' will only consider results for Novice, Junior and Senior events.", key='event_regex')
        st.checkbox("Include errors only?", help="Whether to only include rule errors.", key='only_include_errors')
        submitted = st.form_submit_button("Generate Report")
        if submitted:
            validations=[validateExists(report_name, "Report Name"),validate_url(url)]
            if all(v[0] for v in validations):
                generate_full_competition_report()
            else:
                        # Show all validation errors
                for valid, message in validations:
                    if not valid:
                        st.error(message)

def createFullSeasonReportLayout():
    report_name = st.text_input("Report Name (aka the name of the output file)", value="" , key='report_name')

def validateExists(name, field_name):
    if not name:
        return False, f"{field_name} is required"
    return True, ""

def validate_url(name, field_name="URL"):
    if not name:
        return False, f"{field_name} is required"
    if not re.match(r'.*[0-9]{4}\/[0-9]{5}\/index.asp$', name):
        return False, f"{field_name} should end with the format 1111/11111/index.asp"
    return True, ""

def generate_full_competition_report():
    print(st.session_state['report_type'])
    url = st.session_state['url_numbers'].replace("/index.asp","")
    report_name_value = st.session_state['report_name']
    event_regex = st.session_state['event_regex']
    only_include_errors = st.session_state['only_include_errors']
    folder_name = LOCAL_RESULTS_FILES_PATH
    if USE_GCP:
        folder_name = GCP_RESULTS_FILES_PATH
    if event_regex != '':
        downloadResults.scrape(url, report_name_value, event_regex=event_regex, excel_folder=folder_name, pdf_folder=f"{folder_name}PDFs/", use_gcp=USE_GCP, only_rule_errors=only_include_errors)
    else:
        downloadResults.scrape(url, report_name_value, excel_folder=folder_name, pdf_folder=f"{folder_name}PDFs/", use_gcp=USE_GCP, only_rule_errors=only_include_errors)
    
    
    if USE_GCP:
        print(f"Download coming for {report_name_value}")
        add_download_link_gcp(report_name_value, extension="xlsx")
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