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
from trialJudgingAnalysis import processPapers
import gcp_interactions_helper
from gcp_interactions_helper import write_file_to_gcp

# GCP_RESULTS_FILES_PATH="gs://skating_orc_reports/Generated/"
GCP_RESULTS_FILES_PATH="skating_orc_reports/gs://skating_orc_reports/Generated/"
LOCAL_RESULTS_FILES_PATH="/Users/rnaphtal/Documents/JudgingAnalysis_Results/Streamlit/"
os.environ["GCLOUD_PROJECT"] = "skating-orc"
USE_GCP=True

@st.fragment
def add_download_link_gcp(report_name, report_type, extension="xlsx", folder_path = GCP_RESULTS_FILES_PATH):
    full_file_name = f"{folder_path}{report_name}.{extension}"

    print(f"download file name: {full_file_name}")
    conn = st.connection('gcs', type=FilesConnection)
    with conn.open(full_file_name, mode="rb", ttl=600) as file:
        bytes= file.read()
        btn = st.download_button(
            label=f"Download {report_type}- {report_name}",
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
    if report_type == "Trial Judge Report":
        createTrialJudgeReportLayout()

## Layouts for different options

def createCompetitionReportLayout():
    with st.form("options_form", border=False):
        st.text_input("Report Name (aka the name of the output file)", value="" , key='report_name')
        st.text_input("Results URL for competition (for example:https://ijs.usfigureskating.org/leaderboard/results/2025/34240/index.asp).", 
                                            key='url')
        st.text_input("Event Regex. (Optional)", value="", help="For example, '.*(Novice|Junior|Senior).*' will only consider results for Novice, Junior and Senior events.", key='event_regex')
        st.checkbox("Include errors only?", help="Whether to only include rule errors.", key='only_include_errors')
        st.form_submit_button("Generate Report", on_click=generate_full_competition_report)

def createFullSeasonReportLayout():
    st.text_input("Report Name (aka the name of the output file)", value="" , key='report_name')
    st.text("This report is not fully supported yet.")

def createTrialJudgeReportLayout():
    st.markdown('''### Trial Judging Report
This report creates reports like the Competition ORC reports but for trial judges. For each event you want compared, you will need to upload two things:

-  The pdf of the results. These should be named with the event abbreviation (for example: "JMFS.pdf").
-  The Excel trial judge sheet from the event. These need to be named with the event abbreviation and _analysis (ex: "JMFS_analysis.xlsx"). It is important that all skater/team names be exactly as written online. This may require changing them on the event sheet if they are not.''')
    with st.form("options_form", border=False):
        report_name = st.text_input("Report Name (aka the name of the output file)", value="" , key='report_name')
        event_files = st.file_uploader("Upload trial judge files", type=['xlsx','pdf'], accept_multiple_files=True, key='trial_files')
        event_names = st.text_input("List of event names separated by commas (ex: JMFS,JWSP, SPSP)", value="" , key='event_names')
        number_tj=st.number_input("Number of Trial Judges", value=1, key='number_tj')
        tj_names = st.text_input("Trial judge names separated by commas", key="tj_names", help="Include all names even if you only care about some.")
        st.checkbox("Report per TJ?", help="Whether to additionally create a report per trial judge.", key='report_per_tj')
        submitted = st.form_submit_button("Generate Report", on_click=generate_trial_judge_report)

## Validation functions for fields 

def validate_trial_judges(num_tjs, tj_names_str):
    if not tj_names_str:
        return False, "Trial Judges names are required"
    tj_names = tj_names_str.split(',')
    if len(tj_names) != num_tjs:
        return False, f"Found {len(tj_names)} names but expected {num_tjs}"
    return True, ""

def validate_trial_judge_files(event_names_str, event_files):
    if not event_names_str:
        return False, "Event names are required"
    event_names = [event_name.strip() for event_name in event_names_str.split(',')]
    file_names = {file.name for file in event_files}
    for event in event_names:
        expected_pdf_name = f"{event}.pdf"
        expected_xlsx_name = f"{event}_analysis.xlsx"
        if expected_pdf_name not in file_names:
            return False, f"Missing expected file {expected_pdf_name}"
        if expected_xlsx_name not in file_names:
            return False, f"Missing expected file {expected_xlsx_name}"
    return True, ""

def validate_exists(name, field_name):
    if not name:
        return False, f"{field_name} is required"
    return True, ""

def validate_url(name, field_name="URL"):
    if not name:
        return False, f"{field_name} is required"
    if not re.match(r'.*[0-9]{4}\/[0-9]{5}\/index.asp$', name):
        return False, f"{field_name} should end with the format 1111/11111/index.asp"
    return True, ""

## Full validation functions for inputs
def validate_competition_report_input():
    report_name = st.session_state['report_name']
    url = st.session_state['url']
    validations=[validate_exists(report_name, "Report Name"),validate_url(url)]
    if all(v[0] for v in validations):
        generate_full_competition_report()
    else:
        # Show all validation errors
        for valid, message in validations:
            if not valid:
                st.error(message)

def validate_trial_judges_input():
    report_name = st.session_state['report_name']
    number_tj = st.session_state['number_tj']
    tj_names = st.session_state['tj_names']
    event_files = st.session_state['trial_files']
    event_names = st.session_state['event_names']
    validations=[validate_exists(report_name, "Report Name"),validate_trial_judges(number_tj, tj_names), validate_trial_judge_files(event_names, event_files)]
    if all(v[0] for v in validations):
        return True
    else:
        # Show all validation errors
        for valid, message in validations:
            if not valid:
                st.error(message)
        return False

## Functions to generate reports    
def generate_full_competition_report():
    if not validate_trial_judges_input():
        return
    print(st.session_state['report_type'])
    url = st.session_state['url'].replace("/index.asp","")
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
        add_download_link_gcp(report_name_value, "Competition ORC Report", extension="xlsx")
    else:
        full_report_path = f"{LOCAL_RESULTS_FILES_PATH}{report_name_value}.xlsx"
        with open(full_report_path, "rb") as file:
            btn = st.download_button(
                label=f"Download Competition Summary Report- {report_name_value}",
                data=file,
                file_name=f"{report_name_value}.xlsx",
                mime="application/vnd.ms-excel"
            )

def generate_trial_judge_report():
    if not validate_trial_judges_input():
        return
    
    report_name_value = st.session_state['report_name']
    report_name_for_directory = report_name_value.replace(" ","_")

    # Save all uploaded reports that are expected
    event_files = st.session_state['trial_files']
    event_names = [event_name.strip() for event_name in st.session_state['event_names'].split(',')]
    expected_file_names=set()
    for event in event_names:
        expected_file_names.add(f"{event}.pdf")
        expected_file_names.add(f"{event}_analysis.xlsx")
    base_file_path = f"{GCP_RESULTS_FILES_PATH}{report_name_for_directory}/"
    for file in event_files:
        if file.name in expected_file_names:
            bytes_data = file.read()
            if USE_GCP:
                file_path = f"{base_file_path}{file.name}"
                print(f"Writing to {file_path}")
                write_file_to_gcp(bytes_data, file_path)
            else:
                print ("Local support to come")
    
    tj_names = [name.strip() for name in st.session_state["tj_names"].split(',')]
    excel_path = f"{base_file_path}{report_name_for_directory}.xlsx"
    processPapers(event_names, excel_path, base_file_path, judges_names=tj_names, use_gcp=USE_GCP)
    add_download_link_gcp(report_name_for_directory,  "Trial Judge Report", extension='xlsx', folder_path=base_file_path)
    if st.session_state["report_per_tj"]:
        for judge in tj_names:
            tj_report_name = f"{report_name_for_directory}_{judge.replace(" ", "_")}"
            excel_path = f"{base_file_path}{tj_report_name}.xlsx"
            processPapers(event_names, excel_path, base_file_path, judges_names=tj_names, use_gcp=USE_GCP, tj_filter=[judge])
            add_download_link_gcp(tj_report_name,  "Trial Judge Report", extension='xlsx', folder_path=base_file_path)
    

if __name__ == '__main__':
    make_gui()