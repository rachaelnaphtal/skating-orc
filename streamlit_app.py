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
import trialJudgingAnalysis
from trialJudgingAnalysis import process_papers
import gcp_interactions_helper
from gcp_interactions_helper import write_file_to_gcp

# GCP_RESULTS_FILES_PATH="gs://skating_orc_reports/Generated/"
GCP_RESULTS_FILES_PATH = "skating_orc_reports/gs://skating_orc_reports/Generated/"
LOCAL_RESULTS_FILES_PATH = (
    "/Users/rnaphtal/Documents/JudgingAnalysis_Results/Streamlit/"
)
os.environ["GCLOUD_PROJECT"] = "skating-orc"
USE_GCP = True


@st.fragment
def add_download_link_gcp(
    report_name, report_type, extension="xlsx", folder_path=GCP_RESULTS_FILES_PATH
):
    full_file_name = f"{folder_path}{report_name}.{extension}"

    print(f"download file name: {full_file_name}")
    conn = st.connection("gcs", type=FilesConnection)
    with conn.open(full_file_name, mode="rb", ttl=600) as file:
        bytes = file.read()
        btn = st.download_button(
            label=f"Download {report_type}- {report_name}",
            data=bytes,
            file_name=f"{report_name}.{extension}",
            mime="application/vnd.ms-excel",
        )


def make_gui():
    st.title("Judging Analysis Report Generation")
    report_type = st.selectbox(
        "Which type of report do you want?",
        ("Competition ORC Report", "Full Season Report", "Trial Judge Report"),
        index=None,
        key="report_type",
    )

    if report_type == "Competition ORC Report":
        createCompetitionReportLayout()
    if report_type == "Full Season Report":
        createFullSeasonReportLayout()
    if report_type == "Trial Judge Report":
        createTrialJudgeReportLayout()


## Layouts for different options


def createCompetitionReportLayout():
    with st.expander("See more details"):
        st.markdown("""
                    ### Competition ORC Report
    This creates a report to analyze the deviations found in a competition. It is meant to then be used by review captains to decide on which deviations are errors for a given competition.
    There are a couple of options:

    -  **Include only some events**: Do this via an event regex. For example, enter '.\*(Novice|Junior|Senior).\*(Women|Men|Pairs).\*' to just include the Novice and higher Singles/Pairs events at a competiiton.
    -  **Only show rule errors**: In this mode, only GOEs that are mathematically impossible will be shown on the final report.
                    """)
    with st.form("options_form", border=False):
        st.text_input(
            "Report Name (aka the name of the output file)", value="", key="report_name"
        )
        st.text_input(
            "Results URL for competition (for example:https://ijs.usfigureskating.org/leaderboard/results/2025/34240/index.asp).",
            key="url",
        )
        st.text_input(
            "Event Regex. (Optional)",
            value="",
            help="For example, '.\*(Novice|Junior|Senior).\*' will only consider results for Novice, Junior and Senior events.",
            key="event_regex",
        )
        st.checkbox(
            "Include errors only?",
            help="Whether to only include rule errors.",
            key="only_include_errors",
        )
        st.form_submit_button(
            "Generate Report", on_click=generate_full_competition_report
        )


def createFullSeasonReportLayout():
    st.text_input(
        "Report Name (aka the name of the output file)", value="", key="report_name"
    )
    st.text("This report is not fully supported yet.")


def createTrialJudgeReportLayout():
    with st.expander("See more details"):
        st.markdown("""
                    ### Trial Judging Report
    This report creates reports like the Competition ORC reports but for trial judges. For each event you want compared, you will need to upload two things:

    -  The pdf of the results. These should be named with the event abbreviation (for example: "JMFS.pdf").
    -  The Excel trial judge sheet from the event. These need to be named with the event abbreviation and _analysis (ex: "JMFS_analysis.xlsx"). It is important that all skater/team names be exactly as written online. This may require changing them on the event sheet if they are not.
                    """)
    with st.form("options_form", border=False):
        report_name = st.text_input(
            "Report Name (aka the name of the output file)", value="", key="report_name"
        )
        event_files = st.file_uploader(
            "Upload trial judge files",
            type=["xlsx", "pdf"],
            accept_multiple_files=True,
            key="trial_files",
        )
        # event_names = st.text_input("List of event names separated by commas (ex: JMFS,JWSP, SPSP)", value="" , key='event_names')
        # number_tj=st.number_input("Number of Trial Judges", value=1, key='number_tj')
        # tj_names = st.text_input("Trial judge names separated by commas", key="tj_names", help="Include all names even if you only care about some.")
        st.checkbox(
            "Report per TJ?",
            help="Whether to additionally create a report per trial judge.",
            key="report_per_tj",
        )
        submitted = st.form_submit_button(
            "Generate Report", on_click=generate_trial_judge_report
        )


## Validation functions for fields


def validate_trial_judges(num_tjs, tj_names_str):
    if not tj_names_str:
        return False, "Trial Judges names are required"
    tj_names = tj_names_str.split(",")
    if len(tj_names) != num_tjs:
        return False, f"Found {len(tj_names)} names but expected {num_tjs}"
    return True, ""


def validate_trial_judge_files(event_files):
    event_names = []
    file_names = {file.name for file in event_files}
    print(file_names)
    for file in event_files:
        if ".pdf" in file.name:
            event_name = file.name.replace(".pdf", "")
            if f"{event_name}_analysis.xlsx" not in file_names:
                print(f" Looking for '{event_name}_analyis.xlsx'")
                return False, f"Found pdf file for {event_name} without xlsx file."
            event_names.append(file.name.replace(".pdf", ""))
        elif "_analysis.xlsx" in file.name:
            event_name = file.name.replace("_analysis.xlsx", "")
            if f"{event_name}.pdf" not in file_names:
                return False, f"Found xlsx file for {event_name} without pdf file."
        else:
            return False, f"Unexpected file {file.name}"

    st.session_state["event_names"] = event_names
    st.success(f"Found files for events {event_names}")
    return True, ""


def validate_exists(name, field_name):
    if not name:
        return False, f"{field_name} is required"
    return True, ""


def validate_url(name, field_name="URL"):
    if not name:
        return False, f"{field_name} is required"
    if not re.match(r".*[0-9]{4}\/[0-9]{5}\/index.asp$", name):
        return False, f"{field_name} should end with the format 1111/11111/index.asp"
    return True, ""


## Full validation functions for inputs
def validate_competition_report_input():
    report_name = st.session_state["report_name"]
    url = st.session_state["url"]
    validations = [validate_exists(report_name, "Report Name"), validate_url(url)]
    if all(v[0] for v in validations):
        return True
    else:
        # Show all validation errors
        for valid, message in validations:
            if not valid:
                st.error(message)
        return False


def validate_trial_judges_input():
    report_name = st.session_state["report_name"]
    # number_tj = st.session_state['number_tj']
    # tj_names = st.session_state['tj_names']
    event_files = st.session_state["trial_files"]
    # event_names = st.session_state['event_names']
    validations = [
        validate_exists(report_name, "Report Name"),
        validate_trial_judge_files(event_files),
    ]
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
    if not validate_competition_report_input():
        return
    print(st.session_state["report_type"])
    url = st.session_state["url"].replace("/index.asp", "")
    report_name_value = st.session_state["report_name"]
    event_regex = st.session_state["event_regex"]
    only_include_errors = st.session_state["only_include_errors"]
    folder_name = LOCAL_RESULTS_FILES_PATH
    if USE_GCP:
        folder_name = GCP_RESULTS_FILES_PATH
    if event_regex != "":
        downloadResults.scrape(
            url,
            report_name_value,
            event_regex=event_regex,
            excel_folder=folder_name,
            pdf_folder=f"{folder_name}PDFs/",
            use_gcp=USE_GCP,
            only_rule_errors=only_include_errors,
        )
    else:
        downloadResults.scrape(
            url,
            report_name_value,
            excel_folder=folder_name,
            pdf_folder=f"{folder_name}PDFs/",
            use_gcp=USE_GCP,
            only_rule_errors=only_include_errors,
        )

    if USE_GCP:
        print(f"Download coming for {report_name_value}")
        add_download_link_gcp(
            report_name_value, "Competition ORC Report", extension="xlsx"
        )
    else:
        full_report_path = f"{LOCAL_RESULTS_FILES_PATH}{report_name_value}.xlsx"
        with open(full_report_path, "rb") as file:
            btn = st.download_button(
                label=f"Download Competition Summary Report- {report_name_value}",
                data=file,
                file_name=f"{report_name_value}.xlsx",
                mime="application/vnd.ms-excel",
            )


def generate_trial_judge_report():
    if not validate_trial_judges_input():
        return

    report_name_value = st.session_state["report_name"]
    report_name_for_directory = report_name_value.replace(" ", "_")

    # Save all uploaded reports that are expected
    event_files = st.session_state["trial_files"]
    event_names = st.session_state["event_names"]
    base_file_path = f"{GCP_RESULTS_FILES_PATH}{report_name_for_directory}/"
    for file in event_files:
        bytes_data = file.read()
        if USE_GCP:
            file_path = f"{base_file_path}{file.name}"
            print(f"Writing to {file_path}")
            write_file_to_gcp(bytes_data, file_path)
        else:
            print("Local support to come")
    tj_names = trialJudgingAnalysis.get_judges_name_from_sheet(
        f"{base_file_path}{event_names[0]}_analysis.xlsx", use_gcp=USE_GCP
    )
    st.success(f"Found trial judge names {tj_names}")
    excel_path = f"{base_file_path}{report_name_for_directory}.xlsx"
    process_papers(
        event_names,
        excel_path,
        base_file_path,
        judges_names=tj_names,
        use_gcp=USE_GCP,
        include_additional_analysis=True,
    )
    add_download_link_gcp(
        report_name_for_directory,
        "Trial Judge Report",
        extension="xlsx",
        folder_path=base_file_path,
    )
    add_download_link_gcp(
        f"{report_name_for_directory}_AdditionalAnalysis",
        "Trial Judge Report",
        extension="xlsx",
        folder_path=base_file_path,
    )
    if st.session_state["report_per_tj"]:
        for judge in tj_names:
            tj_report_name = f"{report_name_for_directory}_{judge.replace(' ', '_')}"
            excel_path = f"{base_file_path}{tj_report_name}.xlsx"
            process_papers(
                event_names,
                excel_path,
                base_file_path,
                judges_names=tj_names,
                use_gcp=USE_GCP,
                tj_filter=[judge],
            )
            add_download_link_gcp(
                tj_report_name,
                "Trial Judge Report",
                extension="xlsx",
                folder_path=base_file_path,
            )


if __name__ == "__main__":
    make_gui()
