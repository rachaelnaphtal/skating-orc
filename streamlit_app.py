import streamlit as st
import asyncio
from io import BytesIO

import downloadResults
import openpyxl
from openpyxl import load_workbook

RESULTS_FILES_PATH="./Generated/"

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
    if event_regex != '':
        downloadResults.scrape(url, report_name_value, event_regex=event_regex, excel_folder=RESULTS_FILES_PATH, pdf_folder=RESULTS_FILES_PATH)
    else:
        downloadResults.scrape(url, report_name_value, excel_folder=RESULTS_FILES_PATH, pdf_folder=RESULTS_FILES_PATH)
    
    full_report_path = f"{RESULTS_FILES_PATH}{report_name_value}.xlsx"
    with open(full_report_path, "rb") as file:
        btn = st.download_button(
            label=f"Download Competition Summary Report- {report_name_value}",
            data=file,
            file_name=f"{report_name_value}.xlsx",
            mime="application/vnd.ms-excel"
        )

if __name__ == '__main__':
    make_gui()