import openpyxl.worksheet
import pdfplumber
from pypdf import PdfReader
import pandas as pd
import re


import asyncio
from pyppeteer import launch
import time

import openpyxl
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font, Color
from openpyxl.worksheet.datavalidation import DataValidation

import judgingParsing
from judgingParsing import parse_scores
from judgingParsing import printToExcel
import downloadResults
from downloadResults import make_competition_summary_page
from gcp_interactions_helper import read_file_from_gcp

def processTrialJudgeSheet(pdf_path, num_judges=7, use_gcp=False, judge_names=[]):
    if use_gcp:
        workbook = openpyxl.load_workbook(read_file_from_gcp(pdf_path), data_only=True)
    else:
        workbook = openpyxl.load_workbook(pdf_path, data_only=True) 
    tj_scores = {}
    pcs_scores = {}
    for sheet in workbook:
        if not re.match(r"^(\d+)", sheet.title):
            continue
        skater_name = sheet.cell(row= 2, column=2,).value
        if skater_name is None or skater_name ==0:
            continue
        tj_scores[skater_name] = {}
        pcs_scores[skater_name] = {}
        for judge_number in range(num_judges):
            judge_name = sheet.cell(row = 4+judge_number, column = 2).value
            if (judge_name not in judge_names):
                raise Exception(f"Judge '{judge_name}' found in sheet but is not listed in names {judge_names}")
            judge_scores = []
            for i in range(13):
                score = sheet.cell(row = 4+judge_number, column = 2+i).value
                if score is None:
                    break
                judge_scores.append(score)
            tj_scores[skater_name][judge_name] = judge_scores
            pcs_scores_list=[]
            for i in range(3):
                score = sheet.cell(row = 4+judge_number, column = 16+i).value
                pcs_scores_list.append(score)
            pcs_scores[skater_name][judge_name] = pcs_scores_list
    return tj_scores, pcs_scores

def analyzeTrialJudges(tj_pdf_path, workbook, pdf_path, base_excel_path,judges, pdf_number, tj_filter, use_gcp=False):
    if len(tj_filter) == 0:
        tj_filter= judges
    (elements_per_skater, pcs_per_skater, skater_details, event_name) = parse_scores(pdf_path, use_gcp=use_gcp)
    tj_scores, pcs_scores = processTrialJudgeSheet(tj_pdf_path, num_judges=len(judges), use_gcp=use_gcp, judge_names=judges)
    element_errors = []
    for skater in tj_scores:
        if skater not in elements_per_skater:
            raise Exception(f"Missing skater {skater} in {event_name}. Was the name spelled correctly on the uploaded sheet?")
        for element_details in elements_per_skater[skater]:
            allScores = element_details["Scores"]
            avg = sum(allScores)/len(allScores)

            judgeNumber = 1
            for judge in tj_scores[skater]:
                if judge not in tj_filter:
                    judgeNumber+=1
                    continue
                if len(tj_scores[skater][judge]) -1 < element_details["Number"]:
                    print (f"score length error on {skater} and judge {judge}")
                goe = tj_scores[skater][judge][element_details["Number"]]
                deviation = goe-avg
                if (abs(deviation)>= 2):
                    element_errors.append({
                        "Skater": skater,
                        "Element": element_details["Element"],
                        "Judge Name": judge,
                        "Judge Number": judgeNumber,
                        "Judge Score": goe,
                        "Panel Average":avg,
                        "Deviation": deviation,
                        "Type": "Deviation"
                        })
                    print("Found deviation")
                judgeNumber+=1
    pcs_errors = add_pcs_errors(pcs_per_skater, pcs_scores, tj_filter)
    printToExcel(workbook, event_name, judges, [], element_errors, pcs_errors,  pdf_number)
    #workbook, event_name, judges, element_errors, element_deviations, pcs_errors, pdf_number
    total_errors = judgingParsing.count_total_errors_per_judge(judges, [], element_errors, pcs_errors)
    num_starts = len(elements_per_skater)
    allowed_errors = judgingParsing.get_allowed_errors(num_starts)
    # print(f"Processed {event_name}")
    return event_name, total_errors, num_starts, allowed_errors

def add_pcs_errors(pcs_per_skater, tj_pcs_scores, tj_filter):
    errors = []
    for skater in tj_pcs_scores:
        for pcs_mark in pcs_per_skater[skater]:
            allScores = pcs_mark["Scores"]
            avg = sum(allScores)/len(allScores)
            judgeNumber = 1
            for judge in tj_pcs_scores[skater]:
                if judge not in tj_filter:
                    judgeNumber+=1
                    continue
                tj_score = tj_pcs_scores[skater][judge][get_component_number(pcs_mark["Component"])]
                deviation = tj_score-avg
                if (abs(deviation) >= 1.5):
                    errors.append({
                    "Skater": skater,
                    "Judge Number": judgeNumber,
                    "Judge Name": judge,
                    "Judge Score": tj_score,
                    "Deviation": deviation,
                    "Component": pcs_mark["Component"],
                    "Type": "Deviation"
                    })
                judgeNumber+=1
    return errors

def get_component_number(name):
    if name == "Skating Skills":
        return 2
    if name == "Composition":
        return 0
    if name == "Presentation":
        return 1

def processPapers(events=[], excel_path = '', tj_pdf_base_path = '', judges_names = [], tj_filter = [], use_gcp=False):
    workbook = openpyxl.Workbook()

    print (f"processing for {events} and judges {judges_names}")
    judge_errors = {}
    event_details = {}
    detailed_rule_errors = []

    i = 0
    for event in events:
        pdf_path = f"{tj_pdf_base_path}{event}.pdf"
        tj_pdf_path = f"{tj_pdf_base_path}{event}_analysis.xlsx"
        (event_name, total_errors, num_starts, allowed_errors) = analyzeTrialJudges(tj_pdf_path, workbook, pdf_path, excel_path, judges_names, 2, tj_filter, use_gcp=use_gcp)

        start_of_summary_rows = sum(total_errors)+11
        event_details[event_name] = {"Num Starts": num_starts, "Allowed Errors": allowed_errors, "Sheet Name": judgingParsing.get_sheet_name(event_name, 2), "Summary Row Start": start_of_summary_rows}
        for i in range(len(judges_names)):
            judge = judges_names[i]
            if len(tj_filter) > 0 and judge not in tj_filter:
                continue
            if judge not in judge_errors:
                judge_errors[judge] = {}
            judge_errors[judge][event_name] = {"Errors": total_errors[i], "Allowed Errors": allowed_errors,  "In Excess": max(total_errors[i]-allowed_errors, 0), "Judge Number": i+1}
        
        
    #Sort sheets
    del workbook['Sheet']
    workbook._sheets.sort(key=lambda ws: ws.title)

    make_competition_summary_page(workbook, "TrialJudge", event_details, judge_errors)
    if (use_gcp):
        downloadResults.save_gcp_workbook(workbook, excel_path)
    else:
        workbook.save(excel_path) 
        
if __name__ == "__main__":
    # Specify paths for the input PDF and output Excel file
    #pdf_base_path = "/Users/rnaphtal/Documents/JudgingAnalysis/TrialJudges/"  # Update with the correct path
    excel_path = "/Users/rachaelnaphtal/Documents/JudgingAnalysis_Results/TrialJudges/ORC_Anomaly_Summary_Analysis.xlsx" 
    tj_pdf_base_path = "/Users/rachaelnaphtal/Documents/JudgingAnalysis_Results/TrialJudges/"

    #GCP example
    excel_path = "skating_orc_reports/gs://skating_orc_reports/Generated/Nats/Nats.xlsx" 
    tj_pdf_base_path = "skating_orc_reports/gs://skating_orc_reports/Generated/Nats/"

    workbook = openpyxl.Workbook()  
    #events= ["NPFS", "JMFS", "JMSP", "JPSP", "JPFS", "SWSP", "SPSP", "SWFS", "SMSP"]
    events = ["JMSP", "JMFS"]
    
    judgesNames = ["Melanya Berggren", "Katie Beriau", "Scott Brody", "Waverly Huston", "Rhea Sy-Benedict", "William Tran", "Mary-E Wightman"]
    processPapers(use_gcp=True, events=events, excel_path=excel_path, tj_pdf_base_path=tj_pdf_base_path, judges_names=judgesNames)

    # for judge in judgesNames:
        # processPapers(events=events, excel_path=f"{tj_pdf_base_path}ORC_Anomaly_Summary_Analysis_{judge}.xlsx", tj_pdf_base_path=tj_pdf_base_path, judges_names=judgesNames, tj_filter=[judge])

    #2024
    excel_path = "/Users/rnaphtal/Documents/JudgingAnalysis/TrialJudges/2024/ORC_Anomaly_Summary_Analysis_Melanya_and_Scott.xlsx" 
    tj_pdf_base_path = "/Users/rnaphtal/Documents/JudgingAnalysis/TrialJudges/2024/"
    events= ["NPSP", "JPSP", "JPFS",  "SPSP","SWSP", "SWFS",  "SMSP"]
    judgesNames = ["Melanya Berggren", "Scott Brody", "Shelbi Gill", "Joy Jin", "Elliot Schwartz",  "Mary-E Wightman"]
    # processPapers(events=events, excel_path=f"{tj_pdf_base_path}ORC_Anomaly_Summary_Analysis_Melanya.xlsx", tj_pdf_base_path=tj_pdf_base_path, judges_names=judgesNames, tj_filter=["Melanya Berggren"])
    # processPapers(events=events, excel_path=f"{tj_pdf_base_path}ORC_Anomaly_Summary_Analysis_Scott.xlsx", tj_pdf_base_path=tj_pdf_base_path, judges_names=judgesNames, tj_filter=["Scott Brody"])


    
