import asyncio
from io import BytesIO
from pyppeteer import launch
import gcp_interactions_helper
import judgingParsing
from judgingParsing import autofit_worksheet
from sharedJudgingAnalysis import format_out_of_range_sheets
import requests
from bs4 import BeautifulSoup
from openpyxl.utils import get_column_letter
import pandas as pd
import re
import openpyxl
import time
import pdfkit
from database import test_connection, get_db_session
from database_loader import DatabaseLoader
from sqlalchemy.orm import Session
from openpyxl import Workbook
from datetime import datetime
from openpyxl.styles import (
    PatternFill,
    Border,
    Side,
    Alignment,
    Protection,
    Font,
    Color,
)
from google.cloud import storage
from gcp_interactions_helper import write_file_to_gcp
from gcp_interactions_helper import save_gcp_workbook


def convert_url_to_pdf(url, pdf_path):
    try:
        pdfkit.from_url(url, pdf_path)
        # print(f"PDF generated and saved at {pdf_path}")
    except Exception as e:
        print(f"PDF generation failed: {e}")


async def generate_pdf(url, pdf_path, use_gcp=False):
    browser = await launch(
        {
            "autoClose": False,
            "handleSIGINT": False,
            "handleSIGTERM": False,
            "handleSIGHUP": False,
        }
    )
    page = await browser.newPage()
    await page.goto(url)
    pdf_data = await page.pdf({"format": "A4"})
    if use_gcp:
        write_file_to_gcp(pdf_data, pdf_path)
    else:
        with open(pdf_path, "wb") as f:
            f.write(pdf_data)
    await browser.close()


def processEvent(
    url,
    eventName,
    judges,
    workbook,
    pdf_number,
    event_regex,
    pdf_folder,
    excel_path,
    only_rule_errors=False,
    use_gcp=False,
    create_thrown_out_analysis=False,
    use_html=True,
    judge_filter=""
):
    if use_html:
        return judgingParsing.extract_judge_scores(
        workbook=workbook,
        pdf_path="",
        base_excel_path=excel_path,
        judges=judges,
        pdf_number=pdf_number,
        event_regex=event_regex,
        only_rule_errors=only_rule_errors,
        url=url,
        use_gcp=use_gcp,
        create_thrown_out_analysis=create_thrown_out_analysis,
        judge_filter=judge_filter,
        use_html=use_html
         )
    pdf_path = f"{pdf_folder}{eventName}.pdf"
    asyncio.run(generate_pdf(url, pdf_path, use_gcp=use_gcp))
    # convert_url_to_pdf(url, pdf_path)
    return judgingParsing.extract_judge_scores(
        workbook,
        pdf_path,
        excel_path,
        judges,
        pdf_number,
        event_regex,
        only_rule_errors,
        use_gcp=use_gcp,
        create_thrown_out_analysis=create_thrown_out_analysis,
        judge_filter=judge_filter,
        use_html=False
    )


def get_page_contents(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    }

    page = requests.get(url, headers=headers)

    if page.status_code == 200:
        return page.text

    return None


def get_urls_and_names(page_contents):
    soup = BeautifulSoup(page_contents, "html.parser")
    links = soup.find_all("a", href=True, string="Final")
    names = soup.find_all("td", class_="event tRow bRow")
    return list(dict.fromkeys(links)), names


def findJudgesNames(soup):
    alltd = soup.find_all("td")
    judges = []
    nextJudge = False
    for td in alltd:
        if td.text.count("Judge ") > 0:
            nextJudge = True
        elif nextJudge:
            judges.append(td.text.split(",")[0])
            nextJudge = False
    return judges


# event_name -> total_errors_per_judge, allowed_errors
def make_competition_summary_page(
    workbook,
    report_name,
    event_details_dict,
    judge_errors,
    only_rule_errors=False,
    use_gcp=False,
):
    sheet = workbook.create_sheet("Summary", 0)

    # Styles
    bold = Font(bold=True)
    gray = PatternFill("solid", fgColor="C0C0C0")
    thin = Side(border_style="thin", color="000000")
    thin_border = Border(top=thin, left=thin, right=thin, bottom=thin)
    wrap_text = Alignment(wrap_text=True)
    vertical_text = Alignment(textRotation=90)

    # sheet.freeze_panes("D1")
    sheet.cell(1, 1, value=report_name)
    sheet.cell(1, 1).font = bold
    sheet.cell(2, 1, value="Official's Review Summary")
    sheet.cell(2, 1).font = bold
    sheet.cell(6, 1, value="EVENT")
    sheet.cell(6, 1).alignment = wrap_text
    sheet.cell(6, 1).border = thin_border
    sheet.cell(6, 2, value="STARTS")
    sheet.cell(6, 2).alignment = wrap_text
    sheet.cell(6, 2).border = thin_border
    sheet.cell(6, 3, value="ALLOWED ERRORS")
    sheet.cell(6, 3).alignment = wrap_text
    sheet.cell(6, 3).border = thin_border
    sheet.column_dimensions["A"].width = 35

    current_row = 9
    events_in_order = sorted(event_details_dict.items())
    for event in events_in_order:
        sheet.cell(current_row, 1, value=event[0])
        sheet.cell(current_row, 2, value=event[1]["Num Starts"])
        sheet.cell(current_row, 3, value=event[1]["Allowed Errors"])
        current_row += 1

    current_row += 1
    sheet.cell(current_row, 1, value="TOTALS")
    sheet.cell(current_row, 1).font = bold

    totals_row = current_row
    for i in range(7, totals_row):
        sheet.cell(i, 1).border = Border(right=thin)
        sheet.cell(i, 2).border = Border(right=thin)
        sheet.cell(i, 3).border = Border(right=thin)
    sheet.cell(totals_row, 1).border = thin_border
    sheet.cell(totals_row, 2).border = thin_border
    sheet.cell(totals_row, 3).border = thin_border

    current_col = 4
    for judge in dict(sorted(judge_errors.items())):
        sheet.cell(6, current_col).value = judge
        sheet.cell(6, current_col + 1).alignment = Alignment(
            wrap_text=True, horizontal="center"
        )
        sheet.merge_cells(
            start_row=6, start_column=current_col, end_row=6, end_column=current_col + 3
        )
        sheet.cell(7, current_col, value="Number Anomalies")
        sheet.cell(7, current_col + 1, value="OAC Recognized Errors")
        sheet.cell(7, current_col + 2, value="Errors in Excess Pre-Review")
        sheet.cell(7, current_col + 3, value="Errors in Excess After Review")
        for i in range(4):
            sheet.cell(7, current_col + i).alignment = vertical_text
        current_row = 9
        for event in events_in_order:
            if event[0] in judge_errors[judge]:
                judge_number = judge_errors[judge][event[0]]["Judge Number"]
                sheet_name = event[1]["Sheet Name"]
                row = judge_number + event[1]["Summary Row Start"] - 1
                sheet.cell(
                    current_row,
                    current_col,
                    value=judge_errors[judge][event[0]]["Errors"],
                )
                sheet.cell(
                    current_row, current_col + 1
                ).value = f"='{sheet_name}'!C{row}"
                # print (f"row:{current_row}, col:{current_col+1} value:{sheet.cell(current_row, current_col+1).value}")
                sheet.cell(
                    current_row,
                    current_col + 2,
                    value=judge_errors[judge][event[0]]["In Excess"],
                )
                sheet.cell(current_row, current_col + 2).font = Font(
                    b=True, color="FF0000"
                )
                sheet.cell(
                    current_row, current_col + 3
                ).value = f"=MAX({get_column_letter(current_col + 1)}{current_row}-C{current_row}, 0)"
                sheet.cell(current_row, current_col + 3).font = Font(
                    b=True, color="FF0000"
                )
                for i in range(4):
                    sheet.cell(current_row, current_col + i).fill = PatternFill(
                        "solid", fgColor="CCE5FF"
                    )
            current_row += 1

        for i in range(4, current_col + 4):
            column_letter = get_column_letter(i)
            sheet.cell(
                totals_row, i
            ).value = f"=SUM({column_letter}9:{column_letter}{totals_row - 1})"
        sheet.cell(totals_row, current_col + 3).fill = PatternFill(
            "solid", fgColor="66B2FF"
        )

        # Add borders
        sheet.cell(6, current_col).border = thin_border
        sheet.cell(7, current_col).border = thin_border
        for i in range(8, totals_row):
            sheet.cell(i, current_col).border = Border(left=thin)
            sheet.cell(i, current_col + 3).border = Border(right=thin)
        for i in range(4):
            sheet.cell(6, current_col + i).border = thin_border
            sheet.cell(7, current_col + i).border = thin_border
            sheet.cell(totals_row, current_col + i).border = thin_border

        current_col += 4


def make_old_summary_sheet(workbook, df_dict, judge_errors, event_regex):
    # Add summary sheet
    sheet = workbook.create_sheet("Summary", 0)

    sheet.cell(1, 1, value="Summary")
    current_col = 1
    summary_row = 6 + max([len(judge_errors[judge]) for judge in judge_errors])

    # Add summary row for all anomalies
    sheet.cell(2, current_col, value="Judge Name")
    sheet.cell(2, current_col + 1, value="# Anomalies")
    sheet.cell(2, current_col + 2, value="# ORC Errors")
    sheet.cell(2, current_col + 3, value="# In Excess Anomalies")
    sheet.cell(2, current_col + 4, value="# In Excess After ORC")
    sheet.cell(2, current_col + 5, value="# Events")
    current_row = 3

    for judge, value in sorted(
        df_dict.items(), key=lambda kv: kv[1]["Errors"].sum(), reverse=True
    ):
        sheet.cell(current_row, current_col, value=judge)
        sheet.cell(
            current_row, current_col + 1, value=int(df_dict[judge]["Errors"].sum())
        )
        sheet.cell(
            current_row, current_col + 3, value=int(df_dict[judge]["In Excess"].sum())
        )
        sheet.cell(current_row, current_col + 5, value=len(judge_errors[judge]))
        current_row += 1
    current_col += 7
    for judge in dict(sorted(judge_errors.items())):
        current_row = 2
        sheet.cell(current_row, current_col, value=judge)
        current_row += 1
        sheet.cell(current_row, current_col, value="Event")
        sheet.cell(current_row, current_col + 1, value="Anomalies")
        sheet.cell(current_row, current_col + 2, value="ORC recognized")
        sheet.cell(current_row, current_col + 3, value="Allowed")
        sheet.cell(current_row, current_col + 4, value="In Excess (Pre Review)")
        sheet.cell(current_row, current_col + 5, value="In Excess (Post Review)")
        current_row += 1

        for event in judge_errors[judge]:
            if (
                event == "Total Errors"
                or event == "Allowed Errors"
                or not re.match(event_regex, event)
            ):
                continue
            sheet.cell(current_row, current_col, event.replace("_", " "))
            num_errors = judge_errors[judge][event]["Errors"]
            sheet.cell(current_row, current_col + 1, value=num_errors)

            num_allowed = int(judge_errors[judge][event]["Allowed Errors"])
            sheet.cell(current_row, current_col + 3, value=num_allowed)

            in_excess = int(judge_errors[judge][event]["In Excess"])
            sheet.cell(current_row, current_col + 4, value=in_excess)
            current_row += 1

        current_row += 1

        sheet.cell(summary_row, current_col, value="Total")
        sheet.cell(
            summary_row, current_col + 1, value=int(df_dict[judge]["Errors"].sum())
        )
        sheet.cell(
            summary_row, current_col + 4, value=int(df_dict[judge]["In Excess"].sum())
        )
        current_col += 5
    judgingParsing.autofit_worksheet(sheet)


def findResultsDetailUrlAndJudgesNames(base_url, results_page_link):
    url = f"{base_url}/{results_page_link}"
    page_contents = get_page_contents(url)
    soup = BeautifulSoup(page_contents, "html.parser")
    link = soup.find("a", href=True, string="Judge detail scores")
    judgesNames = findJudgesNames(soup)
    event_name = soup.find_all('h1')[0].get_text()
    return (link["href"], judgesNames, event_name)

def loadCompetitionInfo(base_url):
    page_contents = get_page_contents(base_url)
    soup = BeautifulSoup(page_contents, "html.parser")
    all_h3_tags = soup.find_all('h3')
    date = all_h3_tags[0].get_text().split(" ")
    start_date = date[0]
    end_date = date[2]
    location = all_h3_tags[2].get_text()
    return (start_date, end_date, location)

def loadInfoForExistingCompetitions():
    session = get_db_session()
    database_obj = DatabaseLoader(session)
    urls = database_obj.getCompetitionUrlsWithNoLocation()
    for url in urls:
        (start_date, end_date, location) = loadCompetitionInfo(f"{url}/index.asp")
        database_obj.updateCompetition(url, location=location, start_date=start_date, end_date=end_date)

def scrape(
    base_url,
    report_name,
    excel_folder="",
    pdf_folder="",
    event_regex="",
    only_rule_errors=False,
    use_gcp=False,
    add_additional_analysis=False,
    write_to_database=False, 
    year="2526",
    judge_filter="",
    specific_exclude="", 
    use_html=True
):
    url = f"{base_url}/index.asp"
    page_contents = get_page_contents(url)
    workbook = openpyxl.Workbook()
    agg_all_element_df = None
    agg_all_pcs_df = None
    session = get_db_session()
    database_obj = DatabaseLoader(session)

    if write_to_database:
        competition_id = database_obj.insert_competition(report_name.replace("_", " "), base_url, year)
        proccessed_segments = database_obj.getSegmentNamesForCompetition(base_url)

    if page_contents:
        links, names = get_urls_and_names(page_contents)
        judge_errors = {}
        event_details = {}
        detailed_rule_errors = []
        for i in range(len(links)):
            (resultsLink, judgesNames, event_name) = findResultsDetailUrlAndJudgesNames(
                base_url, links[i]["href"]
            )
            # event_name_formatted = (
            #     event_name
            #     .replace("/", "")
            #     .replace(" ", "_")
            #     .replace("__", "_")
            #     .replace("-", "_")
            # )
            # event_name_formatted = event_name_formatted.split("/")[0]
            # event_name_formatted = event_name_formatted.split(":")[0]
            if specific_exclude and re.match(specific_exclude, event_name):
                continue
    
            (
                event_name,
                total_errors,
                num_starts,
                allowed_errors,
                rule_errors,
                all_element_dict,
                all_pcs_dict,
            ) = processEvent(
                f"{base_url}/{resultsLink}",
                i,
                judgesNames,
                workbook,
                i,
                event_regex,
                pdf_folder,
                excel_folder,
                only_rule_errors=only_rule_errors,
                use_gcp=use_gcp,
                create_thrown_out_analysis=add_additional_analysis or write_to_database,
                judge_filter=judge_filter, 
                use_html=use_html
            )
            if total_errors == None:
                # This is an event to skip per the regex
                continue
            start_of_summary_rows = sum(total_errors) + 11
            event_details[event_name] = {
                "Num Starts": num_starts,
                "Allowed Errors": allowed_errors,
                "Sheet Name": judgingParsing.get_sheet_name(event_name, i),
                "Summary Row Start": start_of_summary_rows,
            }
            for i in range(len(judgesNames)):
                judge = judgesNames[i]
                if len(judge_filter) > 0 and judge_filter!=judge:
                    continue
                if judge not in judge_errors:
                    judge_errors[judge] = {}
                judge_errors[judge][event_name] = {
                    "Errors": total_errors[i],
                    "Allowed Errors": allowed_errors,
                    "In Excess": max(total_errors[i] - allowed_errors, 0),
                    "Judge Number": i + 1,
                }

            for rule_error in rule_errors:
                rule_error["Competition"] = report_name
                rule_error["Event"] = event_name
                detailed_rule_errors.append(rule_error)

            # Processing of additional info
            all_pcs_df = pd.DataFrame.from_dict(all_pcs_dict)
            all_element_df = pd.DataFrame.from_dict(all_element_dict)
            if agg_all_pcs_df is None:
                agg_all_pcs_df = all_pcs_df
            else:
                agg_all_pcs_df = pd.concat([agg_all_pcs_df, all_pcs_df])

            if agg_all_element_df is None:
                agg_all_element_df = all_element_df
            else:
                agg_all_element_df = pd.concat([agg_all_element_df, all_element_df])

            # write to database
            if write_to_database and event_name not in proccessed_segments:
                print(f"Writing segment {event_name} {datetime.now().strftime("%H:%M:%S")}")
                segment_id = database_obj.insert_segment(event_name, competition_id)
                database_obj.insert_element_scores(judgesNames, all_element_dict, segment_id, rule_errors)
                database_obj.insert_pcs_scores(judgesNames, all_pcs_dict, segment_id)
            else:
                print(f"Skipping segment {event_name}")
        # Sort sheets
        del workbook["Sheet"]
        workbook._sheets.sort(key=lambda ws: ws.title)

        df_dict = {}
        for judge in judge_errors:
            df_dict[judge] = pd.DataFrame.from_dict(judge_errors[judge], orient="index")

        errors_dict_to_return = pd.DataFrame.from_dict(detailed_rule_errors)

        make_competition_summary_page(
            workbook, report_name, event_details, judge_errors
        )

    else:
        print("Failed to get page contents.")

    excel_path = f"{excel_folder}{report_name}.xlsx"
    if use_gcp:
        save_gcp_workbook(workbook, excel_path)
    else:
        workbook.save(excel_path)

    if add_additional_analysis:
        create_additional_analysis_sheet(
            agg_all_element_df, agg_all_pcs_df, excel_folder, report_name, use_gcp=use_gcp
        )
    print("Finished " + report_name + datetime.now().strftime("%H:%M:%S"))
    return df_dict, errors_dict_to_return


def create_summary_element_df(df, grouping_name):
    # Filter only thrown-out elements
    df_thrown_out = df[df["Thrown out"] == True]

    # Group by Judge Name and Element Type
    summary = (
        df_thrown_out.groupby(["Judge Name", grouping_name])
        .agg(
            Num_Low=("High", lambda x: (x == False).sum()),
            Num_High=("High", lambda x: (x == True).sum()),
        )
        .reset_index()
    )

    # Get the total number of judged elements per Judge Name & Element Type
    total_judged = (
        df.groupby(["Judge Name", grouping_name])
        .size()
        .reset_index(name="Total # Judged")
    )

    # Merge with thrown-out data
    summary = summary.merge(total_judged, on=["Judge Name", grouping_name], how="left")

    # Calculate percentages
    summary["% Low"] = summary["Num_Low"] / summary["Total # Judged"]
    summary["% High"] = summary["Num_High"] / summary["Total # Judged"]
    summary["% Out"] = (summary["Num_Low"] + summary["Num_High"]) / summary[
        "Total # Judged"
    ]

    # Rename columns to match your desired format
    summary.rename(columns={"Num_Low": "# Low", "Num_High": "# High"}, inplace=True)
    return summary

def make_analysis_cover_sheet(workbook):
    sheet = workbook.create_sheet("Overview")

    sheet.column_dimensions["A"].width = 25
    sheet.column_dimensions["B"].width = 150
    sheet.row_dimensions[3].height = 40
    sheet.row_dimensions[15].height = 40

    bold = Font(bold=True)

    sheet.cell(1, 1, value="Overview").font = Font(bold=True, size=18)
    sheet.merge_cells("A3:B4")
    sheet.cell(
        3,
        1,
        value="This workbook contains additional analysis of the judging data, specifically related to deviations. \n The first two sheets show the percentage of GOEs or PCS of each type that are extremes related to the judging panel. The first six columns show the absolute numbers and the final three show the percentages of the total.",
    )
    sheet.cell(3, 1).alignment = Alignment(wrap_text=True)

    sheet.cell(7, 1, value="Definitions:").font = bold
    sheet.cell(8, 1, value="Thrown out:").font = bold
    sheet.cell(9, 1, value="Low vs High:").font = bold

    sheet.cell(
        8,
        2,
        value="The number of scores that are the extremes of the panel. If at least three judges give the same score, it does not count as thrown out.",
    )
    sheet.cell(
        9,
        2,
        value="Low refers to the judge being lower than the average of the panel. The total is the sum of low and high.",
    )
    sheet.cell(8, 2).alignment = Alignment(wrap_text=True)
    sheet.cell(9, 2).alignment = Alignment(wrap_text=True)

    sheet.merge_cells("A12:B12")
    sheet.cell(
        12,
        1,
        value="The later sheets show the distinct scores given before processing. Feel free to filter to dig into specifics more.",
    )


def create_additional_analysis_sheet(
    all_element_df, all_pcs_df, excel_folder, report_name, use_gcp=False
):
    excel_path = f"{excel_folder}{report_name}_Additional_Analysis.xlsx"
    excel_buffer = BytesIO()
    if not use_gcp:
        excel_buffer = excel_path
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        make_analysis_cover_sheet(writer.book)
        # Analyze elements
        summary_goe_df = create_summary_element_df(all_element_df, "Element Type")
        summary_goe_df.to_excel(
            writer, sheet_name="GOE Thrown out", float_format="%.2f", index=False
        )
        format_out_of_range_sheets(writer.sheets["GOE Thrown out"])

        summary_pcs_df = create_summary_element_df(all_pcs_df, "Component")
        summary_pcs_df.to_excel(
            writer, sheet_name="PCS Thrown out", float_format="%.2f", index=False
        )
        format_out_of_range_sheets(writer.sheets["PCS Thrown out"])

        all_element_df.to_excel(writer, sheet_name="All Elements", index=False)
        writer.sheets["All Elements"].auto_filter.ref = writer.sheets[
            "All Elements"
        ].dimensions
        all_pcs_df.to_excel(writer, sheet_name="All PCS", index=False)
        writer.sheets["All PCS"].auto_filter.ref = writer.sheets["All PCS"].dimensions

        for sheet in writer.sheets:
            if sheet != "Overview":
                judgingParsing.autofit_worksheet(writer.sheets[sheet])

    if use_gcp:
        gcp_interactions_helper.write_file_to_gcp(excel_buffer.getvalue(), excel_path)


def create_season_summary(pdf_folder = "", excel_folder = "", full_report_name="2425Summary", only_rule_errors=False):
    start = time.time()
    workbook = openpyxl.Workbook()
    events = {
        "Eastern_Synchro_Sectionals": "2025/34239",
        "Midwest_Synchro_Sectionals": "2025/34240",
        "Pacific_Coast_Synchro_Sectionals": "2025/34241",
        "Dallas_Classic": "2024/33436",
        "Cactus_Classic": "2024/34414",
        "Peach_Open": "2024/33518",
        "Glacier_Falls": "2024/33519",
        "Lake_Placid": "2024/33491",
        # "Philadelphia": "2024/33453",
        # "Scott_Hamilton": "2024/33501",
        # "Copper_Cup": "2024/33425",
        # "Cup_of_Colorado": "2024/33507",
        # "Skate_the_Lake": "2024/33520",
        # "MiddleAtlantics": "2024/33515",
        # "SkateSF": "2024/33479",
        # "Potomac": "2024/33523",
        # # "Providence": "",
        # "Chicagoland": "2024/33497",
        # "JohnSmith": "2024/33451",
        # "Pasadena": "2024/33509",
        # "PNIW": "2024/33489",
        # "Challenge_Cup": "2024/34444",
        # "Skate_Cleveland": "2024/33466",
        # "Austin_Autumn_Classic": "2024/33458",
        "BostonNQS": "2024/33526",
        "Pacifics2425": "2024/34291",
        "Midwesterns_DanceFinal2425": "2024/34290",
        "Easterns_PairsFinal2425": "2024/34289",
        "2025USChampionships": "2025/35539",
        
    }
    summary_dict = {}
    all_rules_errors = []

    event_regex = ""
    if only_rule_errors:
        event_regex = ".*Women|Men|Boys|Girls.*"
    for event_name in events:
        start_event = time.time()
        result, rule_errors = scrape(
            f"https://ijs.usfigureskating.org/leaderboard/results/{events[event_name]}",
            f"{event_name}",
            event_regex,
            only_rule_errors=only_rule_errors,
            write_to_database=True,
            pdf_folder=pdf_folder
        )
        all_rules_errors.append(rule_errors)
        for judge in result:
            result[judge]["Competition"] = event_name
            if judge not in summary_dict:
                summary_dict[judge] = result[judge]
            else:
                summary_dict[judge] = pd.concat(
                    [summary_dict[judge], result[judge]], axis=0
                )
        print(f"{time.time() - start_event} seconds for {event_name}")
    print_summary_workbook(workbook, summary_dict, full_report_name)
    print_rule_error_summary_workbook(
        pd.concat(all_rules_errors, ignore_index=False), full_report_name
    )
    print(f"{time.time() - start} seconds elapsed total")


def print_summary_workbook(workbook, summary_dict, full_report_name):
    sheet = workbook.create_sheet("Summary", 0)
    # Specifying style
    # bold = xlwt.easyxf('font: bold 1')
    sheet.cell(1, 1, value="Summary")
    current_col = 1

    # Add summary sheet for all anomalies
    sheet.cell(2, current_col, value="Judge Name")
    sheet.cell(2, current_col + 1, value="# Anomalies")
    sheet.cell(2, current_col + 2, value="# In Excess")
    sheet.cell(2, current_col + 3, value="# Events")
    sheet.cell(2, current_col + 4, value="In Excess per event")
    current_row = 3

    for judge, value in sorted(
        summary_dict.items(), key=lambda kv: kv[1]["In Excess"].sum(), reverse=True
    ):
        sheet.cell(current_row, current_col, value=judge)
        sheet.cell(current_row, current_col + 1, value=int(value["Errors"].sum()))
        sheet.cell(current_row, current_col + 2, value=int(value["In Excess"].sum()))
        num_events = len(value[value["Errors"] >= 0])
        sheet.cell(current_row, current_col + 3, value=num_events)
        sheet.cell(
            current_row,
            current_col + 4,
            value=float(value["In Excess"].sum()) / float(num_events),
        )
        current_row += 1

    excel_path = f"{excel_folder}{full_report_name}.xls"
    workbook.save(excel_path)
    # Add sheets per judge
    writer = pd.ExcelWriter(
        f"{excel_folder}{full_report_name}_perJudge.xlsx", engine="openpyxl"
    )
    for judge in sorted(summary_dict.keys()):
        print_sheet_per_judge(writer, judge, summary_dict[judge])
    writer.close()


def print_sheet_per_judge(writer, judge_name: str, judge_df):
    judge_df.rename(columns={"Errors": "Anomalies", "Allowed Errors": "Allowed"})
    judge_df.to_excel(writer, sheet_name=judge_name)
    writer.sheets[judge_name].column_dimensions["A"].width = 35
    writer.sheets[judge_name].column_dimensions["B"].width = 12
    writer.sheets[judge_name].column_dimensions["C"].width = 12
    writer.sheets[judge_name].column_dimensions["D"].width = 12
    writer.sheets[judge_name].column_dimensions["E"].width = 30


def print_rule_error_summary_workbook(rule_errors, full_report_name):
    writer = pd.ExcelWriter(
        f"{excel_folder}{full_report_name}_RuleErrors.xlsx", engine="openpyxl"
    )

    grouped_df = rule_errors.groupby("Judge Name").size()
    grouped_df = grouped_df.sort_values(ascending=False)
    grouped_df.to_excel(writer, sheet_name="Summary")
    autofit_worksheet(writer.sheets["Summary"])

    rule_errors.to_excel(writer, sheet_name="All Errors")
    autofit_worksheet(writer.sheets["All Errors"])

    # Add sheets per judge
    for judge in sorted(rule_errors["Judge Name"].unique()):
        judge_df = rule_errors[rule_errors["Judge Name"] == judge]
        judge_df.to_excel(writer, sheet_name=judge)
        autofit_worksheet(writer.sheets[judge])
    writer.close()


# pdf_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/Results/"  # Update with the correct path
# excel_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/"
# base_url = 'https://ijs.usfigureskating.org/leaderboard/results/2024/34290'
# report_name = "Mids2024_ORC_Report"

# #Mids 2425
# pdf_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/Results/"  # Update with the correct path
# excel_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/"
# base_url = 'https://ijs.usfigureskating.org/leaderboard/results/2024/34290'
# report_name = "Mids2425_ORC_Report"

if __name__ == "__main__":
    # #Easterns/ Pairs Final 2425
    pdf_folder = "/Users/rachaelnaphtal/Documents/JudgingAnalysis_Results/Easterns/Results/"  # Update with the correct path
    excel_folder = "/Users/rachaelnaphtal/Documents/JudgingAnalysis_Results/Official/"
    base_url = "https://ijs.usfigureskating.org/leaderboard/results/2024/34289"
    # scrape("https://ijs.usfigureskating.org/leaderboard/results/2025/35539", "US_Champs_25")
    # scrape("https://ijs.usfigureskating.org/leaderboard/results/2025/35539", "US_Champs_25_SP", event_regex=".*(Women|Men|Pairs).*")
    # scrape("https://ijs.usfigureskating.org/leaderboard/results/2025/35539", "US_Champs_25_Dance", event_regex=".*(Dance).*")
    # scrape("https://ijs.usfigureskating.org/leaderboard/results/2025/34240", "Midwestern_Synchro_25", event_regex=".*(Novice|Junior|Senior).*")
    # scrape("https://ijs.usfigureskating.org/leaderboard/results/2025/34241", "PacificCoast_Synchro_25", event_regex=".*(Novice|Junior|Senior).*")
    # scrape(
    #     "https://ijs.usfigureskating.org/leaderboard/results/2025/34241",
    #     "Pacific_Coast_Synchro_25",
    #     excel_folder=excel_folder,
    #     pdf_folder=pdf_folder,
    #     # event_regex=".*(Dance).*",
    #     add_additional_analysis=False,
    #     write_to_database=True
    # )
    # scrape("https://ijs.usfigureskating.org/leaderboard/results/2025/34241", "PacificCoast_Synchro_25_all")

    # scrape(base_url, "2024_Pairs_Final_with_errors", ".?(Novice|Junior|Senior).?(Pairs).?")
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2024/34290', "2024_Dance_Final", ".*(Novice|Junior|Senior).?(Dance).*")
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2023/33513', "2023_Boston_NQS", "")
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2025/35701', "PhiladelphiaSummerChallengeNQS2025", "", write_to_database=True, pdf_folder=pdf_folder)
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2025/34237', "2025USSynchroChamps", "", write_to_database=True, pdf_folder=pdf_folder)
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2025/35645', "GlacierFallsSummerClassicNQS2025", "", write_to_database=True, pdf_folder=pdf_folder)
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2025/35895', "DuPageNQS2025", "", write_to_database=True, pdf_folder=pdf_folder)
    scrape('https://ijs.usfigureskating.org/leaderboard/results/2025/36275', "DallasClassicNQS2025", "", write_to_database=True, pdf_folder=pdf_folder) 
    scrape('https://ijs.usfigureskating.org/leaderboard/results/2025/35672', "LakePlacidNQS2025", "", write_to_database=True, pdf_folder=pdf_folder)
    scrape('https://ijs.usfigureskating.org/leaderboard/results/2025/35638', "2025SiliconValleyOpen", "", write_to_database=True, pdf_folder=pdf_folder)
    # create_season_summary(pdf_folder=pdf_folder, excel_folder=excel_folder)

    #Issues
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2022/30670', '2022_John_Smith_Memorial_NQS', write_to_database=True, pdf_folder=pdf_folder, year=2223, specific_exclude='314 Juvenile Boys SP')
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2022/30472', '2022_Cup_of_Colorado_NQS', write_to_database=True, pdf_folder=pdf_folder, year=2223, specific_exclude="(47) Intermediate Pairs")
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2022/30692', '2022_Pasadena_Open_Championships_NQS', write_to_database=True, pdf_folder=pdf_folder, year=2223)
    # Dance
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2022/30629', '2022_Glacier_Falls_Summer_Classic_NQS', write_to_database=True, pdf_folder=pdf_folder, year=2223)
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2022/30690', '2022_Silicon_Valley_Open_NQS', write_to_database=True, pdf_folder=pdf_folder, year=2223)
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2022/30693', '2022_Onyx_Challenge_NQS', write_to_database=True, pdf_folder=pdf_folder, year=2223)
    # scrape('https://ijs.usfigureskating.org/leaderboard/results/2022/30712', '2022_Challenge_Cup_NQS', write_to_database=True, pdf_folder=pdf_folder, year=2223)

    loadInfoForExistingCompetitions()