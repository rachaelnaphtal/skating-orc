import asyncio
from pyppeteer import launch
import judgingParsing 
from judgingParsing import FitSheetWrapper
import requests
from bs4 import BeautifulSoup
import xlwt 
from xlwt import Workbook 
import pandas as pd
import xlsxwriter

async def generate_pdf(url, pdf_path):
    browser = await launch()
    page = await browser.newPage()
    
    await page.goto(url)
    
    await page.pdf({'path': pdf_path, 'format': 'A4'})
    
    await browser.close()


def processEvent(url, eventName, judges, workbook, pdf_number):
    pdf_path = f"{pdf_folder}{eventName}.pdf"
    excel_path = excel_folder
    asyncio.get_event_loop().run_until_complete(generate_pdf(url, pdf_path))
    return judgingParsing.extract_judge_scores(workbook, pdf_path, excel_path, judges, pdf_number)
    
def get_page_contents(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }

    page = requests.get(url, headers=headers)

    if page.status_code == 200:
        return page.text

    return None

def get_urls_and_names(page_contents):
    soup = BeautifulSoup(page_contents, 'html.parser')
    links = soup.find_all('a', href=True, string="Final")
    names = soup.find_all('td', class_='event tRow bRow')
    return list(set(links)), names
#processEvent("https://ijs.usfigureskating.org/leaderboard/results/2024/34290/SEGM028.html")


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


def findResultsDetailUrlAndJudgesNames(base_url, results_Page_link):
    url = f"{base_url}/{results_Page_link}"
    page_contents = get_page_contents(url)
    soup = BeautifulSoup(page_contents, 'html.parser')
    link = soup.find('a', href=True, string="Judge detail scores")
    judgesNames = findJudgesNames(soup)
    return (link["href"], judgesNames)

def scrape(base_url, report_name):
    url = f"{base_url}/index.asp"
    page_contents = get_page_contents(url)
    workbook = xlwt.Workbook()  

    if page_contents:
        links, names = get_urls_and_names(page_contents)
        judgeErrors = {}
        for i in range(len(links)):
            (resultsLink, judgesNames) = findResultsDetailUrlAndJudgesNames(base_url, links[i]["href"])
            (event_name, total_errors, allowed_errors) = processEvent(f"{base_url}/{resultsLink}", i, judgesNames, workbook, i)
            for i in range(len(judgesNames)):
                judge = judgesNames[i]
                if judge not in judgeErrors:
                    judgeErrors[judge] = {}
                judgeErrors[judge][event_name] = {"Errors": total_errors[i], "Allowed Errors": allowed_errors,  "In Excess": max(total_errors[i]-allowed_errors, 0)}
        
        sheet = FitSheetWrapper(workbook.add_sheet("Summary")) 
        # Specifying style 
        bold = xlwt.easyxf('font: bold 1') 
        blue = xlwt.easyxf('pattern: pattern solid, fore_colour blue;')
        sheet.write(0, 0, "Summary", bold)
        current_col = 0
        summary_row=5+ max([len(judgeErrors[judge]) for judge in judgeErrors])

        # Add summary row for all anomalies
        sheet.write(1, current_col, "Judge Name")
        sheet.write(1, current_col+1, "# Anomalies")
        sheet.write(1, current_col+2, "# In Excess")
        sheet.write(1, current_col+3, "# Events")
        current_row = 2

       # print (judgeErrors)
        df_dict = {}
        for judge in judgeErrors:
            df_dict[judge] = pd.DataFrame.from_dict(judgeErrors[judge], orient='index')
        for judge, value in sorted(df_dict.items(), key=lambda kv: kv[1]['Errors'].sum(), reverse=True):
            sheet.write(current_row, current_col, judge)
            sheet.write(current_row, current_col+1, int(df_dict[judge]["Errors"].sum()))
            sheet.write(current_row, current_col+2, int(df_dict[judge]["In Excess"].sum()))
            sheet.write(current_row, current_col+3, len(judgeErrors[judge]))
            current_row+=1
        current_col+=5
        for judge in judgeErrors:
            current_row = 1
            sheet.write(current_row, current_col, judge, bold)
            current_row+=1
            sheet.write(current_row, current_col, "Event")
            sheet.write(current_row, current_col+1, "Anomalies")
            sheet.write(current_row, current_col+2, "ORC recognized")
            sheet.write(current_row, current_col+3, "Allowed")
            sheet.write(current_row, current_col+4, "In Excess")
            current_row+=1

           
            for event in judgeErrors[judge]:
                if event == "Total Errors" or event == "Allowed Errors":
                    continue
                sheet.write(current_row, current_col, event.replace("_", " "))
                num_errors = judgeErrors[judge][event]["Errors"]
                sheet.write(current_row, current_col+1, num_errors)

                num_allowed = int(judgeErrors[judge][event]["Allowed Errors"])
                sheet.write(current_row, current_col+3, num_allowed)

                in_excess = int(judgeErrors[judge][event]["In Excess"])
                sheet.write(current_row, current_col+4, in_excess)
                current_row+=1

            current_row+=1

            sheet.write(summary_row, current_col, "Total", bold)
            sheet.write(summary_row, current_col+1, int(df_dict[judge]["Errors"].sum()), bold)
            sheet.write(summary_row, current_col+4, int(df_dict[judge]["In Excess"].sum()), bold)
            current_col+=5
        
            
    else:
        print('Failed to get page contents.')

    excel_path = f"{excel_folder}{report_name}.xls"
    workbook.save(excel_path) 
    print ("Finished " + report_name)
    return df_dict

def create_season_summary(full_report_name="2425Summary"):
    workbook = xlwt.Workbook()  
    events = {
        "Dallas_Classic":"2024/33436",
        "Cactus_Classic":"2024/34414",
        "Peach_Open":"2024/33518",
        "Glacier_Falls":"2024/33519",
        "Lake_Placid":"2024/33491",
        "Philadelphia":"2024/33453",
        "Scott_Hamilton":"2024/33501",
        "Copper_Cup":"2024/33425",
        "Cup_of_Colorado":"2024/33507",
        "Skate_the_Lake":"2024/33520",
        "MiddleAtlantics":"2024/33515",
        "SkateSF": "2024/33479", 
        "Potomac": "2024/33523",
        #"Providence": "",
        
        "Chicagoland":"2024/33497",
        "JohnSmith":"2024/33451",
        "Pasadena":"2024/33509",
        "PNIW":"2024/33489",
        "Challenge_Cup": "2024/34444",
        "Skate_Cleveland": "2024/33466",
        "Austin_Autumn_Classic": "2024/33458",
        "BostonNQS" : "2024/33526",
        "Pacifics2425": "2024/34291",
        "Midwesterns_DanceFinal2425": "2024/34290",
        "Easterns_PairsFinal2425": "2024/34289",
    }
    summary_dict = {}

    for event_name in events:
        result = scrape(f"https://ijs.usfigureskating.org/leaderboard/results/{events[event_name]}", f"{event_name}")
        for judge in result:
            result[judge]["Competition"] = event_name
            if judge not in summary_dict:
                summary_dict[judge] = result[judge]
            else:
                summary_dict[judge] = pd.concat([summary_dict[judge], result[judge]], axis=0)
    print_summary_workbook(workbook, summary_dict, full_report_name)
                
def print_summary_workbook(workbook, summary_dict, full_report_name):
    sheet = FitSheetWrapper(workbook.add_sheet("Summary")) 
    # Specifying style 
    bold = xlwt.easyxf('font: bold 1') 
    sheet.write(0, 0, "Summary", bold)
    current_col = 0

    # Add summary sheet for all anomalies
    sheet.write(1, current_col, "Judge Name")
    sheet.write(1, current_col+1, "# Anomalies")
    sheet.write(1, current_col+2, "# In Excess")
    sheet.write(1, current_col+3, "# Events")
    sheet.write(1, current_col+4, "In Excess per event")
    current_row = 2

    for judge, value in sorted(summary_dict.items(), key=lambda kv: kv[1]['In Excess'].sum(), reverse=True):
        sheet.write(current_row, current_col, judge)
        sheet.write(current_row, current_col+1, int(value["Errors"].sum()))
        sheet.write(current_row, current_col+2, int(value["In Excess"].sum()))
        num_events = len(value[value["Errors"] >= 0])
        sheet.write(current_row, current_col+3, num_events)
        sheet.write(current_row, current_col+4, float(value["In Excess"].sum())/float(num_events))
        current_row+=1

    excel_path = f"{excel_folder}{full_report_name}.xls"
    workbook.save(excel_path) 
    # Add sheets per judge
    writer = pd.ExcelWriter(f"{excel_folder}{full_report_name}_perJudge.xlsx", engine = 'xlsxwriter')
    for judge in sorted(summary_dict.keys()):
        print_sheet_per_judge(writer, judge, summary_dict[judge])
    writer.close()

def print_sheet_per_judge(writer, judge_name, judge_df):
    judge_df.rename(columns={"Errors": "Anomalies", "Allowed Errors": "Allowed"})
    judge_df.to_excel(writer, sheet_name = judge_name)
    writer.sheets[judge_name].set_column(0, 0, 35)
    writer.sheets[judge_name].set_column(1, 1, 12)
    writer.sheets[judge_name].set_column(2, 2, 12)
    writer.sheets[judge_name].set_column(3, 3, 12)
    writer.sheets[judge_name].set_column(4, 4, 30)

pdf_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/Results/"  # Update with the correct path
excel_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/"
base_url = 'https://ijs.usfigureskating.org/leaderboard/results/2024/34290'
#report_name = "Mids2024_ORC_Report"

# #Mids 2425
# pdf_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/Results/"  # Update with the correct path
# excel_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/"
# base_url = 'https://ijs.usfigureskating.org/leaderboard/results/2024/34290'
# report_name = "Mids2425_ORC_Report"

#Easterns/ Pairs Final 2425
pdf_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/Easterns/Results/"  # Update with the correct path
excel_folder = "/Users/rnaphtal/Documents/JudgingAnalysis/Easterns/"
base_url = 'https://ijs.usfigureskating.org/leaderboard/results/2024/34289'
report_name = "Easterns_ORC_Report"
scrape(base_url, report_name)
#create_season_summary()