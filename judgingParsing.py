import pdfplumber
from pypdf import PdfReader
import pandas as pd
import re

import asyncio
from pyppeteer import launch
import xlwt 
from xlwt import Workbook 
import arial10

USING_ISU_COMPONENT_METHOD=False

class FitSheetWrapper(object):
    """Try to fit columns to max size of any entry.
    To use, wrap this around a worksheet returned from the 
    workbook's add_sheet method, like follows:

        sheet = FitSheetWrapper(book.add_sheet(sheet_name))

    The worksheet interface remains the same: this is a drop-in wrapper
    for auto-sizing columns.
    """
    def __init__(self, sheet):
        self.sheet = sheet
        self.widths = dict()

    def write(self, r, c, label='', *args, **kwargs):
        self.sheet.write(r, c, label, *args, **kwargs)
        width = int(arial10.fitwidth(label))
        if width > self.widths.get(c, 0):
            self.widths[c] = width
            self.sheet.col(c).width = width

    def __getattr__(self, attr):
        return getattr(self.sheet, attr)

async def generate_pdf(url, pdf_path, judges):
    browser = await launch()
    page = await browser.newPage()
    
    await page.goto(url)
    
    await page.pdf({'path': pdf_path, 'format': 'A4'})
    
    await browser.close()

def extract_judge_scores(workbook, pdf_path, base_excel_path,judges, pdf_number):
    # Initialize list for storing extracted data
    elements_per_skater = {}
    pcs_per_skater = {}
    skater_details = {}
    event_name=""

    # Open the PDF file
    with PdfReader(pdf_path) as pdf:
    #with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                return
    
        #if "JUDGES DETAILS PER SKATER" in text:
            text = text.replace("\xa0", "")
            
            # Split text into lines
            lines = text.split("\n")
            if event_name == "":
                event_name=lines[2].replace("/","").replace(" ", "_").replace("__", "_").replace("-", "_") 
                event_name = event_name.split("/")[0]
                event_name = event_name.split(":")[0]

            current_skater = None
            for line in lines:
                # Match skater's name section
                skater_match = re.match(r"^(\d+)\s+([A-Za-z\(\)\/\-\s]+),?([A-Za-z\&\(\)\-\s]+)?\s?([\d]{1,3}\.[\d\.]{2})\s?([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)$", line)
                if skater_match:
                    skater_name = skater_match.group(2).strip()
                    technical_score = skater_match.group(5)
                    current_skater = skater_name
                    if current_skater not in elements_per_skater:
                        elements_per_skater[current_skater] = []
                        pcs_per_skater[current_skater] = []
                        skater_details[current_skater] = technical_score
                    continue

                # Match elements and judge scores
                element_match = re.match(r"^(\d+)(\s)*([\w\+\.\*<>!]+(?:\s+\w+)?)\s+(?:([F*<!>q]+)\s+)?(-?[\d\.]+x?)\s+(?:([F\*x]+)\s+)?(-?[\d\.]+)\s+(-?(?:-?\d\s+){3,9})\s*([\d\.]+\s)?([\d\.]+)", line)
                pcs_match = re.match(r"^^\s*(Timing|Presentation|Skating\sSkills|Composition)\s+([\d\.]+)\s+((?:[\d\.]{4}\s*){3,9})([\d\.]+)", line)
                if current_skater and element_match:
                   # element_number = int(element_match.group(1))
                    element_name = element_match.group(3)
                    judges_scores = list(map(float, element_match.group(8).split()))
                    total_score = float(element_match.group(10))
                    #scores = list(map(int, element_match.group(2).split()))
                    elements_per_skater[current_skater].append({
                        "Element": element_name,
                        "Scores": judges_scores,
                        "Value": total_score
                    })
                elif current_skater and pcs_match:
                    component = pcs_match.group(1)
                    no_spaces = pcs_match.group(3).replace(" ","")
                    scores = [no_spaces[i:i+4] for i in range(0, len(no_spaces), 4)]
                    judges_scores = list(map(float, scores))
                    pcs_per_skater[current_skater].append({
                        "Component": component,
                        "Scores": judges_scores
                    })
      

        for skater in elements_per_skater:
            foundElements = round(sum([x["Value"] for x in elements_per_skater[skater]]), 2)
            expected = float(skater_details[skater])
            if (foundElements != expected):
                print (f"Elements for skater {skater} do not match. Expected TES:{expected}, Sum of elements:{foundElements} {pdf_path}")
                #print (f"Elements for {skater}: {[f"{x["Element"]} ({x["Value"]})" for x in skater_scores[skater]]}")
            #else:
                #print (f"Elements for skater {skater} do match.")
            pcs = pcs_per_skater[skater]
            if (len(pcs) != 3):
                print(f"Components missing for skater {skater} {pdf_path}")
        
        element_errors = findElementDeviations(elements_per_skater, judges)
        pcs_errors = findPCSDeviations(pcs_per_skater, judges)
        total_errors = count_total_errors_per_judge(judges, element_errors, pcs_errors)

        printToExcel(workbook, base_excel_path, event_name, judges, element_errors, pcs_errors, total_errors, pdf_number)
        print(f"Num Skaters: {len(skater_details)}")
        print (list(elements_per_skater.keys()))
        return (event_name, total_errors, get_allowed_errors(len(skater_details)))
   
    
def get_allowed_errors(num_skaters : int):
    if num_skaters <= 10:
        return 1
    elif num_skaters <=20:
        return 2
    return 3

def findElementDeviations(skater_scores, judges):
    errors = []
    for skater in skater_scores:
        for element in skater_scores[skater]:
            allScores = element["Scores"]
            avg = sum(allScores)/len(allScores)
            judgeNumber = 1
            for judgeNumber in range(1,len(allScores)+1):
                deviation = abs(avg-allScores[judgeNumber-1])
                if (deviation> 2):
                    #print (f"Deviation found for judge {judgeNumber} on skater {skater}, element {element["Element"]}")
                    errors.append({
                        "Skater": skater,
                        "Element": element["Element"],
                        "Judge Number": judgeNumber,
                        "Judge Name": judges[judgeNumber-1],
                        "Judge Score": allScores[judgeNumber-1],
                        "Panel Average":avg,
                        "Deviation": deviation
                        })
    return errors

def findPCSDeviations(skater_scores, judges):
    errors = []
    for skater in skater_scores:
        deviation_points = [float(0)]*(len(judges)+1)
        for component in skater_scores[skater]:
            allScores = component["Scores"]
            avg = sum(allScores)/len(allScores)
            for judgeNumber in range(1,len(allScores)+1):
                deviation = abs(avg-allScores[judgeNumber-1])
                if (not USING_ISU_COMPONENT_METHOD and deviation > 1.5):
                    errors.append({
                    "Skater": skater,
                    "Judge Number": judgeNumber,
                    "Judge Name": judges[judgeNumber-1],
                    "Judge Score": allScores[judgeNumber-1],
                    "Deviation": deviation
                    })
                deviation_points[judgeNumber]= deviation_points[judgeNumber]+ deviation

        for judgeNumber in range(1,len(allScores)+1):
            if (USING_ISU_COMPONENT_METHOD and deviation_points[judgeNumber] > 4.5):
                # Add errors here if using ISU method
                errors.append({
                    "Skater": skater,
                    "Judge Number": judgeNumber,
                    "Judge Name": judges[judgeNumber-1],
                    "Judge Score": "",
                    "Deviation": deviation_points[judgeNumber]
                    })
    return errors

def count_total_errors_per_judge(judges, element_errors, pcs_errors) -> list:
    errors_list = [0]*len(judges)
    for error in element_errors:
        judgeNumber = error["Judge Number"]
        errors_list[judgeNumber-1] = errors_list[judgeNumber-1]+ 1
    for error in pcs_errors:
        judgeNumber = error["Judge Number"]
        errors_list[judgeNumber-1] = errors_list[judgeNumber-1]+ 1
    return errors_list

def printToExcel(workbook, base_excel_path, event_name, judges, element_errors, pcs_errors, total_errors, pdf_number):
    sheet_name = event_name
    with_el_match = re.match(r"(\d{1,3})_(\d{1,3}_)?(.*)", sheet_name)
    if with_el_match:
        sheet_name=with_el_match.group(3)
    sheet_name = f"{sheet_name[0:min(len(sheet_name),26)]}{pdf_number}"
    sheet = FitSheetWrapper(workbook.add_sheet(sheet_name)) 
    # Specifying style 
    bold = xlwt.easyxf('font: bold 1') 
    sheet.write(0, 0, event_name, bold) 
    # Headers
    sheet.write(3, 0, "Judge")
    sheet.write(3, 1, "Judge Score")
    sheet.write(3, 2, "Deviation From Panel Average")
    sheet.write(3, 3, "Skater(s)/Couple(s)")
    #sheet.write(3, 4, "Element #")
    sheet.write(3, 5, "Element Name")
    sheet.write(3, 6, "ORC Comments")
    sheet.write(3, 7, "ORC Error?")
    sheet.write(4, 0, "A. RANGES OF GOE", bold)

    current_row = 5
    for error in element_errors:
        sheet.write(current_row, 0, f"J{error["Judge Number"]}- {error["Judge Name"]}")
        sheet.write(current_row, 1, error["Judge Score"])
        sheet.write(current_row, 2, error["Deviation"])
        sheet.write(current_row, 3, error["Skater"])
        sheet.write(current_row, 5, error["Element"])
        current_row=current_row+1

    current_row+=1
    sheet.write(current_row, 0, "B. RANGES OF PROGRAM COMPONENTS", bold)
    current_row+=1
    for error in pcs_errors:
        sheet.write(current_row, 0, str(f"J{error["Judge Number"]}- {error["Judge Name"]}"))
        sheet.write(current_row, 1, error["Judge Score"])
        sheet.write(current_row, 2, str(error["Deviation"]))
        sheet.write(current_row, 3, error["Skater"])
        current_row=current_row+1

    current_row+=2
    sheet.write(current_row, 0, "Judge", bold)
    sheet.write(current_row, 1, "# of Anomalies", bold)
    sheet.write(current_row, 2, "ORC Recognized", bold)
    current_row+=1

    for i in range(len(judges)):
        sheet.write(current_row, 0, judges[i])
        sheet.write(current_row, 1, total_errors[i])
        current_row+=1
    
    print (f"Processed {event_name}")
    
   

if __name__ == "__main__":
    # Specify paths for the input PDF and output Excel file
    pdf_path = "/Users/rnaphtal/Documents/JudgingAnalysis/2425/Results/2.pdf"  # Update with the correct path
    excel_path = "/Users/rnaphtal/Documents/JudgingAnalysis/Easterns/" 

    workbook = xlwt.Workbook()  
    extract_judge_scores(workbook, pdf_path, excel_path, ["Name1", "Name2", "Name3", "Name4", "Name5", "Name6", "Name7", "Name8", "Name9"], 2)
    excel_path = f"{excel_path}DeviationsReport2.xls"
    workbook.save(excel_path) 