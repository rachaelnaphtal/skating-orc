from nicegui import ui
from nicegui.events import ValueChangeEventArguments
from enum import Enum

class ReportType(Enum):
    COMPETITION_REPORT = 1
    FULL_SEASON_SUMMARY = 2
    SEGMENT_REPORT = 3
    TRIAL_JUDGE_REPORT = 4

class ReportParams:
    def __init__(self):
        self.type = None
        self.competition_url=''
    
    def isCompetitionReport(self):
        return self.type == ReportType.COMPETITION_REPORT

report_params = ReportParams()

# def show(event: ValueChangeEventArguments):
#     name = type(event.sender).__name__
#     ui.notify(f'{name}: {event.value}')

def switchReportType(event: ValueChangeEventArguments):
    report_params.type = event.value
    print (event.value)

def generateReport(event: ValueChangeEventArguments):
    if report_params.type == ReportType.COMPETITION_REPORT:
        ui.notify('Generating Report')
        print ("Generating competition report")
    else:
        ui.notify('Report Not Yet Supported')

ui.markdown('*Officials Review Reports Generation*')
#ui.radio(['Competition Report', 'Full Season Summary', 'Segment Report'], value='A', on_change=show).props('inline')
ui.toggle({ReportType.COMPETITION_REPORT: 'Competition Report', ReportType.FULL_SEASON_SUMMARY: 'Full Season Summary', 
           ReportType.SEGMENT_REPORT: 'Segment Report', ReportType.TRIAL_JUDGE_REPORT: 'Trial Judge Report'}, 
          on_change=switchReportType).bind_value(report_params, 'number')
ui.button('Generate Report', on_click=generateReport)

with ui.column().bind_visibility_from(report_params, "isCompetitionReport()"):
    ui.input(label='Competition Results URL (ending in the number)', placeholder='Ex: https://ijs.usfigureskating.org/leaderboard/results/2025/34240',
         on_change=lambda e: url.set_text('you typed: ' + e.value),
         validation={'Input wrong length': lambda value: len(value) != 63}).props('clearable')
    ui.label().bind_text_to(report_params, 'competition_url')
# with ui.row():
#     ui.checkbox('Checkbox', on_change=show)
#     ui.switch('Switch', on_change=show)
# ui.radio(['A', 'B', 'C'], value='A', on_change=show).props('inline')
# with ui.row():
#     ui.input('Text input', on_change=show)
#     ui.select(['One', 'Two'], value='One', on_change=show)
# ui.link('And many more...', '/documentation').classes('mt-8')

ui.run()