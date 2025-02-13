from nicegui import ui
from nicegui.events import ValueChangeEventArguments
from enum import Enum
import downloadResults
import asyncio


class ReportType(Enum):
    COMPETITION_REPORT = 1
    FULL_SEASON_SUMMARY = 2
    SEGMENT_REPORT = 3
    TRIAL_JUDGE_REPORT = 4


class ReportParams:
    def __init__(self):
        self.type = None
        self.competition_url = ""
        self.event_regex = ".*"
        self.report_name = ""

    def isCompetitionReport(self):
        return self.type == ReportType.COMPETITION_REPORT


report_params = ReportParams()

# def show(event: ValueChangeEventArguments):
#     name = type(event.sender).__name__
#     ui.notify(f'{name}: {event.value}')


def switchReportType(event: ValueChangeEventArguments):
    report_params.type = event.value
    print(event.value)


def generateReport(event: ValueChangeEventArguments):
    loop = asyncio.get_running_loop()
    if report_params.type == ReportType.COMPETITION_REPORT:
        ui.notify("Generating Report")
        print(f"Generating competition report for url {report_params.competition_url}")
        loop.run_until_complete(
            downloadResults.scrape(
                report_params.competition_url,
                report_params.report_name,
                event_regex=report_params.event_regex,
            )
        )
    else:
        ui.notify("Report Not Yet Supported")


ui.markdown("Officials Review Reports Generation")
# ui.radio(['Competition Report', 'Full Season Summary', 'Segment Report'], value='A', on_change=show).props('inline')
ui.toggle(
    {
        ReportType.COMPETITION_REPORT: "Competition Report",
        ReportType.FULL_SEASON_SUMMARY: "Full Season Summary",
        ReportType.SEGMENT_REPORT: "Segment Report",
        ReportType.TRIAL_JUDGE_REPORT: "Trial Judge Report",
    },
    on_change=switchReportType,
).bind_value(report_params, "number")

ui.label("Competitions Report Parameters")
ui.label(
    "Competition Results Number URL (ending in the number). For example: https://ijs.usfigureskating.org/leaderboard/results/2025/34240"
)
ui.input(
    placeholder="Ex: https://ijs.usfigureskating.org/leaderboard/results/2025/34240",
    validation={"Input wrong length": lambda value: len(value) != 63},
).props("flat dense width=300").bind_value(report_params, "competition_url")
ui.label("Name of report")
ui.input(
    placeholder="", validation={"Report Name": lambda value: len(value) < 1}
).props("flat dense width=300").bind_value(report_params, "report_name")
ui.label("Regex for events. For all use '.*'.")
ui.input(
    placeholder="", validation={"Input too short": lambda value: len(value) < 3}
).props("flat dense width=300").bind_value(report_params, "event_regex")
ui.button("Generate Report", on_click=generateReport)
# with ui.row():
#     ui.checkbox('Checkbox', on_change=show)
#     ui.switch('Switch', on_change=show)
# ui.radio(['A', 'B', 'C'], value='A', on_change=show).props('inline')
# with ui.row():
#     ui.input('Text input', on_change=show)
#     ui.select(['One', 'Two'], value='One', on_change=show)
# ui.link('And many more...', '/documentation').classes('mt-8')

ui.run()
