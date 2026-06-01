"""
USFS / ISU listing season codes for the international officials report.

The report anchor season (e.g. ``2627``) is the listing cycle being evaluated.
Service windows use the ``n`` USFS season codes immediately before that anchor
(excluding the anchor season itself).
"""

from __future__ import annotations

from datetime import date

import pandas as pd

try:
    from activityAnalysis.load_activity_data import calendar_years_for_usfs_season_codes
except ModuleNotFoundError:
    from load_activity_data import calendar_years_for_usfs_season_codes

REPORT_LISTING_SEASON_DEFAULT = 2627
REPORT_LISTING_SEASON_OPTIONS: tuple[int, ...] = (2627, 2728)
REPORT_SEASON_WINDOW_OPTIONS: tuple[int, ...] = (2, 3, 4)
REPORT_SEASON_WINDOW_DEFAULT = 4


def format_usfs_season_code(code: int) -> str:
    """``2627`` → ``26-27``."""
    text = f"{int(code):04d}"
    return f"{text[:2]}-{text[2:]}"


def listing_calendar_year_from_season_code(season_code: int) -> int:
    """Map anchor season ``2627`` → July 1 listing calendar year ``2027``."""
    return 2000 + int(season_code) % 100


def listing_season_code_from_calendar_year(listing_calendar_year: int) -> int:
    """Map July 1 listing calendar year ``2027`` → anchor season ``2627``."""
    end = int(listing_calendar_year) % 100
    start = end - 1
    return int(f"{start:02d}{end:02d}")


def listing_calendar_year(as_of: date | None = None) -> int:
    """Calendar year of the next ISU listing cycle (July 1 anchor)."""
    d = as_of or date.today()
    return d.year if d.month >= 7 else d.year


def default_listing_season_code(as_of: date | None = None) -> int:
    return listing_season_code_from_calendar_year(listing_calendar_year(as_of))


def season_codes_preceding_listing(anchor_season_code: int, n: int) -> list[int]:
    """
    USFS season codes for the ``n`` seasons immediately before ``anchor_season_code``.

    Example: anchor ``2627``, n=3 → ``[2324, 2425, 2526]``.
    Example: anchor ``2728``, n=3 → ``[2425, 2526, 2627]``.
    """
    if n <= 0:
        return []
    anchor = int(anchor_season_code)
    codes: list[int] = []
    code = anchor - 101
    for _ in range(n):
        codes.insert(0, code)
        code -= 101
    return codes


def competition_year_matches_seasons(
    year_val: object,
    season_codes: list[int],
) -> bool:
    if year_val is None or (isinstance(year_val, float) and pd.isna(year_val)):
        return False
    text_val = str(year_val).strip()
    if not text_val.isdigit():
        return False
    n = int(text_val)
    if n in season_codes:
        return True
    calendar_years = calendar_years_for_usfs_season_codes(season_codes)
    if len(text_val) == 4 and n in calendar_years:
        return True
    return False


def filter_panel_to_season_codes(
    panel: pd.DataFrame,
    season_codes: list[int] | None,
) -> pd.DataFrame:
    """Keep panel rows whose competition season is in ``season_codes``."""
    if panel.empty or not season_codes:
        return panel.iloc[0:0] if season_codes is not None else panel
    mask = panel["competition_year"].apply(
        lambda y: competition_year_matches_seasons(y, season_codes)
    )
    return panel.loc[mask].reset_index(drop=True)
