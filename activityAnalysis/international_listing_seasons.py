"""
USFS / ISU listing season codes for the international officials report.

The report anchor season (e.g. ``2627``) is the listing cycle being evaluated.
Service windows use the ``n`` USFS season codes immediately before that anchor
(excluding the anchor season itself).
"""

from __future__ import annotations

from datetime import date

import pandas as pd

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
    """True when ``competition.year`` is a USFS season code (e.g. ``2526``)."""
    if year_val is None or (isinstance(year_val, float) and pd.isna(year_val)):
        return False
    text_val = str(year_val).strip()
    if not text_val.isdigit():
        return False
    return int(text_val) in {int(x) for x in season_codes}


def season_year_sql_predicate(alias: str = "c") -> str:
    """SQL fragment: ``competition.year`` equals a USFS season code (``2526``, …)."""
    return f"""
              AND btrim({alias}.year::text) IN :season_year_codes
"""


def season_codes_as_bind_strings(season_codes: list[int]) -> list[str]:
    """Bind values for ``season_year_sql_predicate`` (``competition.year`` is text-like)."""
    return [str(int(x)) for x in season_codes]


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
