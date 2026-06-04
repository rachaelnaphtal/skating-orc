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


def listing_reference_july1(season_code: int) -> date:
    """
    July 1 at the start of the listing anchor season.

    ``2627`` → ``2026-07-01``; ``2728`` → ``2027-07-01``. Used for age and time in grade.
    """
    start_yy = int(season_code) // 100
    return date(2000 + start_yy, 7, 1)


def format_listing_reference_july1(season_code: int) -> str:
    """Demographics reference label, e.g. ``as of July 1, 2026`` for listing ``2627``."""
    return f"as of July 1, {listing_reference_july1(season_code).year}"


def age_listing_column_label(season_code: int) -> str:
    return f"Age {format_listing_reference_july1(season_code)}"


def years_in_grade_listing_column_label(season_code: int) -> str:
    return f"Years in grade {format_listing_reference_july1(season_code)}"


def listing_season_codes_for_projection(*, years_ahead: int = 12) -> list[int]:
    """Listing season codes from configured options through ``years_ahead`` future cycles."""
    codes = list(REPORT_LISTING_SEASON_OPTIONS)
    code = max(codes)
    for _ in range(years_ahead):
        code += 101
        codes.append(code)
    return sorted({int(c) for c in codes})


def promote_first_eligible_column_label() -> str:
    return "First promote year (years)"


def calendar_year_for_listing_milestone(
    first_listing_season: int | None,
    *,
    current_listing_season_code: int,
) -> int | None:
    """
    Calendar year of the listing reference July 1 for a milestone season.

    If the milestone is on or before the current listing anchor, uses the current listing year.
    """
    if first_listing_season is None or (
        isinstance(first_listing_season, float) and pd.isna(first_listing_season)
    ):
        return None
    season = int(first_listing_season)
    if season <= int(current_listing_season_code):
        season = int(current_listing_season_code)
    return int(listing_reference_july1(season).year)


def format_listing_milestone_year_display(
    first_listing_season: int | None,
    *,
    current_listing_season_code: int,
) -> str:
    year = calendar_year_for_listing_milestone(
        first_listing_season,
        current_listing_season_code=current_listing_season_code,
    )
    return "—" if year is None else str(year)


def format_promote_first_eligible_display(
    first_listing_season: int | None,
    *,
    current_listing_season_code: int,
) -> str:
    """
    Calendar year (July 1 anchor) of the first listing when promote year rules are met.

    ``2627`` → ``2026``. If already satisfied at the current listing, uses the current season.
    """
    return format_listing_milestone_year_display(
        first_listing_season,
        current_listing_season_code=current_listing_season_code,
    )


def age_out_column_label() -> str:
    return "Age out year (70 on July 1)"


def format_age_out_display(
    age_out_listing: int | None,
    *,
    current_listing_season_code: int,
) -> str:
    """First listing July 1 when age as of that date is at least 70."""
    return format_listing_milestone_year_display(
        age_out_listing,
        current_listing_season_code=current_listing_season_code,
    )


def current_age_bin_label(
    age: int,
    *,
    bin_width: int = 5,
    age_out_at: int = 70,
) -> str:
    """
    Five-year age band aligned to the age-out threshold (e.g. 65–69, 60–64, 70+).
    """
    age = int(age)
    if age >= int(age_out_at):
        return f"{int(age_out_at)}+"
    high = int(age_out_at) - 1
    while high >= 0:
        low = max(0, high - int(bin_width) + 1)
        if low <= age <= high:
            return f"{low}–{high}"
        high = low - 1
    low = 0
    high = min(int(bin_width) - 1, int(age_out_at) - 1)
    return f"{low}–{high}"


def _current_age_bin_sort_key(label: str, *, age_out_at: int = 70) -> int:
    if label.endswith("+"):
        return int(age_out_at)
    low = int(label.split("–", 1)[0])
    return low


def histogram_counts_by_current_age_bins(
    ages: pd.Series,
    *,
    bin_width: int = 5,
    age_out_at: int = 70,
) -> pd.DataFrame:
    """Count officials in age bands aligned to age-out (65–69, 60–64, …, 70+)."""
    if bin_width <= 0:
        raise ValueError("bin_width must be positive")
    valid = ages.dropna().astype(int)
    if valid.empty:
        return pd.DataFrame(columns=["bin_label", "count"])
    labels = valid.map(
        lambda a: current_age_bin_label(a, bin_width=bin_width, age_out_at=age_out_at)
    )
    counts = (
        pd.DataFrame({"bin_label": labels})
        .groupby("bin_label", as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    counts["_sort"] = counts["bin_label"].map(
        lambda lb: _current_age_bin_sort_key(lb, age_out_at=age_out_at)
    )
    return counts.sort_values("_sort", ascending=False).drop(columns="_sort")[
        ["bin_label", "count"]
    ]


def histogram_counts_by_year_bins(
    calendar_years: pd.Series,
    *,
    bin_width: int = 5,
) -> pd.DataFrame:
    """Count values in calendar-year bins (single year or ranges like 2020–2024)."""
    if bin_width <= 0:
        raise ValueError("bin_width must be positive")
    years = calendar_years.dropna().astype(int)
    if years.empty:
        return pd.DataFrame(columns=["bin_label", "count"])
    if bin_width == 1:
        bin_start = years
        labels = years.map(str)
    else:
        bin_start = (years // bin_width) * bin_width
        labels = bin_start.map(lambda b: f"{int(b)}–{int(b + bin_width - 1)}")
    return (
        pd.DataFrame({"bin_start": bin_start, "bin_label": labels})
        .groupby(["bin_start", "bin_label"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("bin_start")[["bin_label", "count"]]
    )


def listing_season_code_from_calendar_year(listing_calendar_year: int) -> int:
    """Map July 1 listing calendar year ``2027`` → anchor season ``2627``."""
    end = int(listing_calendar_year) % 100
    start = end - 1
    return int(f"{start:02d}{end:02d}")


def listing_season_code_for_july1_calendar_year(calendar_year: int) -> int:
    """Map calendar year of listing July 1 (e.g. ``2026``) → season code ``2627``."""
    y = int(calendar_year)
    return int(f"{y % 100:02d}{(y + 1) % 100:02d}")


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
