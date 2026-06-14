"""Drill-down filter helpers for PCS deviation judge breakdown tables."""

from __future__ import annotations

import pandas as pd

_PCS_DETAIL_COLUMN_ALIASES: dict[str, str] = {
    "judge_name": "Judge",
    "discipline": "Discipline",
    "component": "Component",
    "pcs_marks": "PCS marks",
    "partial_marking_score": "Partial marking score",
    "mean_pcs_bias": "Mean PCS bias",
    "mean_abs_error": "Mean |error|",
    "mean_sigma": "Mean σ̂",
    "control_bin": "Control bin",
}


def _normalize_pcs_detail_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    rename = {
        src: dst
        for src, dst in _PCS_DETAIL_COLUMN_ALIASES.items()
        if src in df.columns and dst not in df.columns
    }
    if rename:
        return df.rename(columns=rename)
    return df


def filter_pcs_judge_detail_tables(
    discipline_detail: pd.DataFrame,
    component_detail: pd.DataFrame,
    control_bin_detail: pd.DataFrame,
    *,
    min_marks: int | None = None,
    disciplines: list[str] | None = None,
    components: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Subset drill-down rows by mark count, discipline, and/or PCS component."""
    jd = _normalize_pcs_detail_columns(
        discipline_detail if isinstance(discipline_detail, pd.DataFrame) else pd.DataFrame()
    )
    jc = _normalize_pcs_detail_columns(
        component_detail if isinstance(component_detail, pd.DataFrame) else pd.DataFrame()
    )
    jb = _normalize_pcs_detail_columns(
        control_bin_detail
        if isinstance(control_bin_detail, pd.DataFrame)
        else pd.DataFrame()
    )

    if min_marks is not None and int(min_marks) > 0:
        m = int(min_marks)
        if not jd.empty and "PCS marks" in jd.columns:
            jd = jd.loc[jd["PCS marks"] >= m]
        if not jc.empty and "PCS marks" in jc.columns:
            jc = jc.loc[jc["PCS marks"] >= m]
        if not jb.empty and "PCS marks" in jb.columns:
            jb = jb.loc[jb["PCS marks"] >= m]

    if disciplines:
        disc_set = set(disciplines)
        if not jd.empty and "Discipline" in jd.columns:
            jd = jd.loc[jd["Discipline"].isin(disc_set)]
        if not jc.empty and "Discipline" in jc.columns:
            jc = jc.loc[jc["Discipline"].isin(disc_set)]
        if not jb.empty and "Discipline" in jb.columns:
            jb = jb.loc[jb["Discipline"].isin(disc_set)]

    if components:
        comp_set = set(components)
        if not jc.empty and "Component" in jc.columns:
            jc = jc.loc[jc["Component"].isin(comp_set)]
        if not jb.empty and "Component" in jb.columns:
            jb = jb.loc[jb["Component"].isin(comp_set)]

    return jd, jc, jb


def pcs_detail_filter_options(
    discipline_detail: pd.DataFrame,
    component_detail: pd.DataFrame,
    control_bin_detail: pd.DataFrame,
) -> tuple[list[str], list[str]]:
    """Distinct discipline and component labels for drill-down filter widgets."""
    jd = _normalize_pcs_detail_columns(
        discipline_detail if isinstance(discipline_detail, pd.DataFrame) else pd.DataFrame()
    )
    jc = _normalize_pcs_detail_columns(
        component_detail if isinstance(component_detail, pd.DataFrame) else pd.DataFrame()
    )
    jb = _normalize_pcs_detail_columns(
        control_bin_detail
        if isinstance(control_bin_detail, pd.DataFrame)
        else pd.DataFrame()
    )
    disciplines: set[str] = set()
    components: set[str] = set()
    for frame in (jd, jc, jb):
        if not frame.empty and "Discipline" in frame.columns:
            disciplines.update(frame["Discipline"].dropna().astype(str))
        if not frame.empty and "Component" in frame.columns:
            components.update(frame["Component"].dropna().astype(str))
    return sorted(disciplines), sorted(components)
