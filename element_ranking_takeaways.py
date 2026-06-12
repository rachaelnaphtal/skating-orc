"""
Short narrative takeaways from element-deviation judge drill-down tables.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

_DETAIL_COLUMN_ALIASES: dict[str, str] = {
    "judge_name": "Judge",
    "discipline": "Discipline",
    "element_type": "Element type",
    "element_marks": "Element marks",
    "partial_marking_score": "Partial marking score",
    "mean_goe_bias": "Mean GOE bias",
    "mean_abs_error": "Mean |error|",
    "mean_sigma": "Mean σ̂",
    "control_int": "Control GOE",
}


def filter_judge_detail_tables(
    discipline_detail: pd.DataFrame,
    element_detail: pd.DataFrame,
    control_goe_detail: pd.DataFrame,
    *,
    min_marks: int | None = None,
    disciplines: list[str] | None = None,
    element_types: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Subset drill-down rows by mark count, discipline, and/or element type."""
    jd = _normalize_detail_columns(
        discipline_detail if isinstance(discipline_detail, pd.DataFrame) else pd.DataFrame()
    )
    je = _normalize_detail_columns(
        element_detail if isinstance(element_detail, pd.DataFrame) else pd.DataFrame()
    )
    jg = _normalize_detail_columns(
        control_goe_detail
        if isinstance(control_goe_detail, pd.DataFrame)
        else pd.DataFrame()
    )

    if min_marks is not None and int(min_marks) > 0:
        m = int(min_marks)
        if not jd.empty and "Element marks" in jd.columns:
            jd = jd.loc[jd["Element marks"] >= m]
        if not je.empty and "Element marks" in je.columns:
            je = je.loc[je["Element marks"] >= m]
        if not jg.empty and "Element marks" in jg.columns:
            jg = jg.loc[jg["Element marks"] >= m]

    if disciplines:
        disc_set = set(disciplines)
        if not jd.empty and "Discipline" in jd.columns:
            jd = jd.loc[jd["Discipline"].isin(disc_set)]
        if not je.empty and "Discipline" in je.columns:
            je = je.loc[je["Discipline"].isin(disc_set)]
        if not jg.empty and "Discipline" in jg.columns:
            jg = jg.loc[jg["Discipline"].isin(disc_set)]

    if element_types:
        et_set = set(element_types)
        if not je.empty and "Element type" in je.columns:
            je = je.loc[je["Element type"].isin(et_set)]
        if not jg.empty and "Element type" in jg.columns:
            jg = jg.loc[jg["Element type"].isin(et_set)]

    return jd, je, jg


def detail_filter_options(
    discipline_detail: pd.DataFrame,
    element_detail: pd.DataFrame,
    control_goe_detail: pd.DataFrame,
) -> tuple[list[str], list[str]]:
    """Distinct discipline and element-type labels for drill-down filter widgets."""
    jd = _normalize_detail_columns(
        discipline_detail if isinstance(discipline_detail, pd.DataFrame) else pd.DataFrame()
    )
    je = _normalize_detail_columns(
        element_detail if isinstance(element_detail, pd.DataFrame) else pd.DataFrame()
    )
    jg = _normalize_detail_columns(
        control_goe_detail
        if isinstance(control_goe_detail, pd.DataFrame)
        else pd.DataFrame()
    )
    disciplines: set[str] = set()
    element_types: set[str] = set()
    for frame in (jd, je, jg):
        if not frame.empty and "Discipline" in frame.columns:
            disciplines.update(frame["Discipline"].dropna().astype(str))
        if not frame.empty and "Element type" in frame.columns:
            element_types.update(frame["Element type"].dropna().astype(str))
    return sorted(disciplines), sorted(element_types)


def _normalize_detail_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Accept display or internal column names from drill-down tables."""
    if df.empty:
        return df
    rename = {
        src: dst
        for src, dst in _DETAIL_COLUMN_ALIASES.items()
        if src in df.columns and dst not in df.columns
    }
    if rename:
        return df.rename(columns=rename)
    return df


def _slice_label(
    discipline: str,
    element_type: str | None = None,
    *,
    control_goe: int | float | None = None,
) -> str:
    base = f"{discipline} — {element_type}" if element_type else discipline
    if control_goe is not None:
        return f"{base} @ control GOE {int(control_goe):+d}"
    return base


def _bias_phrase(bias: float, *, neutral: float) -> str:
    if bias > neutral:
        return "above panel median"
    if bias < -neutral:
        return "below panel median"
    return "near panel median"


def _n_suffix(n: int, *, min_marks: int) -> str:
    if n < min_marks:
        return f" ({n:,} marks — small sample)"
    return f" ({n:,} marks)"


def build_element_ranking_judge_takeaways(
    judge_name: str,
    marking_row: pd.Series | dict[str, Any] | None,
    discipline_detail: pd.DataFrame,
    element_detail: pd.DataFrame,
    control_goe_detail: pd.DataFrame | None = None,
    *,
    min_marks: int = 30,
    min_bin_marks: int = 15,
    top_n: int = 3,
    bias_neutral: float = 0.02,
) -> list[str]:
    """
    Build bullet-point takeaways from per-judge drill-down tables.

    ``control_goe_detail`` rows are judge × discipline × element type × rounded
    panel-median GOE (the σ̂ model bins).
    """
    lines: list[str] = []
    jd = _normalize_detail_columns(
        discipline_detail if isinstance(discipline_detail, pd.DataFrame) else pd.DataFrame()
    )
    je = _normalize_detail_columns(
        element_detail if isinstance(element_detail, pd.DataFrame) else pd.DataFrame()
    )
    jg = _normalize_detail_columns(
        control_goe_detail
        if isinstance(control_goe_detail, pd.DataFrame)
        else pd.DataFrame()
    )

    if "Judge" in jd.columns:
        jd = jd.loc[jd["Judge"] == judge_name]
    if "Judge" in je.columns:
        je = je.loc[je["Judge"] == judge_name]
    if "Judge" in jg.columns:
        jg = jg.loc[jg["Judge"] == judge_name]

    if marking_row is not None:
        row = (
            marking_row
            if isinstance(marking_row, pd.Series)
            else pd.Series(marking_row)
        )
        if "Mean GOE bias" in row.index and pd.notna(row["Mean GOE bias"]):
            bias = float(row["Mean GOE bias"])
            phrase = _bias_phrase(bias, neutral=bias_neutral)
            n_marks = int(row["Element marks"]) if "Element marks" in row.index else None
            suffix = f" across {n_marks:,} element marks." if n_marks else "."
            lines.append(
                f"Overall GOE bias is **{bias:+.3f}** ({phrase} on average){suffix}"
            )
        if "Marking score" in row.index and pd.notna(row["Marking score"]):
            m_score = float(row["Marking score"])
            lines.append(
                f"Overall marking score **{m_score:.3f}** "
                "(√(mean(m²)); lower = closer to the control-score model)."
            )

    if (
        not jd.empty
        and "Partial marking score" in jd.columns
        and "Discipline" in jd.columns
        and "Element marks" in jd.columns
    ):
        jd_sorted = jd.sort_values("Partial marking score", ascending=False)
        reliable = jd_sorted.loc[jd_sorted["Element marks"] >= min_marks]
        pick = reliable if not reliable.empty else jd_sorted
        top = pick.iloc[0]
        disc = str(top["Discipline"])
        pscore = float(top["Partial marking score"])
        n = int(top["Element marks"])
        lines.append(
            f"Largest discipline-level normalized deviation: **{disc}** "
            f"(partial score {pscore:.3f}{_n_suffix(n, min_marks=min_marks)})."
        )
        if len(pick) > 1:
            bottom = pick.sort_values("Partial marking score", ascending=True).iloc[0]
            if str(bottom["Discipline"]) != disc:
                lines.append(
                    f"Closest to the model by discipline: **{bottom['Discipline']}** "
                    f"(partial score {float(bottom['Partial marking score']):.3f}, "
                    f"{int(bottom['Element marks']):,} marks)."
                )

        if "Mean GOE bias" in jd.columns:
            jd_bias = jd.loc[jd["Element marks"] >= min_marks]
            if jd_bias.empty:
                jd_bias = jd
            generous = jd_bias.loc[jd_bias["Mean GOE bias"].idxmax()]
            stingy = jd_bias.loc[jd_bias["Mean GOE bias"].idxmin()]
            g_bias = float(generous["Mean GOE bias"])
            s_bias = float(stingy["Mean GOE bias"])
            if g_bias > bias_neutral and str(generous["Discipline"]) != str(
                stingy["Discipline"]
            ):
                lines.append(
                    f"Most generous by discipline: **{generous['Discipline']}** "
                    f"({g_bias:+.3f} GOE vs median{_n_suffix(int(generous['Element marks']), min_marks=min_marks)})."
                )
            if s_bias < -bias_neutral and str(generous["Discipline"]) != str(
                stingy["Discipline"]
            ):
                lines.append(
                    f"Most stingy by discipline: **{stingy['Discipline']}** "
                    f"({s_bias:+.3f} GOE vs median{_n_suffix(int(stingy['Element marks']), min_marks=min_marks)})."
                )

    if (
        not je.empty
        and "Partial marking score" in je.columns
        and "Discipline" in je.columns
        and "Element type" in je.columns
        and "Element marks" in je.columns
    ):
        je_sorted = je.sort_values("Partial marking score", ascending=False)
        reliable_elem = je_sorted.loc[je_sorted["Element marks"] >= min_marks]
        elem_pool = reliable_elem if not reliable_elem.empty else je_sorted
        top_elems = elem_pool.head(top_n)
        parts: list[str] = []
        for _, erow in top_elems.iterrows():
            label = _slice_label(str(erow["Discipline"]), str(erow["Element type"]))
            parts.append(
                f"**{label}** ({float(erow['Partial marking score']):.3f}, "
                f"{int(erow['Element marks']):,} marks)"
            )
        if parts:
            lines.append(
                "Largest normalized deviations by element type: "
                + "; ".join(parts)
                + "."
            )

        if "Mean GOE bias" in je.columns:
            je_bias = je.loc[je["Element marks"] >= min_marks].copy()
            if je_bias.empty:
                je_bias = je.copy()
            je_bias["abs_bias"] = je_bias["Mean GOE bias"].abs()
            directional = je_bias.loc[
                je_bias["abs_bias"] >= bias_neutral
            ].sort_values("abs_bias", ascending=False)
            if not directional.empty:
                drow = directional.iloc[0]
                dbias = float(drow["Mean GOE bias"])
                dlabel = _slice_label(str(drow["Discipline"]), str(drow["Element type"]))
                lines.append(
                    f"Strongest directional bias by element type: **{dlabel}** "
                    f"({_bias_phrase(dbias, neutral=bias_neutral)}, {dbias:+.3f} GOE"
                    f"{_n_suffix(int(drow['Element marks']), min_marks=min_marks)})."
                )

        small = je_sorted.loc[je_sorted["Element marks"] < min_marks]
        if not small.empty and len(small) <= 5:
            labels = [
                _slice_label(str(r["Discipline"]), str(r["Element type"]))
                for _, r in small.head(3).iterrows()
            ]
            lines.append(
                "Low mark counts for some element types "
                f"(<{min_marks} marks), e.g. {', '.join(labels)} — treat those slices cautiously."
            )
        elif not small.empty:
            lines.append(
                f"{len(small)} element-type slice(s) have fewer than {min_marks} marks; "
                "prioritize slices with larger counts when interpreting."
            )

    if (
        not jg.empty
        and "Partial marking score" in jg.columns
        and "Discipline" in jg.columns
        and "Element type" in jg.columns
        and "Control GOE" in jg.columns
        and "Element marks" in jg.columns
    ):
        jg_sorted = jg.sort_values("Partial marking score", ascending=False)
        reliable_bins = jg_sorted.loc[jg_sorted["Element marks"] >= min_bin_marks]
        bin_pool = reliable_bins if not reliable_bins.empty else jg_sorted
        top_bins = bin_pool.head(top_n)
        parts: list[str] = []
        for _, brow in top_bins.iterrows():
            label = _slice_label(
                str(brow["Discipline"]),
                str(brow["Element type"]),
                control_goe=int(brow["Control GOE"]),
            )
            parts.append(
                f"**{label}** ({float(brow['Partial marking score']):.3f}, "
                f"{int(brow['Element marks']):,} marks)"
            )
        if parts:
            bin_threshold = (
                f" (bins with ≥{min_bin_marks:,} marks only)"
                if not reliable_bins.empty
                else ""
            )
            lines.append(
                "Largest normalized deviations by control GOE range"
                + bin_threshold
                + ": "
                + "; ".join(parts)
                + "."
            )

        if "Mean GOE bias" in jg.columns and not je.empty and "Element marks" in je.columns:
            focus = je.sort_values("Element marks", ascending=False).head(1)
            if not focus.empty:
                frow = focus.iloc[0]
                fdisc = str(frow["Discipline"])
                ftype = str(frow["Element type"])
                subset = jg.loc[
                    (jg["Discipline"] == fdisc) & (jg["Element type"] == ftype)
                ].copy()
                subset = subset.loc[subset["Element marks"] >= min_bin_marks]
                if len(subset) >= 2:
                    high = subset.loc[subset["Partial marking score"].idxmax()]
                    low = subset.loc[subset["Partial marking score"].idxmin()]
                    if int(high["Control GOE"]) != int(low["Control GOE"]):
                        lines.append(
                            f"Within **{fdisc} — {ftype}**, normalized deviation "
                            f"(√(mean(m²)) after σ̂ scaling) is highest at control GOE "
                            f"**{int(high['Control GOE']):+d}** "
                            f"(partial {float(high['Partial marking score']):.3f}, "
                            f"{int(high['Element marks']):,} marks) vs lowest at "
                            f"**{int(low['Control GOE']):+d}** "
                            f"(partial {float(low['Partial marking score']):.3f}, "
                            f"{int(low['Element marks']):,} marks), among GOE bins "
                            f"with ≥{min_bin_marks:,} marks."
                        )
                    bias_high = subset.loc[subset["Mean GOE bias"].idxmax()]
                    bias_low = subset.loc[subset["Mean GOE bias"].idxmin()]
                    if (
                        float(bias_high["Mean GOE bias"]) > bias_neutral
                        and float(bias_low["Mean GOE bias"]) < -bias_neutral
                        and int(bias_high["Control GOE"]) != int(bias_low["Control GOE"])
                    ):
                        lines.append(
                            f"For **{fdisc} — {ftype}**, GOE bias swings from "
                            f"**{float(bias_low['Mean GOE bias']):+.3f}** at control GOE "
                            f"{int(bias_low['Control GOE']):+d} to "
                            f"**{float(bias_high['Mean GOE bias']):+.3f}** at "
                            f"{int(bias_high['Control GOE']):+d} "
                            f"(among GOE bins with ≥{min_bin_marks:,} marks)."
                        )

    if not lines and jd.empty and je.empty and jg.empty:
        return []

    return lines
