"""
Short narrative takeaways from element-deviation judge drill-down tables.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def _slice_label(discipline: str, element_type: str | None = None) -> str:
    if element_type:
        return f"{discipline} — {element_type}"
    return discipline


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
    *,
    min_marks: int = 30,
    top_n: int = 3,
    bias_neutral: float = 0.02,
) -> list[str]:
    """
    Build bullet-point takeaways from per-judge discipline and element-type tables.

    ``discipline_detail`` / ``element_detail`` use the display column names from
    ``compute_judge_discipline_breakdown`` (``Judge``, ``Partial marking score``, etc.).
    """
    lines: list[str] = []
    jd = discipline_detail if isinstance(discipline_detail, pd.DataFrame) else pd.DataFrame()
    je = element_detail if isinstance(element_detail, pd.DataFrame) else pd.DataFrame()

    if "Judge" in jd.columns:
        jd = jd.loc[jd["Judge"] == judge_name]
    if "Judge" in je.columns:
        je = je.loc[je["Judge"] == judge_name]

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

    if not jd.empty and "Partial marking score" in jd.columns:
        jd_sorted = jd.sort_values("Partial marking score", ascending=False)
        reliable = jd_sorted.loc[jd_sorted["Element marks"] >= min_marks]
        pick = reliable if not reliable.empty else jd_sorted
        top = pick.iloc[0]
        disc = str(top["Discipline"])
        pscore = float(top["Partial marking score"])
        n = int(top["Element marks"])
        lines.append(
            f"Largest discipline-level deviation: **{disc}** "
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

    if not je.empty and "Partial marking score" in je.columns:
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
                "Largest deviations by element type: " + "; ".join(parts) + "."
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

    if not lines and jd.empty and je.empty:
        return []

    return lines
