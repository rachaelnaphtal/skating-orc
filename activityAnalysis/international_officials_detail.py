"""Per-official appointment detail report for the international officials app."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import pandas as pd
import streamlit as st

try:
    from app_query_params import qp_get, qp_get_int
except ModuleNotFoundError:
    import os
    import sys

    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from app_query_params import qp_get, qp_get_int

try:
    from activityAnalysis.international_listing_seasons import (
        competition_year_matches_seasons,
        format_usfs_season_code,
    )
    from activityAnalysis.international_officials_data import (
        _nullable_int_for_sql,
        get_international_official_activity_detail,
        load_international_panel_segments_bulk,
    )
    from activityAnalysis.international_requirements import (
        RequirementEvaluation,
        _batch_isu_listing_keys,
        _international_level_id,
        _isu_level_id,
        directory_listing_tier_for_level,
        evaluate_requirements_for_appointment,
        format_competition_alternatives_detail,
        listing_tier_display_label,
        should_evaluate_promote_requirements,
    )
    from activityAnalysis.load_activity_data import get_engine
except ModuleNotFoundError:
    from international_listing_seasons import (
        competition_year_matches_seasons,
        format_usfs_season_code,
    )
    from international_officials_data import (
        _nullable_int_for_sql,
        get_international_official_activity_detail,
        load_international_panel_segments_bulk,
    )
    from international_requirements import (
        RequirementEvaluation,
        _batch_isu_listing_keys,
        _international_level_id,
        _isu_level_id,
        directory_listing_tier_for_level,
        evaluate_requirements_for_appointment,
        format_competition_alternatives_detail,
        listing_tier_display_label,
        should_evaluate_promote_requirements,
    )
    from load_activity_data import get_engine


_SCOPE_FILTER_ALL = "(All scopes)"
_SEASON_FILTER_ALL = None


def competition_scope_filter_options(detail: pd.DataFrame) -> list[str]:
    opts = [_SCOPE_FILTER_ALL]
    if detail.empty:
        return opts
    if (detail["competition_scope"] == "International").any():
        opts.append("International")
    if (detail["competition_scope"] == "National").any():
        opts.append("National")
    return opts


def filter_detail_by_competition_scope(detail: pd.DataFrame, choice: str) -> pd.DataFrame:
    if detail.empty or not choice or choice == _SCOPE_FILTER_ALL:
        return detail
    return detail.loc[detail["competition_scope"] == choice].reset_index(drop=True)


def season_codes_in_detail(detail: pd.DataFrame, season_codes: list[int]) -> list[int]:
    if detail.empty:
        return []
    return [
        code
        for code in sorted(season_codes)
        if detail["competition_year"]
        .apply(lambda y: competition_year_matches_seasons(y, [code]))
        .any()
    ]


def filter_detail_by_season(
    detail: pd.DataFrame,
    season_code: int | None,
) -> pd.DataFrame:
    if detail.empty or season_code is None:
        return detail
    mask = detail["competition_year"].apply(
        lambda y: competition_year_matches_seasons(y, [season_code])
    )
    return detail.loc[mask].reset_index(drop=True)


def discipline_id_to_param(discipline_id: Any) -> str:
    disc = _nullable_int_for_sql(discipline_id)
    return str(disc) if disc is not None else "none"


def discipline_id_from_param(text: str | None) -> int | None:
    if not text or text.lower() == "none":
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def appointment_detail_query_params(
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
    listing_season_code: int,
    report_season_window: int,
    active_only: bool,
    include_requirements: bool,
) -> dict[str, str]:
    return {
        "view": "appointment",
        "oid": str(int(official_id)),
        "atid": str(int(appointment_type_id)),
        "did": discipline_id_to_param(discipline_id),
        "listing": str(int(listing_season_code)),
        "seasons": str(int(report_season_window)),
        "active": "1" if active_only else "0",
        "req": "1" if include_requirements else "0",
    }


def appointment_detail_url(
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
    listing_season_code: int,
    report_season_window: int,
    active_only: bool,
    include_requirements: bool,
) -> str:
    """Full URL for a per-appointment report (for ``LinkColumn``)."""
    params = appointment_detail_query_params(
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        listing_season_code=listing_season_code,
        report_season_window=report_season_window,
        active_only=active_only,
        include_requirements=include_requirements,
    )
    try:
        base = urlsplit(str(st.context.url))
    except Exception:
        base = urlsplit("http://localhost:8501")
    query = urlencode(params)
    return urlunsplit((base.scheme, base.netloc, base.path, query, ""))


def parse_appointment_detail_params() -> dict[str, Any] | None:
    if qp_get("view") != "appointment":
        return None
    official_id = qp_get_int("oid")
    appointment_type_id = qp_get_int("atid")
    if official_id is None or appointment_type_id is None:
        return None
    return {
        "official_id": official_id,
        "appointment_type_id": appointment_type_id,
        "discipline_id": discipline_id_from_param(qp_get("did")),
        "listing_season_code": qp_get_int("listing"),
        "report_season_window": qp_get_int("seasons"),
        "active_only": qp_get("active") != "0",
        "include_requirements": qp_get("req") != "0",
    }


def clear_appointment_detail_params() -> None:
    for key in ("view", "oid", "atid", "did", "listing", "seasons", "active", "req"):
        if key in st.query_params:
            del st.query_params[key]


def _primary_requirement_evaluation(
    evals: list[RequirementEvaluation],
) -> RequirementEvaluation | None:
    """The one rule set that applies; prefer applicable over N/A."""
    if not evals:
        return None
    applicable = [e for e in evals if not e.not_applicable]
    return applicable[0] if applicable else evals[0]


def _streamlit_safe_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Build a plain string dataframe so Streamlit/pyarrow never sees numpy int64 metadata."""
    if df.empty:
        return pd.DataFrame(columns=list(df.columns))

    work = df.reset_index(drop=True).copy()
    if "competition_year" in work.columns:
        if "Season" in work.columns:
            work = work.drop(columns=["competition_year"])
        else:
            work = work.rename(columns={"competition_year": "Season"})

    columns = list(work.columns)
    data: dict[str, list[str]] = {}
    for col in columns:
        if col == "Season":
            data[col] = [
                format_usfs_season_code(int(v)) if pd.notna(v) else ""
                for v in work[col]
            ]
        else:
            data[col] = ["" if pd.isna(v) else str(v) for v in work[col]]

    return pd.DataFrame(data, columns=columns)


def _render_qualifying_activity_table(activity: pd.DataFrame | None, *, caption: str) -> None:
    if activity is None or activity.empty:
        return
    st.caption(caption)
    display = activity.rename(
        columns={
            "competition_year": "Season",
            "competition_name": "Competition",
            "competition_scope": "Comp scope",
            "competition_type": "Comp type",
            "panel_roles": "Panel role(s)",
        }
    )
    show_cols = [
        c
        for c in [
            "Season",
            "Competition",
            "Comp scope",
            "Comp type",
            "Panel role(s)",
        ]
        if c in display.columns
    ]
    st.dataframe(
        _streamlit_safe_dataframe(display[show_cols]),
        width="stretch",
        hide_index=True,
        column_config={
            "Competition": st.column_config.TextColumn("Competition", width="large"),
        },
    )


def _render_requirement_evaluation(title: str, ev: RequirementEvaluation) -> None:
    """Single rule set — label/value rows instead of a wide table."""
    st.markdown(f"### {title}")

    if ev.not_applicable:
        st.markdown(f"**{ev.label}** ({ev.isu_rule_ref})")
        st.info(ev.not_applicable_reason or "This requirement profile does not apply.")
        return

    status = "Meets requirements" if ev.meets else "Does not meet requirements"
    st.markdown(f"**{ev.label}** ({ev.isu_rule_ref})")
    st.markdown(f"**Overall:** {status}")
    if ev.summary_note and not ev.meets:
        st.caption(ev.summary_note)

    seasons = ", ".join(format_usfs_season_code(c) for c in ev.season_codes)
    st.caption(f"Season window: {seasons}")

    if not ev.rule_results:
        return

    for rule in ev.rule_results:
        met = "Yes" if rule.met else "No"
        with st.container(border=True):
            st.markdown(f"**{rule.display_label}**")
            st.write(f"Met: **{met}**")
            if rule.metric == "competition_alternatives":
                st.markdown("**Options:**")
                for line in format_competition_alternatives_detail(
                    rule.detail, met=rule.met
                ):
                    st.markdown(f"- {line}")
            else:
                st.write(f"Progress: {rule.detail}")
            if (
                rule.qualifying_competitions is not None
                and not rule.qualifying_competitions.empty
                and len(ev.rule_results) > 1
            ):
                _render_qualifying_activity_table(
                    rule.qualifying_competitions,
                    caption="Competitions for this rule",
                )

    _render_qualifying_activity_table(
        ev.qualifying_activity,
        caption=(
            "Qualifying competitions (includes other panel roles where the requirement allows, "
            f"seasons {', '.join(format_usfs_season_code(c) for c in ev.season_codes)})"
        ),
    )


def render_appointment_detail_report(
    *,
    summary_row: pd.Series,
    listing_season_code: int,
    report_season_window: int,
    report_season_codes: list[int],
    active_only: bool,
    include_requirements: bool,
    panel_bulk: pd.DataFrame | None = None,
) -> None:
    """Full activity + requirement breakdown for one summary row."""
    official_id = int(summary_row["official_id"])
    appointment_type_id = int(summary_row["appointment_type_id"])
    discipline_id = _nullable_int_for_sql(summary_row.get("discipline_id"))
    official_name = (summary_row.get("official_name") or "").strip() or f"Official {official_id}"
    appointment_type = summary_row.get("appointment_type") or ""
    discipline = summary_row.get("discipline") or "—"
    appointment_level = summary_row.get("appointment_level") or ""
    appointment_level_id = summary_row.get("appointment_level_id")

    if st.button("← Back to summary", type="secondary"):
        clear_appointment_detail_params()
        st.rerun()

    st.title(official_name)
    st.caption(
        f"{appointment_type} · {discipline} · {appointment_level or 'Level unknown'}"
        f" · Listing {format_usfs_season_code(listing_season_code)}"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Competitions", int(summary_row.get("competition_count") or 0))
    c2.metric("Segments", int(summary_row.get("segment_count") or 0))
    panel = panel_bulk
    if panel is None:
        panel = load_international_panel_segments_bulk([official_id])

    from sqlalchemy.orm import Session

    with Session(get_engine()) as session:
        isu_level_id = _isu_level_id(session)
        international_level_id = _international_level_id(session)
    isu_listing_keys = _batch_isu_listing_keys([official_id])
    show_promote = should_evaluate_promote_requirements(
        official_id,
        appointment_type_id,
        discipline_id,
        appointment_level,
        appointment_level_id,
        isu_level_id=isu_level_id,
        isu_listing_keys=isu_listing_keys,
    )

    if include_requirements:
        c3.metric("Maintain", summary_row.get("maintain") or "N/A")
        promote_val = summary_row.get("promote") or ("—" if not show_promote else "N/A")
        c4.metric("Promote", promote_val)

    st.caption(
        "Activity seasons shown: "
        + ", ".join(format_usfs_season_code(c) for c in report_season_codes)
    )

    if include_requirements:
        maintain_evals = evaluate_requirements_for_appointment(
            official_id,
            appointment_type_id,
            discipline_id,
            "maintain",
            listing_season_code=listing_season_code,
            panel_bulk=panel,
            isu_level_id=isu_level_id,
            isu_listing_keys=isu_listing_keys,
        )
        listing_tier = directory_listing_tier_for_level(
            appointment_level,
            level_id=appointment_level_id,
            isu_level_id=isu_level_id,
            international_level_id=international_level_id,
        )
        tier_rules = [e for e in maintain_evals if e.listing_tier == listing_tier]
        maintain_primary = _primary_requirement_evaluation(tier_rules)
        if maintain_primary is None:
            st.markdown(f"### Maintain ({listing_tier_display_label(listing_tier)} listing)")
            st.info("No matching requirement rules configured.")
        else:
            _render_requirement_evaluation(
                f"Maintain ({listing_tier_display_label(listing_tier)} listing)",
                maintain_primary,
            )

        if not show_promote:
            st.markdown("### Promote")
            st.info(
                "Promotion checks apply to International-level appointments not yet ISU-listed."
            )
        else:
            promote_evals = evaluate_requirements_for_appointment(
                official_id,
                appointment_type_id,
                discipline_id,
                "promote",
                listing_season_code=listing_season_code,
                panel_bulk=panel,
                isu_level_id=isu_level_id,
                isu_listing_keys=isu_listing_keys,
            )
            promote_applicable = [e for e in promote_evals if not e.not_applicable]
            promote_primary = _primary_requirement_evaluation(promote_applicable)
            if promote_primary is None:
                st.markdown("### Promote to ISU")
                st.info("No promotion requirement profile applies.")
            else:
                _render_requirement_evaluation("Promote to ISU", promote_primary)

    st.markdown("### Panel activity (this appointment)")
    detail = get_international_official_activity_detail(
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        official_id=official_id,
        active_appointments_only=active_only,
        panel_bulk=panel,
        season_codes=report_season_codes,
    )
    if detail.empty:
        st.info("No matching panel segments in the selected seasons.")
        return

    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        scope_options = competition_scope_filter_options(detail)
        scope_filter = st.selectbox(
            "Competition scope",
            options=scope_options,
            index=0,
            help="Filter panel activity to international or national qualifying competitions.",
        )
    with filter_col2:
        season_options: list[int | None] = [_SEASON_FILTER_ALL] + season_codes_in_detail(
            detail, report_season_codes
        )
        season_filter = st.selectbox(
            "Season",
            options=season_options,
            index=0,
            format_func=lambda c: "(All seasons)"
            if c is None
            else f"{format_usfs_season_code(c)} ({c})",
            help="Filter panel activity to one report season.",
        )

    detail = filter_detail_by_competition_scope(detail, scope_filter)
    detail = filter_detail_by_season(detail, season_filter)
    if detail.empty:
        st.info("No panel segments match the selected filters.")
        return

    detail_display = detail.rename(
        columns={
            "competition_year": "Season",
            "competition_name": "Competition",
            "competition_scope": "Comp scope",
            "competition_type": "Comp type",
            "segment_name": "Segment",
            "segment_level": "Level",
            "segment_discipline": "Segment discipline",
            "appointment_type": "Appointment",
        }
    )
    show_cols = [
        c
        for c in [
            "Season",
            "Competition",
            "Comp scope",
            "Comp type",
            "Segment",
            "Level",
            "Segment discipline",
            "Appointment",
            "start_date",
            "end_date",
            "results_url",
        ]
        if c in detail_display.columns
    ]
    st.dataframe(
        _streamlit_safe_dataframe(detail_display[show_cols]),
        width="stretch",
        hide_index=True,
        column_config={
            "Competition": st.column_config.TextColumn("Competition", width="large"),
            "Segment": st.column_config.TextColumn("Segment", width="medium"),
            "results_url": st.column_config.LinkColumn("Results", display_text="Open"),
        },
    )
