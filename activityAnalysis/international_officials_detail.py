"""Per-official appointment detail report for the international officials app."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import pandas as pd
import streamlit as st

try:
    from app_query_params import qp_get, qp_get_int, sync_query_params
except ModuleNotFoundError:
    import os
    import sys

    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from app_query_params import qp_get, qp_get_int, sync_query_params

try:
    from activityAnalysis.international_listing_seasons import (
        format_listing_reference_july1,
        promote_first_eligible_column_label,
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
    from activityAnalysis.international_officials_report import (
        AppointmentDetailContext,
        _demographics_display_value,
        appointment_detail_pdf_filename,
        build_appointment_detail_context,
        build_appointment_detail_pdf,
    )
    from activityAnalysis.international_segment_eligibility import (
        enrich_panel_with_rule411_eligibility,
    )
    from ijs_results_urls import results_page_url
    from activityAnalysis.load_activity_data import get_engine
except ModuleNotFoundError:
    from international_listing_seasons import (
        format_listing_reference_july1,
        promote_first_eligible_column_label,
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
    from international_officials_report import (
        AppointmentDetailContext,
        _demographics_display_value,
        appointment_detail_pdf_filename,
        build_appointment_detail_context,
        build_appointment_detail_pdf,
    )
    from international_segment_eligibility import enrich_panel_with_rule411_eligibility
    from ijs_results_urls import results_page_url
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
) -> dict[str, str]:
    return {
        "view": "appointment",
        "oid": str(int(official_id)),
        "atid": str(int(appointment_type_id)),
        "did": discipline_id_to_param(discipline_id),
        "listing": str(int(listing_season_code)),
        "seasons": str(int(report_season_window)),
        "active": "1" if active_only else "0",
    }


def appointment_detail_url(
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
    listing_season_code: int,
    report_season_window: int,
    active_only: bool,
) -> str:
    """Full URL for a per-appointment report (for ``LinkColumn``)."""
    params = appointment_detail_query_params(
        official_id=official_id,
        appointment_type_id=appointment_type_id,
        discipline_id=discipline_id,
        listing_season_code=listing_season_code,
        report_season_window=report_season_window,
        active_only=active_only,
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
    }


def clear_appointment_detail_params() -> None:
    for key in ("view", "oid", "atid", "did", "listing", "seasons", "active", "req"):
        if key in st.query_params:
            del st.query_params[key]


def _appointment_nav_key(
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
) -> tuple[int, int, int | None]:
    return (
        int(official_id),
        int(appointment_type_id),
        _nullable_int_for_sql(discipline_id),
    )


def _appointment_nav_label(row: pd.Series) -> str:
    name = (row.get("official_name") or "").strip() or f"Official {row['official_id']}"
    appt = (row.get("appointment_type") or "Appointment").strip()
    disc = row.get("discipline")
    if disc is not None and str(disc).strip() and not pd.isna(disc):
        return f"{name} — {appt} — {disc}"
    return f"{name} — {appt}"


def _navigate_to_appointment_detail(
    *,
    official_id: int,
    appointment_type_id: int,
    discipline_id: Any,
    listing_season_code: int,
    report_season_window: int,
    active_only: bool,
) -> None:
    sync_query_params(
        **appointment_detail_query_params(
            official_id=official_id,
            appointment_type_id=appointment_type_id,
            discipline_id=discipline_id,
            listing_season_code=listing_season_code,
            report_season_window=report_season_window,
            active_only=active_only,
        )
    )
    st.rerun()


def render_detail_appointment_nav(
    *,
    nav_appointments: pd.DataFrame,
    current_official_id: int,
    current_appointment_type_id: int,
    current_discipline_id: Any,
    listing_season_code: int,
    report_season_window: int,
    active_only: bool,
) -> None:
    """Selectbox + prev/next to jump between appointment detail reports."""
    if nav_appointments.empty:
        return

    sorted_nav = nav_appointments.sort_values(
        by=["official_name", "appointment_type", "discipline"],
        na_position="last",
    ).reset_index(drop=True)
    options: list[tuple[int, int, int | None]] = []
    labels: dict[tuple[int, int, int | None], str] = {}
    for _, row in sorted_nav.iterrows():
        key = _appointment_nav_key(
            int(row["official_id"]),
            int(row["appointment_type_id"]),
            row["discipline_id"],
        )
        if key in labels:
            continue
        options.append(key)
        labels[key] = _appointment_nav_label(row)

    if len(options) <= 1:
        return

    current_key = _appointment_nav_key(
        current_official_id,
        current_appointment_type_id,
        current_discipline_id,
    )
    if current_key not in options:
        options.insert(0, current_key)
        labels[current_key] = "Current appointment"

    current_index = options.index(current_key)
    prev_col, select_col, next_col = st.columns([1, 6, 1])
    with prev_col:
        if st.button("◀", help="Previous appointment", disabled=current_index <= 0):
            prev = options[current_index - 1]
            _navigate_to_appointment_detail(
                official_id=prev[0],
                appointment_type_id=prev[1],
                discipline_id=prev[2],
                listing_season_code=listing_season_code,
                report_season_window=report_season_window,
                active_only=active_only,
            )
    with next_col:
        if st.button(
            "▶",
            help="Next appointment",
            disabled=current_index >= len(options) - 1,
        ):
            nxt = options[current_index + 1]
            _navigate_to_appointment_detail(
                official_id=nxt[0],
                appointment_type_id=nxt[1],
                discipline_id=nxt[2],
                listing_season_code=listing_season_code,
                report_season_window=report_season_window,
                active_only=active_only,
            )
    with select_col:
        selected = st.selectbox(
            "Jump to appointment",
            options=options,
            index=current_index,
            format_func=lambda k: labels[k],
            key="intl_officials_detail_nav",
        )
    if selected != current_key:
        _navigate_to_appointment_detail(
            official_id=selected[0],
            appointment_type_id=selected[1],
            discipline_id=selected[2],
            listing_season_code=listing_season_code,
            report_season_window=report_season_window,
            active_only=active_only,
        )


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
            "results_url": "Results",
        }
    )
    if "Results" in display.columns:
        display["Results"] = display["Results"].map(
            lambda u: results_page_url(u) if pd.notna(u) else None
        )
    show_cols = [
        c
        for c in [
            "Season",
            "Competition",
            "Comp scope",
            "Comp type",
            "Panel role(s)",
            "Results",
        ]
        if c in display.columns
    ]
    st.dataframe(
        _streamlit_safe_dataframe(display[show_cols]),
        width="stretch",
        hide_index=True,
        column_config={
            "Competition": st.column_config.TextColumn("Competition", width="large"),
            "Results": st.column_config.LinkColumn(
                "Results",
                display_text="Open",
                help="Competition results page used to verify panel service.",
            ),
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
    st.caption(
        f"Season window: {seasons}. "
        "International segments (types 15–17) count only when they meet ISU service definitions "
        "(Singles ≥6 entries; Pairs/Dance ≥4; Synchronized ≥2 ISU Members only; "
        "≥2 ISU Members for Singles/Pairs/Dance when nations can be verified from names)."
    )

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


def _requirement_metric_label(ev: RequirementEvaluation | None) -> str:
    if ev is None:
        return "N/A"
    if ev.not_applicable:
        return "N/A"
    return "Yes" if ev.meets else "No"


def render_appointment_detail_report(
    *,
    summary_row: pd.Series,
    listing_season_code: int,
    report_season_window: int,
    report_season_codes: list[int],
    active_only: bool,
    panel_bulk: pd.DataFrame | None = None,
    nav_appointments: pd.DataFrame | None = None,
    detail_context: AppointmentDetailContext | None = None,
) -> None:
    """Full activity + requirement breakdown for one summary row."""
    ctx = detail_context or build_appointment_detail_context(
        summary_row,
        listing_season_code=listing_season_code,
        report_season_codes=report_season_codes,
        active_only=active_only,
        panel_bulk=panel_bulk,
    )
    official_id = ctx.official_id
    appointment_type_id = ctx.appointment_type_id
    discipline_id = ctx.discipline_id
    official_name = ctx.official_name
    appointment_type = ctx.appointment_type
    discipline = ctx.discipline
    appointment_level = ctx.appointment_level
    maintain_primary = ctx.maintain_primary
    promote_primary = ctx.promote_primary
    show_promote = ctx.show_promote
    listing_tier = ctx.listing_tier

    nav_col, back_col, pdf_col = st.columns([5, 1, 2])
    with back_col:
        if st.button("← Back", type="secondary", help="Back to summary"):
            clear_appointment_detail_params()
            st.rerun()
    with pdf_col:
        pdf_cache_key = (
            official_id,
            appointment_type_id,
            discipline_id,
            listing_season_code,
            report_season_window,
        )
        if st.session_state.get("intl_detail_pdf_key") != pdf_cache_key:
            st.session_state.pop("intl_detail_pdf", None)
            st.session_state.pop("intl_detail_pdf_name", None)

        try:
            if st.button(
                "Download PDF",
                type="primary",
                use_container_width=True,
                key="intl_detail_pdf_btn",
            ):
                with st.spinner("Building PDF…"):
                    st.session_state["intl_detail_pdf"] = build_appointment_detail_pdf(ctx)
                    st.session_state["intl_detail_pdf_name"] = appointment_detail_pdf_filename(
                        ctx
                    )
                    st.session_state["intl_detail_pdf_key"] = pdf_cache_key

            if (
                st.session_state.get("intl_detail_pdf_key") == pdf_cache_key
                and st.session_state.get("intl_detail_pdf")
            ):
                st.download_button(
                    "Save PDF",
                    data=st.session_state["intl_detail_pdf"],
                    file_name=st.session_state["intl_detail_pdf_name"],
                    mime="application/pdf",
                    use_container_width=True,
                    on_click="ignore",
                )
        except RuntimeError as exc:
            st.caption(str(exc))

    if nav_appointments is not None:
        render_detail_appointment_nav(
            nav_appointments=nav_appointments,
            current_official_id=official_id,
            current_appointment_type_id=appointment_type_id,
            current_discipline_id=discipline_id,
            listing_season_code=listing_season_code,
            report_season_window=report_season_window,
            active_only=active_only,
        )

    st.title(official_name)
    st.caption(
        f"{appointment_type} · {discipline} · {appointment_level or 'Level unknown'}"
        f" · Listing {format_usfs_season_code(listing_season_code)}"
    )

    as_of = format_listing_reference_july1(listing_season_code)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Competitions", ctx.competition_count)
    c2.metric("Segments", ctx.segment_count)
    c3.metric("Maintain", _requirement_metric_label(maintain_primary))
    c4.metric(
        "Promote",
        _requirement_metric_label(promote_primary) if show_promote else "—",
    )
    d1, d2 = st.columns(2)
    d1.metric(f"Age {as_of}", _demographics_display_value(ctx.age_as_of_listing))
    d2.metric(
        f"Years in grade {as_of}",
        _demographics_display_value(ctx.years_in_grade),
    )
    if ctx.grade_date is not None:
        st.caption(f"Grade date (achieved or appointed): {ctx.grade_date.isoformat()}")
    if ctx.show_promote:
        st.metric(
            promote_first_eligible_column_label(),
            ctx.promote_first_eligible_display,
        )

    st.caption(
        "Activity seasons shown: "
        + ", ".join(format_usfs_season_code(c) for c in report_season_codes)
        + f". Age and years in grade are {as_of}."
        + (
            " First promote year (years) is the July 1 listing year when all promote year requirements are met; "
            "competition requirements may still apply."
            if show_promote
            else ""
        )
    )

    if maintain_primary is None:
        st.markdown(f"### Maintain ({listing_tier_display_label(listing_tier)} listing)")
        st.info("No matching requirement rules configured.")
    else:
        _render_requirement_evaluation(
            f"Maintain ({listing_tier_display_label(listing_tier)} listing)",
            maintain_primary,
        )

    if ctx.promote_note:
        st.markdown("### Promote")
        st.info(ctx.promote_note)
    elif promote_primary is None:
        st.markdown("### Promote to ISU")
        st.info("No promotion requirement profile applies.")
    else:
        _render_requirement_evaluation("Promote to ISU", promote_primary)

    st.markdown("### Panel activity (this appointment)")
    st.caption(
        "Rule 411 column shows whether an international segment meets ISU entry minimums "
        "for service credit (Singles ≥6, Pairs/Dance ≥4, Synchronized ≥2 ISU Members; "
        "nations inferred from NOC suffixes on skater/team names when present)."
    )
    detail = enrich_panel_with_rule411_eligibility(ctx.panel_detail.copy())
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
            "rule411_entry_count": "Entries",
            "rule411_distinct_noc_count": "Nations",
            "rule411_status": "Rule 411",
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
            "Entries",
            "Nations",
            "Rule 411",
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
            "Rule 411": st.column_config.TextColumn(
                "Rule 411",
                help="Whether this international segment meets ISU Rule 411 entry/member minimums.",
                width="small",
            ),
            "results_url": st.column_config.LinkColumn("Results", display_text="Open"),
        },
    )
