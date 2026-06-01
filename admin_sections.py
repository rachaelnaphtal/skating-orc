"""
Admin-only Streamlit sections used by ``pages/admin.py`` (/admin).
"""

from __future__ import annotations

import os
import sys
import tempfile
import traceback as _tb
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import text as sqlt

from analytics_connection import get_analytics_safe
from officials_competition_types import format_officials_competition_type_select_label

_REPO_ROOT = Path(__file__).resolve().parent
_ACTIVITY = _REPO_ROOT / "activityAnalysis"
if str(_ACTIVITY) not in sys.path:
    sys.path.insert(0, str(_ACTIVITY))

import officials_directory_loader as _officials_directory_loader  # noqa: E402


def render_admin_password_gate() -> None:
    pw = os.environ.get("ADMIN_PASSWORD", "")
    if not pw:
        return
    if st.session_state.get("admin_authenticated"):
        return
    st.info("This section is password protected.")
    entered = st.text_input("Admin password", type="password", key="admin_gate_pw")
    if st.button("Unlock", key="admin_gate_unlock"):
        if entered == pw:
            st.session_state["admin_authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()


def render_merge_judges() -> None:
    st.subheader("Merge Judges")
    st.info(
        "If this is a **US judge** who only differs by protocol spelling or name history, "
        "try **Judge ↔ directory matcher** first — it links both scoring judge records to "
        "the same directory official without rewriting scores. Use merge here mainly when "
        "matcher is not a fit (e.g. you truly want one judge row removed or non‑US edge cases)."
    )
    st.write(
        "Use this when the same judge appears under two different names. "
        "All scores from the **duplicate** will be reassigned to the **judge to keep**, "
        "then the duplicate record will be deleted. This cannot be undone."
    )

    analytics = get_analytics_safe()
    judges = analytics.get_judges()
    if not judges:
        st.error("No judges found in database.")
        st.stop()

    judge_options = {name: judge_id for judge_id, name, location in judges}
    judge_names = list(judge_options.keys())

    col1, col2 = st.columns(2)
    with col1:
        keep_name = st.selectbox(
            "Judge to keep (primary)", judge_names, key="admin_merge_keep"
        )
    with col2:
        dupe_options = [n for n in judge_names if n != keep_name]
        dupe_name = st.selectbox(
            "Judge to merge & remove (duplicate)",
            dupe_options,
            key="admin_merge_dupe",
        )

    keep_id = judge_options[keep_name]
    dupe_id = judge_options[dupe_name]

    session = analytics.session

    pcs_count = session.execute(
        sqlt("SELECT COUNT(*) FROM pcs_score_per_judge WHERE judge_id = :id"),
        {"id": dupe_id},
    ).scalar()
    elem_count = session.execute(
        sqlt("SELECT COUNT(*) FROM element_score_per_judge WHERE judge_id = :id"),
        {"id": dupe_id},
    ).scalar()

    st.markdown("---")
    st.write("**Preview — scores that will be reassigned:**")
    preview_col1, preview_col2 = st.columns(2)
    with preview_col1:
        st.metric("PCS scores", pcs_count)
    with preview_col2:
        st.metric("Element scores", elem_count)

    if pcs_count == 0 and elem_count == 0:
        st.warning(
            f"**{dupe_name}** has no scores in the database. "
            "Only the judge record itself will be deleted."
        )

    st.markdown("---")
    confirmed = st.checkbox(
        f'I understand this will permanently merge "{dupe_name}" into "{keep_name}" '
        f'and delete the "{dupe_name}" record.',
        key="admin_merge_confirm",
    )

    if st.button("Execute Merge", disabled=not confirmed, type="primary", key="admin_merge_go"):
        try:
            session.execute(
                sqlt("UPDATE pcs_score_per_judge SET judge_id = :keep WHERE judge_id = :dupe"),
                {"keep": keep_id, "dupe": dupe_id},
            )
            session.execute(
                sqlt(
                    "UPDATE element_score_per_judge SET judge_id = :keep WHERE judge_id = :dupe"
                ),
                {"keep": keep_id, "dupe": dupe_id},
            )
            for tbl in ("judge_excess_anomalies_cache", "judge_summary_cache"):
                try:
                    session.execute(sqlt("SAVEPOINT merge_cache"))
                    session.execute(
                        sqlt(f"DELETE FROM {tbl} WHERE judge_id IN (:keep, :dupe)"),
                        {"keep": keep_id, "dupe": dupe_id},
                    )
                    session.execute(sqlt("RELEASE SAVEPOINT merge_cache"))
                except Exception:
                    session.execute(sqlt("ROLLBACK TO SAVEPOINT merge_cache"))
            session.execute(sqlt("DELETE FROM judge WHERE id = :dupe"), {"dupe": dupe_id})
            session.commit()
            st.cache_resource.clear()
            st.success(
                f"Done! **{pcs_count}** PCS scores and **{elem_count}** element scores "
                f"have been moved to **{keep_name}**. "
                f'The record for "{dupe_name}" has been deleted.'
            )
        except Exception as e:
            session.rollback()
            st.error(f"Merge failed and was rolled back: {e}")


def render_public_competition_officials_types_breakdown() -> None:
    st.subheader("Public competitions ↔ officials types")
    st.caption(
        "Counts **public.competition** rows linked to each **officials_analysis.competition_type**. "
        "Assign the link when using **Load Competition** on the main app, or update rows in SQL."
    )
    analytics = get_analytics_safe()
    df = analytics.get_public_competition_officials_type_breakdown()
    if df.empty:
        st.info(
            "No rows returned. Confirm PostgreSQL includes ``officials_analysis``, apply migration "
            "**activityAnalysis/migrations/006_public_competition_officials_competition_type.sql**, "
            "and that competition types exist from activity imports."
        )
        return
    df = df.copy()
    df["grouped_label"] = df.apply(
        lambda r: (
            format_officials_competition_type_select_label(
                int(r["officials_competition_type_id"]),
                str(r["officials_competition_type_name"]),
            )
            if pd.notna(r["officials_competition_type_id"])
            else str(r["officials_competition_type_name"])
        ),
        axis=1,
    )
    display = df.rename(
        columns={
            "grouped_label": "Grouped label",
            "officials_competition_type_id": "Officials type id",
            "officials_competition_type_name": "Officials type name",
            "public_competition_count": "Public competitions linked",
        }
    )
    display = display[
        [
            "Grouped label",
            "Officials type id",
            "Officials type name",
            "Public competitions linked",
        ]
    ]
    st.dataframe(display, width="stretch", hide_index=True)


def render_manage_judge_emails() -> None:
    from email_reports import ensure_email_table, get_email_list, upsert_email_list, delete_email_entry

    st.subheader("Manage Judge Emails")
    st.write(
        "Upload a spreadsheet (CSV or Excel) with two columns: **Name** and **Email**. "
        "Existing entries are updated by name; new entries are added. "
        "Column headers must be exactly `Name` and `Email`."
    )

    _em_session = get_analytics_safe().session
    ensure_email_table(_em_session)

    uploaded_email_file = st.file_uploader(
        "Upload Name/Email spreadsheet",
        type=["csv", "xlsx", "xls"],
        key="admin_email_list_upload",
    )
    if uploaded_email_file:
        try:
            if uploaded_email_file.name.endswith(".csv"):
                email_upload_df = pd.read_csv(uploaded_email_file)
            else:
                email_upload_df = pd.read_excel(uploaded_email_file)

            col_map = {}
            for col in email_upload_df.columns:
                if col.strip().lower() == "name":
                    col_map[col] = "judge_name"
                elif col.strip().lower() == "email":
                    col_map[col] = "email"

            if "judge_name" not in col_map.values() or "email" not in col_map.values():
                st.error(
                    f"Could not find 'Name' and 'Email' columns. "
                    f"Found: {list(email_upload_df.columns)}"
                )
            else:
                email_upload_df = email_upload_df.rename(columns=col_map)[["judge_name", "email"]]
                ins, upd = upsert_email_list(_em_session, email_upload_df)
                st.success(f"Done — {ins} new entries added, {upd} updated.")
        except Exception as _e:
            st.error(f"Failed to read file: {_e}")

    email_list_df = get_email_list(_em_session)
    if email_list_df.empty:
        st.info("No judge emails stored yet. Upload a spreadsheet above to get started.")
    else:
        st.write(f"**{len(email_list_df)} judges** in email list:")
        st.dataframe(
            email_list_df.rename(columns={"judge_name": "Name", "email": "Email"}),
            width="stretch",
            hide_index=True,
        )

        with st.expander("Remove an entry"):
            del_name = st.selectbox(
                "Select judge to remove",
                email_list_df["judge_name"].tolist(),
                key="admin_del_email_name",
            )
            if st.button("Remove", key="admin_del_email_btn"):
                delete_email_entry(_em_session, del_name)
                st.success(f'Removed "{del_name}" from the email list.')
                st.rerun()


def render_email_competition_reports() -> None:
    from email_reports import (
        DEFAULT_EMAIL_BODY,
        DEFAULT_EMAIL_SUBJECT,
        build_report_for_judge,
        get_email_list,
        match_judge_to_email,
        send_report_email,
    )

    st.subheader("Email Competition Reports")
    st.write(
        "Select a competition and send each judge their individual HTML report by email. "
        "Judges not in the email list will be listed so you can decide what to do — "
        "their reports are still generated but won't be sent."
    )

    _em_session = get_analytics_safe().session
    _all_comps = get_analytics_safe().get_competitions()
    if not _all_comps:
        st.info("No competitions found in the database.")
        return

    comp_options = {f"{name} ({year})": cid for cid, name, year in _all_comps}
    selected_comp_label = st.selectbox(
        "Competition", list(comp_options.keys()), key="admin_email_comp_select"
    )
    selected_comp_id = comp_options[selected_comp_label]
    selected_comp_name = selected_comp_label

    _email_list_df = get_email_list(_em_session)

    _comp_df = get_analytics_safe().get_competition_segment_statistics(int(selected_comp_id))
    _judge_map = {}
    if not _comp_df.empty and "judge_id" in _comp_df.columns:
        for _, _jrow in _comp_df[["judge_id", "judge_name"]].drop_duplicates().iterrows():
            _judge_map[_jrow["judge_id"]] = _jrow["judge_name"]

    if not _judge_map:
        st.info("No judges found for this competition.")
        return

    matched, unmatched = [], []
    for jid, jname in _judge_map.items():
        em = match_judge_to_email(jname, _email_list_df)
        if em:
            matched.append({"Judge": jname, "Email": em, "judge_id": jid})
        else:
            unmatched.append({"Judge": jname})

    col_m, col_u = st.columns(2)
    with col_m:
        st.write(f"**Will send ({len(matched)})**")
        if matched:
            st.dataframe(
                pd.DataFrame(matched)[["Judge", "Email"]],
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("None matched — upload an email list under Manage Judge Emails first.")
    with col_u:
        st.write(f"**No email — skip ({len(unmatched)})**")
        if unmatched:
            st.dataframe(pd.DataFrame(unmatched), width="stretch", hide_index=True)
        else:
            st.info("All judges matched!")

    if not matched:
        return

    st.markdown("---")
    with st.expander("Email server settings", expanded=True):
        st.write(
            "These credentials are used only for this session and are "
            "never stored. For Gmail, use an "
            "[App Password](https://myaccount.google.com/apppasswords) "
            "rather than your regular password."
        )
        _sc1, _sc2 = st.columns([3, 1])
        with _sc1:
            _smtp_host = st.text_input(
                "SMTP host",
                value=st.session_state.get("admin_smtp_host", ""),
                placeholder="smtp.gmail.com",
                key="admin_smtp_host_input",
            )
        with _sc2:
            _smtp_port = st.number_input(
                "Port",
                min_value=1,
                max_value=65535,
                value=st.session_state.get("admin_smtp_port", 587),
                key="admin_smtp_port_input",
            )
        _smtp_user = st.text_input(
            "From email address",
            value=st.session_state.get("admin_smtp_user", ""),
            placeholder="yourname@gmail.com",
            key="admin_smtp_user_input",
        )
        _smtp_pass = st.text_input(
            "Password / App password",
            type="password",
            key="admin_smtp_pass_input",
        )
        _smtp_from_name = st.text_input(
            "Sender display name",
            value=st.session_state.get("admin_smtp_from_name", "Figure Skating Officials"),
            key="admin_smtp_from_name_input",
        )
        st.session_state["admin_smtp_host"] = _smtp_host
        st.session_state["admin_smtp_port"] = _smtp_port
        st.session_state["admin_smtp_user"] = _smtp_user
        st.session_state["admin_smtp_from_name"] = _smtp_from_name

    _smtp_ready = all([_smtp_host.strip(), _smtp_user.strip(), _smtp_pass.strip()])

    st.markdown("**Email content**")
    st.caption(
        "Use `{judge_name}`, `{competition_name}`, and `{from_name}` "
        "anywhere — they'll be filled in individually for each judge."
    )
    _email_subject = st.text_input(
        "Subject line",
        value=st.session_state.get("admin_email_subject", DEFAULT_EMAIL_SUBJECT),
        key="admin_email_subject_input",
    )
    _email_body = st.text_area(
        "Email body",
        value=st.session_state.get("admin_email_body", DEFAULT_EMAIL_BODY),
        height=220,
        key="admin_email_body_input",
    )
    st.session_state["admin_email_subject"] = _email_subject
    st.session_state["admin_email_body"] = _email_body

    if st.button("Send reports", type="primary", key="admin_send_reports_btn", disabled=not _smtp_ready):
        _smtp_cfg = {
            "host": _smtp_host.strip(),
            "port": int(_smtp_port),
            "user": _smtp_user.strip(),
            "password": _smtp_pass.strip(),
            "from_name": _smtp_from_name.strip() or "Figure Skating Officials",
        }

        def _find_nonascii(label, s):
            return [(i, repr(ch), f"U+{ord(ch):04X}") for i, ch in enumerate(str(s)) if ord(ch) > 127]

        _debug_lines = []
        for _lbl, _val in [
            ("subject template", _email_subject),
            ("body template", _email_body),
            ("from_name", _smtp_from_name),
            ("competition name", selected_comp_name),
            ("SMTP username", _smtp_user),
        ]:
            _hits = _find_nonascii(_lbl, _val)
            if _hits:
                _debug_lines.append(
                    f"⚠️ Non-ASCII in **{_lbl}**: "
                    + ", ".join(f"pos {p}: {ch} ({cp})" for p, ch, cp in _hits)
                )
        _pass_hits = _find_nonascii("password", _smtp_pass)
        if _pass_hits:
            _debug_lines.append(
                f"⚠️ Non-ASCII character(s) found in **password** at "
                f"{len(_pass_hits)} position(s) — these will be automatically "
                f"replaced with regular spaces. This commonly happens when "
                f"copy-pasting a Gmail App Password from a browser."
            )
        for _jrow in matched:
            _hits = _find_nonascii("judge name", _jrow["Judge"])
            if _hits:
                _debug_lines.append(
                    f"⚠️ Non-ASCII in judge **{_jrow['Judge']}**: "
                    + ", ".join(f"pos {p}: {ch} ({cp})" for p, ch, cp in _hits)
                )

        if _debug_lines:
            with st.expander("Non-ASCII characters detected (may cause errors)"):
                for _dl in _debug_lines:
                    st.markdown(_dl)

        _analytics = get_analytics_safe()
        results = []
        prog = st.progress(0)
        for i, row in enumerate(matched):
            _step = "building report"
            try:
                html_bytes, _ = build_report_for_judge(
                    _analytics, int(row["judge_id"]), int(selected_comp_id)
                )
                _step = "sending email"
                send_report_email(
                    _smtp_cfg,
                    row["Email"],
                    row["Judge"],
                    selected_comp_name,
                    html_bytes,
                    subject_template=_email_subject,
                    body_template=_email_body,
                )
                results.append((row["Judge"], row["Email"], True, "", ""))
            except Exception as _exc:
                results.append(
                    (row["Judge"], row["Email"], False, f"[{_step}] {_exc}", _tb.format_exc())
                )
            prog.progress((i + 1) / len(matched))

        sent = [r for r in results if r[2]]
        failed = [r for r in results if not r[2]]
        if sent:
            st.success(f"Sent {len(sent)} report(s) successfully.")
        if failed:
            st.error(f"{len(failed)} failed to send:")
            for name, addr, _, err, tb in failed:
                st.write(f"- **{name}** ({addr}): {err}")
                with st.expander(f"Full traceback — {name}"):
                    st.code(tb)
    elif not _smtp_ready:
        st.caption("Fill in the email server settings above to enable sending.")


def render_directory_import() -> None:
    st.subheader("Import officials directory (Excel)")
    st.write(
        "Upload the US Figure Skating **Officials** directory export (``.xlsx`` / ``.xls``). "
        "The workbook must include a sheet named **Officials**. "
        "Requires ``DATABASE_URL`` and PostgreSQL ``officials_analysis`` schema."
    )
    up = st.file_uploader(
        "Directory Excel file",
        type=["xlsx", "xls"],
        key="admin_officials_directory_xlsx",
    )
    if not up:
        return
    if not st.button("Run directory import", type="primary", key="admin_officials_directory_run"):
        return

    suffix = ".xlsx" if up.name.lower().endswith(".xlsx") else ".xls"
    path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(up.getbuffer())
            path = tmp.name

        log_box = st.empty()
        lines: list[str] = []

        def _capture(msg: str) -> None:
            lines.append(str(msg))
            log_box.code("\n".join(lines[-300:]))

        with st.spinner("Running import…"):
            result = _officials_directory_loader.run_officials_directory_import(path, log=_capture)

        st.success(
            f"Import finished — officials upserted: **{result['officials_upserted']}**, "
            f"appointments upserted: **{result['appointments_upserted']}**, "
            f"duplicate appointments removed: **{result['appointments_deduped_removed']}**, "
            f"marked inactive: **{result['appointments_marked_inactive']}**."
        )
    except Exception as e:
        st.exception(e)
    finally:
        if path:
            try:
                os.unlink(path)
            except OSError:
                pass


def render_judge_directory_matcher_embedded() -> None:
    from judge_official_matcher_app import render_judge_official_matcher

    render_judge_official_matcher(embedded=True)


def render_international_requirement_rules() -> None:
    """Edit ISU maintain / promote thresholds (Rules 412–416, 828–862)."""
    from load_activity_data import activity_database_is_postgresql
    import international_requirements as ir

    st.subheader("International requirement rules")
    st.caption(
        "Thresholds for ISU maintain / promote service checks in the International Officials app. "
        "Apply migrations **017** and **018** before editing. "
        "Seminars, exams, and age limits are not stored here."
    )

    if not activity_database_is_postgresql():
        st.error("Requires PostgreSQL with ``officials_analysis.international_requirement_*`` tables.")
        return

    rule_sets = ir.load_requirement_rule_sets_admin()
    if rule_sets.empty:
        st.warning("No rule sets found — run migration 017/018 or check database connection.")
        return

    tab_sets, tab_rules = st.tabs(["Rule sets", "Rules"])

    with tab_sets:
        st.write("Edit season window, labels, and active flag. Save per row.")
        display_cols = [
            "id",
            "isu_rule_ref",
            "purpose",
            "label",
            "appointment_type_id",
            "discipline_id",
            "listing_tier",
            "sport",
            "season_window",
            "sort_order",
            "active",
        ]
        view = rule_sets[display_cols].copy()
        edited = st.data_editor(
            view,
            num_rows="fixed",
            disabled=["id", "isu_rule_ref", "purpose", "appointment_type_id", "discipline_id", "listing_tier", "sport"],
            column_config={
                "active": st.column_config.CheckboxColumn("Active"),
                "season_window": st.column_config.NumberColumn("Season window", min_value=1, max_value=10),
            },
            key="admin_intl_req_rule_sets",
        )

        if st.button("Save rule set changes", type="primary", key="admin_intl_req_save_sets"):
            changed = 0
            for _, row in edited.iterrows():
                orig = rule_sets.loc[rule_sets["id"] == row["id"]].iloc[0]
                if (
                    int(row["season_window"]) != int(orig["season_window"])
                    or str(row["label"]) != str(orig["label"])
                    or int(row["sort_order"]) != int(orig["sort_order"])
                    or bool(row["active"]) != bool(orig["active"])
                ):
                    ir.update_requirement_rule_set(
                        int(row["id"]),
                        label=str(row["label"]),
                        season_window=int(row["season_window"]),
                        sort_order=int(row["sort_order"]),
                        active=bool(row["active"]),
                    )
                    changed += 1
            if changed:
                st.success(f"Updated {changed} rule set(s).")
                st.rerun()
            else:
                st.info("No changes to save.")

    with tab_rules:
        rs_options = {
            f"{r.isu_rule_ref} — {r.label} (id {r.id})": int(r.id)
            for r in rule_sets.sort_values(["sort_order", "id"]).itertuples()
        }
        selected_label = st.selectbox("Rule set", list(rs_options.keys()), key="admin_intl_req_rs_pick")
        rs_id = rs_options[selected_label]

        rules = ir.load_requirement_rules_admin(rs_id)
        if rules.empty:
            st.info("No rules for this set.")
            return

        def _fmt_int_array(arr: Any) -> str:
            if arr is None or (isinstance(arr, float) and pd.isna(arr)):
                return ""
            return ", ".join(str(int(x)) for x in arr)

        def _fmt_str_array(arr: Any) -> str:
            if arr is None or (isinstance(arr, float) and pd.isna(arr)):
                return ""
            return ", ".join(str(x) for x in arr)

        rules_view = rules.copy()
        rules_view["role_appointment_type_ids"] = rules_view["role_appointment_type_ids"].map(_fmt_int_array)
        rules_view["competition_type_ids"] = rules_view["competition_type_ids"].map(_fmt_int_array)
        rules_view["segment_levels"] = rules_view["segment_levels"].map(_fmt_str_array)

        if rules_view["metric"].eq("competition_alternatives").any():
            st.json(rules.loc[rules["metric"] == "competition_alternatives", "metric_config"].iloc[0])

        edited_rules = st.data_editor(
            rules_view,
            num_rows="fixed",
            disabled=["id", "rule_set_id", "metric", "metric_config"],
            column_config={
                "require_championship_or_olympic": st.column_config.CheckboxColumn("Championship/Olympic only"),
                "include_qualifying_national": st.column_config.CheckboxColumn("Include qualifying national"),
                "min_value": st.column_config.NumberColumn("Min value", min_value=0),
            },
            key="admin_intl_req_rules",
        )

        st.caption(
            "Role IDs: 1=Judge, 4=Referee, 8=Data Operator, 9=TS, 11=TC. "
            "Competition types: 15=ISU Championship, 16=ISU Competition, 17=International. "
            "**Include qualifying national**: also count ``qualifying = true`` (not adult/collegiate). "
            "**competition_alternatives** rules use ``metric_config`` JSON (see migration 020 / Rule 861.4.b)."
        )

        if st.button("Save rule changes", type="primary", key="admin_intl_req_save_rules"):
            changed = 0
            for _, row in edited_rules.iterrows():
                orig = rules.loc[rules["id"] == row["id"]].iloc[0]
                kwargs: dict[str, Any] = {}
                if int(row["min_value"]) != int(orig["min_value"]):
                    kwargs["min_value"] = int(row["min_value"])
                if str(row.get("display_label") or "") != str(orig.get("display_label") or ""):
                    kwargs["display_label"] = str(row["display_label"])
                if int(row["sort_order"]) != int(orig["sort_order"]):
                    kwargs["sort_order"] = int(row["sort_order"])
                if bool(row["require_championship_or_olympic"]) != bool(orig["require_championship_or_olympic"]):
                    kwargs["require_championship_or_olympic"] = bool(row["require_championship_or_olympic"])
                if bool(row.get("include_qualifying_national", False)) != bool(
                    orig.get("include_qualifying_national", False)
                ):
                    kwargs["include_qualifying_national"] = bool(row["include_qualifying_national"])

                for col, parser in (
                    ("role_appointment_type_ids", _fmt_int_array),
                    ("competition_type_ids", _fmt_int_array),
                    ("segment_levels", _fmt_str_array),
                ):
                    new_val = str(row[col] or "").strip()
                    old_val = parser(orig[col])
                    if new_val != old_val:
                        kwargs[col] = new_val

                if kwargs:
                    ir.update_requirement_rule(int(row["id"]), **kwargs)
                    changed += 1

            if changed:
                st.success(f"Updated {changed} rule(s).")
                st.rerun()
            else:
                st.info("No changes to save.")
