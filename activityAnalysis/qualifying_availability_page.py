"""Streamlit UI for 2027-style qualifying availability (activity tracker report)."""

from __future__ import annotations

import html
import os
import tempfile

import pandas as pd
import streamlit as st

try:
    from app_query_params import (
        QUALIFYING_ALL_LABEL,
        QUALIFYING_ANY_LEVEL_LABEL,
        apply_activity_qualifying_competition_from_query,
        apply_activity_qualifying_form_from_query,
        apply_activity_qualifying_level_from_query,
        apply_bool_param,
        apply_choice_param,
        slug_map_for_labels,
    )
    from activityAnalysis.qualifying_form_store import (
        QUALIFYING_COMPETITION_GROUP_OPTIONS,
        build_qualifying_availability_report,
        person_assignments_report_query,
        delete_competition_criterion,
        get_active_directory_appointment_combinations,
        get_competition_criteria,
        get_official_form_response,
        list_form_competitions,
        list_qualifying_forms,
        load_qualifying_form_workbook,
        resolve_report_criteria_filters,
        save_competition_criteria,
        save_competition_group,
    )
except ModuleNotFoundError:
    from app_query_params import (
        QUALIFYING_ALL_LABEL,
        QUALIFYING_ANY_LEVEL_LABEL,
        apply_activity_qualifying_competition_from_query,
        apply_activity_qualifying_form_from_query,
        apply_activity_qualifying_level_from_query,
        apply_bool_param,
        apply_choice_param,
        slug_map_for_labels,
    )
    from qualifying_form_store import (
        QUALIFYING_COMPETITION_GROUP_OPTIONS,
        build_qualifying_availability_report,
        person_assignments_report_query,
        delete_competition_criterion,
        get_active_directory_appointment_combinations,
        get_competition_criteria,
        get_official_form_response,
        list_form_competitions,
        list_qualifying_forms,
        load_qualifying_form_workbook,
        resolve_report_criteria_filters,
        save_competition_criteria,
        save_competition_group,
    )

def _sorted_unique_labels(df: pd.DataFrame, col: str) -> list[str]:
    if df.empty or col not in df.columns:
        return []
    vals = df[col].dropna().astype(str).str.strip()
    vals = vals[vals != ""]
    return sorted(vals.unique().tolist(), key=str.casefold)


def _cascade_from_combinations(
    combos: pd.DataFrame,
    *,
    appointment_type: str | None = None,
    discipline: str | None = None,
    include_any_level: bool = True,
) -> tuple[list[str], list[str], list[str]]:
    """Appointment type → discipline → level options from a combinations dataframe."""
    df = combos
    at_opts = _sorted_unique_labels(df, "appointment_type")
    if not appointment_type or appointment_type not in at_opts:
        return at_opts, [], [(QUALIFYING_ANY_LEVEL_LABEL)] if include_any_level else []

    sub_at = df.loc[df["appointment_type"] == appointment_type]
    disc_opts = _sorted_unique_labels(sub_at, "discipline")
    if not discipline or discipline not in disc_opts:
        return at_opts, disc_opts, [(QUALIFYING_ANY_LEVEL_LABEL)] if include_any_level else []

    sub_disc = sub_at.loc[sub_at["discipline"] == discipline]
    lvl_opts = _sorted_unique_labels(sub_disc, "level")
    if include_any_level:
        lvl_opts = [QUALIFYING_ANY_LEVEL_LABEL] + lvl_opts
    return at_opts, disc_opts, lvl_opts


def _criteria_to_combinations(crit_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize criteria rows to the same shape as directory combinations."""
    if crit_df.empty:
        return crit_df
    out = crit_df.rename(
        columns={
            "appointment_type_id": "appointment_type_id",
            "appointment_type": "appointment_type",
            "discipline_id": "discipline_id",
            "discipline": "discipline",
            "level_id": "level_id",
            "level": "level",
        }
    )
    return out[
        [
            "appointment_type_id",
            "appointment_type",
            "discipline_id",
            "discipline",
            "level_id",
            "level",
        ]
    ].copy()


def _subset_crit_combos(
    crit_combos: pd.DataFrame,
    filter_at: str,
    filter_disc: str,
) -> pd.DataFrame:
    df = crit_combos
    if filter_at != QUALIFYING_ALL_LABEL:
        df = df.loc[df["appointment_type"] == filter_at]
    if filter_disc != QUALIFYING_ALL_LABEL:
        df = df.loc[df["discipline"] == filter_disc]
    return df


def _report_level_options(sub_crit: pd.DataFrame) -> list[str]:
    opts = [QUALIFYING_ALL_LABEL]
    if sub_crit["level_id"].isna().any():
        opts.append(QUALIFYING_ANY_LEVEL_LABEL)
    for lvl in _sorted_unique_labels(sub_crit, "level"):
        if lvl not in opts:
            opts.append(lvl)
    return opts


_WRAP_REPORT_COLUMNS = frozenset(
    {"Directory appointments", "Notes", "Role priority"}
)
# In the on-screen table only (still included in CSV download).
_SCREEN_HIDE_COLUMNS: tuple[str, ...] = ("Email",)

# Hidden in compact view (wide text columns at the end of the table).
_COMPACT_HIDE_COLUMNS: tuple[str, ...] = (
    "Directory appointments",
    "Notes",
    "Role priority",
)
_NUMERIC_REPORT_COLUMNS = frozenset(
    {
        "Last champs (in role)",
        "Last sectionals (in role)",
        "Last champs (overall)",
        "Last sectionals (overall)",
        "Total comps (2 yr)",
        "Total comps (2 yr, in role)",
    }
)


def _format_report_cell(column: str, value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if column in _NUMERIC_REPORT_COLUMNS:
        try:
            return str(int(value))
        except (TypeError, ValueError):
            return ""
    return str(value).strip()


def _availability_report_html_table(
    df: pd.DataFrame,
    *,
    name_official_ids: pd.Series | None = None,
) -> str:
    """HTML table with wrapping for long text columns (Streamlit dataframe does not wrap)."""
    cols = list(df.columns)
    head_parts: list[str] = []
    for c in cols:
        sort_kind = "numeric" if c in _NUMERIC_REPORT_COLUMNS else "text"
        head_parts.append(
            f'<th scope="col" data-sort="{sort_kind}">{html.escape(c)}</th>'
        )
    head = "".join(head_parts)
    body_rows: list[str] = []
    for row_idx, (_, row) in enumerate(df.iterrows()):
        cells: list[str] = []
        for col in cols:
            text = _format_report_cell(col, row[col])
            if (
                col == "Name"
                and name_official_ids is not None
                and row_idx < len(name_official_ids)
            ):
                oid = int(name_official_ids.iloc[row_idx])
                label = html.escape(text or f"Official {oid}")
                href = html.escape(
                    person_assignments_report_query(oid), quote=True
                )
                cells.append(
                    f'<td><a href="{href}" target="_parent">{label}</a></td>'
                )
                continue
            css = "wrap" if col in _WRAP_REPORT_COLUMNS else ""
            cells.append(
                f'<td class="{css}">{html.escape(text)}</td>' if css else f"<td>{html.escape(text)}</td>"
            )
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return (
        "<table id='qual-availability-report'>"
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody></table>"
    )


def _availability_report_table_css(scroll_height_px: int) -> str:
    """CSS for iframe table; scroll container must be the wrap div for sticky ``thead``."""
    return f"""
<style>
html, body {{
  margin: 0;
  padding: 0;
  height: 100%;
  overflow: hidden;
}}
#qual-name-filter-bar {{
  margin: 0 0 0.5rem 0;
  padding: 0.25rem 0;
}}
#qual-name-filter-bar label {{
  display: block;
  font-size: 0.85rem;
  margin-bottom: 0.25rem;
}}
#qual-name-filter {{
  width: 100%;
  max-width: 24rem;
  padding: 0.35rem 0.5rem;
  font-size: 0.9rem;
  box-sizing: border-box;
}}
#qual-availability-report-wrap {{
  height: 100%;
  max-height: {int(scroll_height_px)}px;
  overflow: auto;
  -webkit-overflow-scrolling: touch;
}}
#qual-availability-report {{
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  font-size: 0.9rem;
}}
#qual-availability-report th,
#qual-availability-report td {{
  border: 1px solid rgba(49, 51, 63, 0.2);
  padding: 0.35rem 0.5rem;
  vertical-align: top;
}}
#qual-availability-report thead th {{
  position: sticky;
  top: 0;
  z-index: 2;
  background: #f0f2f6;
  box-shadow: 0 1px 0 rgba(49, 51, 63, 0.25);
  cursor: pointer;
  user-select: none;
}}
#qual-availability-report thead th:hover {{
  background: #e6e9ef;
}}
#qual-availability-report td.wrap {{
  white-space: pre-wrap;
  word-break: break-word;
  min-width: 12rem;
  max-width: 36rem;
}}
#qual-availability-report a {{
  color: inherit;
  text-decoration: underline;
}}
</style>
"""

_SORTABLE_TABLE_SCRIPT = """
<script>
(function () {
  const table = document.getElementById("qual-availability-report");
  if (!table) return;
  const tbody = table.querySelector("tbody");
  const headers = table.querySelectorAll("thead th");
  if (!tbody || !headers.length) return;

  let nameCol = 0;
  headers.forEach((th, col) => {
    if ((th.textContent || "").trim() === "Name") {
      nameCol = col;
    }
  });
  const filterInput = document.getElementById("qual-name-filter");
  if (filterInput) {
    const applyNameFilter = () => {
      const q = (filterInput.value || "").trim().toLowerCase();
      tbody.querySelectorAll("tr").forEach((tr) => {
        const cell = tr.cells[nameCol];
        const text = cell ? (cell.textContent || "").trim().toLowerCase() : "";
        tr.style.display = !q || text.includes(q) ? "" : "none";
      });
    };
    filterInput.addEventListener("input", applyNameFilter);
    filterInput.addEventListener("search", applyNameFilter);
  }

  let sortCol = -1;
  let sortAsc = true;
  headers.forEach((th, col) => {
    th.addEventListener("click", () => {
      const typ = th.getAttribute("data-sort") || "text";
      if (sortCol === col) {
        sortAsc = !sortAsc;
      } else {
        sortCol = col;
        sortAsc = true;
      }
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((a, b) => {
        const av = (a.cells[col] && a.cells[col].textContent) ? a.cells[col].textContent.trim() : "";
        const bv = (b.cells[col] && b.cells[col].textContent) ? b.cells[col].textContent.trim() : "";
        if (typ === "numeric") {
          const an = av === "" ? Number.NEGATIVE_INFINITY : parseFloat(av);
          const bn = bv === "" ? Number.NEGATIVE_INFINITY : parseFloat(bv);
          return sortAsc ? an - bn : bn - an;
        }
        const cmp = av.localeCompare(bv, undefined, { sensitivity: "base", numeric: true });
        return sortAsc ? cmp : -cmp;
      });
      rows.forEach((r) => tbody.appendChild(r));
      headers.forEach((h) => h.removeAttribute("aria-sort"));
      th.setAttribute("aria-sort", sortAsc ? "ascending" : "descending");
    });
  });
})();
</script>
"""


def _render_sortable_availability_table(
    display_df: pd.DataFrame,
    *,
    name_official_ids: pd.Series | None = None,
    compact: bool = False,
) -> None:
    """Sortable, wrapping HTML table (``st.dataframe`` does not wrap long text well)."""
    row_h = 22 if compact else 30
    est_h = min(820, max(160, 100 + len(display_df) * row_h))
    payload = (
        _availability_report_table_css(est_h)
        + "<div id='qual-name-filter-bar'>"
        + "<label for='qual-name-filter'>Search name</label>"
        + "<input type='search' id='qual-name-filter' "
        + "placeholder='Type to filter rows by name…' autocomplete='off'>"
        + "</div>"
        + "<div id='qual-availability-report-wrap'>"
        + _availability_report_html_table(
            display_df, name_official_ids=name_official_ids
        )
        + "</div>"
        + _SORTABLE_TABLE_SCRIPT
    )
    st.iframe(payload, height=est_h)


def render_qualifying_availability_page(
    *,
    cache_ttl_sec: int,
    activity_url_changed: bool = False,
    on_stop,
) -> None:
    """Render the Qualifying availability report; calls ``on_stop()`` to end the Streamlit run."""

    @st.cache_data(ttl=cache_ttl_sec)
    def _forms():
        return list_qualifying_forms()

    @st.cache_data(ttl=cache_ttl_sec)
    def _comps(form_id: int):
        return list_form_competitions(int(form_id))

    @st.cache_data(ttl=cache_ttl_sec)
    def _dir_combos():
        return get_active_directory_appointment_combinations()

    @st.cache_data(ttl=cache_ttl_sec)
    def _criteria(competition_id: int):
        return get_competition_criteria(int(competition_id))

    @st.cache_data(ttl=cache_ttl_sec)
    def _availability_report(
        form_id: int,
        competition_id: int,
        criteria_filters_key: tuple[tuple[int, int, int | None], ...],
        in_role_at_id: int | None,
        in_role_disc_id: int | None,
        include_available: bool,
        include_no_reply: bool,
        include_unavailable: bool,
    ):
        return build_qualifying_availability_report(
            int(form_id),
            int(competition_id),
            criteria_filters=list(criteria_filters_key),
            in_role_appointment_type_id=in_role_at_id,
            in_role_discipline_id=in_role_disc_id,
            include_available=include_available,
            include_no_reply=include_no_reply,
            include_unavailable=include_unavailable,
        )

    def _clear_caches() -> None:
        _forms.clear()
        _comps.clear()
        _dir_combos.clear()
        _criteria.clear()
        _availability_report.clear()

    with st.expander("Load workbook", expanded=False):
        st.markdown(
            "Upload the Excel export (e.g. **2027 SPD Synchro Adults Qualifying**). "
            "Apply migrations ``008_qualifying_availability_form.sql`` and "
            "``009_qualifying_competition_group.sql`` first. "
            "Reloading the same **form label** replaces responses and availability; "
            "**competition criteria are kept** when the competition name (column) is unchanged."
        )
        form_label = st.text_input(
            "Form label",
            value="2027 SPD Synchro Adults Qualifying",
            key="qualifying_form_label",
        )
        uploaded = st.file_uploader(
            "Availability workbook (.xlsx)",
            type=["xlsx"],
            key="qualifying_availability_upload",
        )
        allow_partial = st.checkbox(
            "Allow workbooks without a Status column (still skips Incomplete rows)",
            value=False,
            key="qualifying_allow_partial",
        )
        if st.button("Load into database", type="primary", key="qualifying_load_btn"):
            if uploaded is None:
                st.error("Choose an .xlsx file first.")
            else:
                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                    tmp.write(uploaded.getvalue())
                    tmp_path = tmp.name
                try:
                    with st.spinner("Loading form…"):
                        stats = load_qualifying_form_workbook(
                            tmp_path,
                            label=form_label.strip() or None,
                            only_complete_responses=not allow_partial,
                            allow_missing_completion_status=allow_partial,
                        )
                    _clear_caches()
                    st.success("Form loaded.")
                    st.json(stats)
                    if stats.get("duplicate_rows_dropped"):
                        st.warning(
                            f"Dropped **{stats['duplicate_rows_dropped']}** older duplicate "
                            f"row(s) for **{len(stats.get('duplicate_member_numbers') or [])}** "
                            f"member(s); kept the most recent response "
                            f"({stats.get('dedupe_by') or 'sheet order'})."
                        )
                    if stats.get("availability_rows_repaired_opt_out"):
                        st.info(
                            f"Corrected **{stats['availability_rows_repaired_opt_out']}** "
                            "availability row(s) to unavailable for global opt-out (column G)."
                        )
                    if stats.get("skipped_incomplete"):
                        st.info(
                            f"Skipped **{stats['skipped_incomplete']}** rows with "
                            "Incomplete status (not stored or used in reports)."
                        )
                    if stats.get("unmatched_member_numbers"):
                        st.warning(
                            f"{len(stats['unmatched_member_numbers'])} member numbers "
                            "did not match ``officials_analysis.officials``."
                        )
                except Exception as ex:
                    st.error(f"Load failed: {ex}")
                finally:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

    forms_df = _forms()
    if forms_df.empty:
        st.info(
            "No form loaded yet. Upload a workbook above or run:\n\n"
            "```\npython activityAnalysis/load_activity_data.py "
            "--load-qualifying-availability path/to/export.xlsx "
            "--qualifying-label \"2027 SPD Synchro Adults Qualifying\"\n```"
        )
        on_stop()
        return

    form_labels = {int(r.form_id): str(r.label) for _, r in forms_df.iterrows()}
    if activity_url_changed:
        apply_activity_qualifying_form_from_query(list(form_labels.keys()))
    pick_form_id = int(
        st.selectbox(
            "Form",
            options=list(form_labels.keys()),
            format_func=lambda fid: form_labels.get(int(fid), str(fid)),
            key="qualifying_form_select",
        )
    )

    comps_df = _comps(pick_form_id)
    if comps_df.empty:
        st.warning("This form has no competition columns.")
        on_stop()
        return

    comp_labels = {
        int(r.competition_id): str(r.title or r.prompt_key)[:80]
        for _, r in comps_df.iterrows()
    }
    if activity_url_changed:
        apply_activity_qualifying_competition_from_query(list(comp_labels.keys()))
    pick_comp_id = int(
        st.selectbox(
            "Competition",
            options=list(comp_labels.keys()),
            format_func=lambda cid: comp_labels.get(int(cid), str(cid)),
            key="qualifying_competition_select",
        )
    )
    comp_row = comps_df.loc[comps_df["competition_id"] == pick_comp_id].iloc[0]
    loc_bits = [
        str(comp_row.get("season_year") or ""),
        str(comp_row.get("location") or ""),
        str(comp_row.get("event_dates") or ""),
    ]
    loc_bits = [b for b in loc_bits if b]
    if loc_bits:
        st.caption(" · ".join(loc_bits))

    group_keys = list(QUALIFYING_COMPETITION_GROUP_OPTIONS.keys())
    stored_group = str(comp_row.get("competition_group") or "").strip().lower()
    if stored_group not in group_keys[1:]:
        stored_group = ""
    group_index = group_keys.index(stored_group) if stored_group in group_keys else 0
    pick_group = st.selectbox(
        "Competition type (for assignment history)",
        options=group_keys,
        format_func=lambda k: QUALIFYING_COMPETITION_GROUP_OPTIONS[k],
        index=group_index,
        key=f"qual_comp_group_{pick_comp_id}",
        help=(
            "S/P/D uses US Championships (type 4) and SPD sectionals (1–3); "
            "Synchronized uses US Synchro Championships (8) and synchro sectionals (5–7, 9)."
        ),
    )
    if pick_group != stored_group:
        save_competition_group(pick_comp_id, pick_group or None)
        _comps.clear()
        st.rerun()

    dir_combos = _dir_combos()
    crit_df = _criteria(pick_comp_id)
    crit_combos = _criteria_to_combinations(crit_df)

    with st.expander("Competition criteria (directory appointments)", expanded=False):
        st.markdown(
            "Add combinations that exist as **active** directory appointments. "
            "The report only offers filters from criteria you configure here."
        )
        if dir_combos.empty:
            st.warning("No active appointments in the directory — cannot suggest criteria.")
        else:
            add_at, add_disc, add_lvl = _cascade_from_combinations(dir_combos)
            c1, c2, c3 = st.columns(3)
            with c1:
                pick_at_name = st.selectbox(
                    "Add: appointment type",
                    options=add_at or ["—"],
                    key=f"qual_crit_at_{pick_comp_id}",
                )
            sub_dir = dir_combos.loc[dir_combos["appointment_type"] == pick_at_name]
            _, add_disc, add_lvl = _cascade_from_combinations(
                sub_dir, appointment_type=pick_at_name
            )
            with c2:
                pick_disc_name = st.selectbox(
                    "Add: discipline",
                    options=add_disc or ["—"],
                    key=f"qual_crit_disc_{pick_comp_id}",
                )
            sub_dir2 = sub_dir.loc[sub_dir["discipline"] == pick_disc_name]
            _, _, add_lvl = _cascade_from_combinations(
                sub_dir2,
                appointment_type=pick_at_name,
                discipline=pick_disc_name,
            )
            with c3:
                pick_lvl_name = st.selectbox(
                    "Add: level",
                    options=add_lvl or [QUALIFYING_ANY_LEVEL_LABEL],
                    key=f"qual_crit_lvl_{pick_comp_id}",
                )
            if st.button("Add criterion", key=f"qual_crit_add_{pick_comp_id}"):
                if pick_at_name == "—" or pick_disc_name == "—":
                    st.error("Choose appointment type and discipline.")
                else:
                    at_id = int(
                        dir_combos.loc[
                            dir_combos["appointment_type"] == pick_at_name,
                            "appointment_type_id",
                        ].iloc[0]
                    )
                    disc_id = int(
                        dir_combos.loc[
                            (dir_combos["appointment_type"] == pick_at_name)
                            & (dir_combos["discipline"] == pick_disc_name),
                            "discipline_id",
                        ].iloc[0]
                    )
                    save_ok = False
                    lid: int | None = None
                    if pick_lvl_name == QUALIFYING_ANY_LEVEL_LABEL:
                        save_ok = True
                    else:
                        lvl_rows = dir_combos.loc[
                            (dir_combos["appointment_type"] == pick_at_name)
                            & (dir_combos["discipline"] == pick_disc_name)
                            & (dir_combos["level"] == pick_lvl_name)
                        ]
                        if lvl_rows.empty:
                            st.error(
                                "That level is not an active directory combination."
                            )
                        else:
                            lv = lvl_rows.iloc[0]["level_id"]
                            lid = None if pd.isna(lv) else int(lv)
                            save_ok = True
                    if save_ok:
                        existing = [
                            (
                                int(r.appointment_type_id),
                                int(r.discipline_id),
                                int(r.level_id)
                                if pd.notna(r.level_id) and r.level_id is not None
                                else None,
                            )
                            for _, r in crit_df.iterrows()
                        ]
                        new_row = (at_id, disc_id, lid)
                        if new_row not in existing:
                            existing.append(new_row)
                        save_competition_criteria(pick_comp_id, existing)
                        _criteria.clear()
                        st.rerun()

        if crit_df.empty:
            st.info("No criteria yet — add at least one before running the report.")
        else:
            for _, row in crit_df.iterrows():
                lvl_show = (
                    QUALIFYING_ANY_LEVEL_LABEL
                    if pd.isna(row.level) or not str(row.level or "").strip()
                    else str(row.level)
                )
                label = f"{row.appointment_type} · {row.discipline} · {lvl_show}"
                col_text, col_btn = st.columns([6, 1])
                with col_text:
                    st.text(label)
                with col_btn:
                    if st.button(
                        "Delete",
                        key=f"qual_crit_del_{pick_comp_id}_{int(row.criteria_id)}",
                    ):
                        delete_competition_criterion(int(row.criteria_id))
                        _criteria.clear()
                        st.rerun()
            if st.button("Clear all criteria", key=f"qual_crit_clear_{pick_comp_id}"):
                save_competition_criteria(pick_comp_id, [])
                _criteria.clear()
                st.rerun()

    st.subheader("Report")
    crit_df = _criteria(pick_comp_id)
    crit_combos = _criteria_to_combinations(crit_df)
    if crit_combos.empty:
        st.info("Configure at least one criterion for this competition to run the report.")
        on_stop()
        return

    rep_at = [QUALIFYING_ALL_LABEL] + _sorted_unique_labels(
        crit_combos, "appointment_type"
    )
    if activity_url_changed and rep_at:
        apply_choice_param(
            "appointment",
            "qual_report_at",
            rep_at,
            slug_map={**slug_map_for_labels(rep_at), "all": QUALIFYING_ALL_LABEL},
        )
        filter_at_qp = st.session_state.get("qual_report_at") or rep_at[0]
        sub_crit_qp = _subset_crit_combos(crit_combos, filter_at_qp, QUALIFYING_ALL_LABEL)
        rep_disc_qp = [QUALIFYING_ALL_LABEL] + _sorted_unique_labels(
            sub_crit_qp, "discipline"
        )
        if rep_disc_qp:
            apply_choice_param(
                "discipline",
                "qual_report_disc",
                rep_disc_qp,
                slug_map={
                    **slug_map_for_labels(rep_disc_qp),
                    "all": QUALIFYING_ALL_LABEL,
                },
            )
        filter_disc_qp = st.session_state.get("qual_report_disc") or rep_disc_qp[0]
        sub_crit2_qp = _subset_crit_combos(crit_combos, filter_at_qp, filter_disc_qp)
        apply_activity_qualifying_level_from_query(_report_level_options(sub_crit2_qp))
        apply_bool_param("show_available", "qual_report_show_available")
        apply_bool_param("show_no_reply", "qual_report_show_no_reply")
        apply_bool_param("show_unavailable", "qual_report_show_unavailable")

    f1, f2, f3 = st.columns(3)
    with f1:
        filter_at = st.selectbox(
            "Filter: appointment type",
            options=rep_at,
            key="qual_report_at",
        )
    sub_crit = _subset_crit_combos(crit_combos, filter_at, QUALIFYING_ALL_LABEL)
    rep_disc = [QUALIFYING_ALL_LABEL] + _sorted_unique_labels(sub_crit, "discipline")
    with f2:
        filter_disc = st.selectbox(
            "Filter: discipline",
            options=rep_disc,
            key="qual_report_disc",
        )
    sub_crit2 = _subset_crit_combos(crit_combos, filter_at, filter_disc)
    rep_lvl = _report_level_options(sub_crit2)

    with f3:
        filter_lvl = st.selectbox(
            "Filter: level",
            options=rep_lvl,
            key="qual_report_lvl",
        )
    st.markdown("**Include officials who are:**")
    b1, b2, b3 = st.columns(3)
    with b1:
        show_available = st.checkbox(
            "Available",
            value=True,
            key="qual_report_show_available",
        )
    with b2:
        show_no_reply = st.checkbox(
            "Didn't reply (no form / blank)",
            value=False,
            key="qual_report_show_no_reply",
        )
    with b3:
        show_unavailable = st.checkbox(
            "Unavailable",
            value=False,
            key="qual_report_show_unavailable",
        )

    criteria_filters = resolve_report_criteria_filters(
        crit_combos, filter_at, filter_disc, filter_lvl
    )
    if not criteria_filters:
        st.warning("No criteria match the selected filters.")
        on_stop()
        return

    if not (show_available or show_no_reply or show_unavailable):
        st.warning("Select at least one availability status to include in the report.")
        on_stop()
        return

    in_role_at_id: int | None = None
    in_role_disc_id: int | None = None
    if filter_at != QUALIFYING_ALL_LABEL and filter_disc != QUALIFYING_ALL_LABEL:
        scoped = crit_combos.loc[
            (crit_combos["appointment_type"] == filter_at)
            & (crit_combos["discipline"] == filter_disc)
        ]
        if not scoped.empty:
            in_role_at_id = int(scoped.iloc[0]["appointment_type_id"])
            in_role_disc_id = int(scoped.iloc[0]["discipline_id"])

    report_df, meta = _availability_report(
        int(pick_form_id),
        int(pick_comp_id),
        tuple(criteria_filters),
        in_role_at_id,
        in_role_disc_id,
        show_available,
        show_no_reply,
        show_unavailable,
    )
    if meta.get("criteria_configured") and not meta.get("criteria_match"):
        st.error("Selected filters do not match configured criteria for this competition.")
        on_stop()
        return
    if report_df.empty:
        st.info(
            "No officials match the selected availability statuses with an active "
            "directory appointment for these filters."
        )
        on_stop()
        return

    if not (meta.get("competition_group") or "").strip():
        st.warning(
            "Set **Competition type (for assignment history)** above to populate "
            "the **Last champs / sectionals** columns."
        )
    if in_role_at_id is None:
        st.caption(
            "Select a specific **appointment type** and **discipline** (not "
            f"``{QUALIFYING_ALL_LABEL}``) to fill **Last champs/sectionals (in role)** "
            "and **Total comps (2 yr, in role)**."
        )
    season_codes = meta.get("other_comp_season_codes") or []
    if season_codes:
        codes = ", ".join(str(c) for c in season_codes)
        cal_years = ", ".join(
            str(y)
            for y in (meta.get("other_comp_calendar_years") or [])
        )
        cal_part = (
            f" or calendar years **{cal_years}**"
            if cal_years
            else ""
        )
        st.caption(
            f"**Total comps (2 yr)** counts distinct competitions from IJS protocol data "
            f"(``segment_official``) with USFS season **{codes}**{cal_part} on "
            "``competition.year``, same source as **Additional Qualifying / "
            "Nonqualifying Activity** on the per-person report. "
            + (
                "**Total comps (2 yr, in role)** uses the same seasons but only panels "
                "matching the selected appointment type and discipline. "
                if meta.get("show_total_comps_in_role")
                else ""
            )
            + "Click a name to open **Per-person assignments**."
        )

    n_crit = meta.get("criteria_count", len(criteria_filters))
    show_detail_columns = st.checkbox(
        "Show directory appointments, notes, and role priority",
        value=True,
        key="qual_report_show_detail_columns",
        help="Uncheck for a compact table without the three wide columns at the end.",
    )
    st.caption(
        f"**{len(report_df)}** officials · **{n_crit}** criterion row(s) in this view · "
        "use **Search name** above the table or click a column header to sort · "
        "**Directory appointments** lists all roles configured for this competition"
        + (
            ""
            if show_detail_columns
            else " · compact view (appointments, notes, and role priority hidden)"
        )
    )
    name_ids = report_df["official_id"].astype(int)
    display_df = report_df.drop(
        columns=["official_id", *_SCREEN_HIDE_COLUMNS], errors="ignore"
    )
    if not show_detail_columns:
        display_df = display_df.drop(columns=list(_COMPACT_HIDE_COLUMNS), errors="ignore")
    _render_sortable_availability_table(
        display_df,
        name_official_ids=name_ids,
        compact=not show_detail_columns,
    )
    st.download_button(
        "Download CSV",
        data=report_df.to_csv(index=False).encode("utf-8"),
        file_name="qualifying_availability_report.csv",
        mime="text/csv",
        key="qualifying_availability_csv_dl",
    )

    with st.expander("Full form response (one official)"):
        pick_name = st.selectbox("Official", report_df["Name"].tolist())
        oid = int(report_df.loc[report_df["Name"] == pick_name, "official_id"].iloc[0])
        payload = get_official_form_response(pick_form_id, oid)
        if payload:
            st.json(payload)
        else:
            st.info("No stored form response for this official.")
    on_stop()
