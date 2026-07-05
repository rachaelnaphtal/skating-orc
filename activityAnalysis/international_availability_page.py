"""Streamlit UI for international judge availability (international officials app)."""

from __future__ import annotations

import html
import os
import tempfile

import pandas as pd
import streamlit as st

try:
    from activityAnalysis.international_availability import (
        DISCIPLINE_FILTER_ALL,
        DISCIPLINE_FILTER_DANCE,
        DISCIPLINE_FILTER_SINGLES_PAIRS,
        LEVEL_FILTER_ALL,
        LEVEL_FILTER_INTERNATIONAL,
        LEVEL_FILTER_ISU,
        build_international_availability_report,
        default_availability_workbook_path,
    )
    from activityAnalysis.international_availability_store import (
        DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL,
        get_international_availability_form,
        international_form_to_dataframe,
        list_international_availability_forms,
        load_international_availability_form_workbook,
    )
except ModuleNotFoundError:
    from international_availability import (
        DISCIPLINE_FILTER_ALL,
        DISCIPLINE_FILTER_DANCE,
        DISCIPLINE_FILTER_SINGLES_PAIRS,
        LEVEL_FILTER_ALL,
        LEVEL_FILTER_INTERNATIONAL,
        LEVEL_FILTER_ISU,
        build_international_availability_report,
        default_availability_workbook_path,
    )
    from international_availability_store import (
        DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL,
        get_international_availability_form,
        international_form_to_dataframe,
        list_international_availability_forms,
        load_international_availability_form_workbook,
    )

_DISCIPLINE_OPTIONS = (
    DISCIPLINE_FILTER_ALL,
    DISCIPLINE_FILTER_SINGLES_PAIRS,
    DISCIPLINE_FILTER_DANCE,
)
_LEVEL_OPTIONS = (
    LEVEL_FILTER_ALL,
    LEVEL_FILTER_INTERNATIONAL,
    LEVEL_FILTER_ISU,
)

_WRAP_COLUMNS = frozenset({"Activity notes", "Other notes"})
_IDENTITY_COLUMNS = frozenset({"Official", "Discipline", "Level"})

_AVAILABILITY_CSS_CLASS = {
    "available": "intl-avail-yes",
    "not_available": "intl-avail-no",
    "no_response": "intl-avail-none",
    "does_not_apply": "intl-avail-na",
    "unknown": "intl-avail-unknown",
}


def _format_report_cell(column: str, value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if column in _IDENTITY_COLUMNS:
        return str(value).strip()
    return str(value).strip()


def _sticky_column_class(column: str) -> str | None:
    if column == "Official":
        return "intl-sticky-col intl-sticky-official"
    if column == "Discipline":
        return "intl-sticky-col intl-sticky-discipline"
    if column == "Level":
        return "intl-sticky-col intl-sticky-level"
    return None


def _td_class_attr(*parts: str | None) -> str:
    classes = [p.strip() for p in parts if p and p.strip()]
    return f' class="{" ".join(classes)}"' if classes else ""


def _availability_report_html_table(
    df: pd.DataFrame,
    *,
    availability_codes: list[list[str]] | None,
    event_labels: list[str],
) -> str:
    cols = list(df.columns)
    head_parts: list[str] = []
    for c in cols:
        sort_kind = "text"
        sticky = _sticky_column_class(c)
        head_parts.append(
            f'<th scope="col" data-sort="{sort_kind}"{_td_class_attr(sticky)}>{html.escape(c)}</th>'
        )
    head = "".join(head_parts)

    event_label_set = set(event_labels)
    body_rows: list[str] = []
    for row_idx, (_, row) in enumerate(df.iterrows()):
        row_codes = (
            availability_codes[row_idx]
            if availability_codes and row_idx < len(availability_codes)
            else None
        )
        cells: list[str] = []
        event_i = 0
        for col in cols:
            text = _format_report_cell(col, row[col])
            sticky = _sticky_column_class(col)
            if col in _IDENTITY_COLUMNS:
                extra = "wrap" if col in _WRAP_COLUMNS else None
                cells.append(
                    f"<td{_td_class_attr(sticky, extra)}>{html.escape(text)}</td>"
                )
                continue
            if col in event_label_set and row_codes is not None and event_i < len(row_codes):
                css = _AVAILABILITY_CSS_CLASS.get(row_codes[event_i], "intl-avail-unknown")
                cells.append(f'<td class="{css}">{html.escape(text)}</td>')
                event_i += 1
                continue
            css = "wrap" if col in _WRAP_COLUMNS else ""
            cells.append(
                f'<td class="{css}">{html.escape(text)}</td>'
                if css
                else f"<td>{html.escape(text)}</td>"
            )
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    return (
        "<table id='intl-availability-report'>"
        f"<thead><tr>{head}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody></table>"
    )


def _availability_report_table_css(
    *,
    filter_bar_height_px: int,
    table_scroll_height_px: int,
    dark_mode: bool = False,
) -> str:
    theme_class = "intl-theme-dark" if dark_mode else "intl-theme-light"
    sticky_body_bg = "#0e1117" if dark_mode else "#ffffff"
    sticky_head_bg = "#262730" if dark_mode else "#f0f2f6"
    sticky_head_hover_bg = "#31333f" if dark_mode else "#e6e9ef"
    border_color = "rgba(250, 250, 250, 0.18)" if dark_mode else "rgba(49, 51, 63, 0.2)"
    return f"""
<style>
html, body {{
  margin: 0;
  padding: 0;
  overflow: hidden;
  box-sizing: border-box;
  color-scheme: {"dark" if dark_mode else "light"};
}}
*, *::before, *::after {{
  box-sizing: inherit;
}}
html.{theme_class}, html.{theme_class} body {{
  background: {"#0e1117" if dark_mode else "#ffffff"};
  color: {"#fafafa" if dark_mode else "#31333f"};
}}
#intl-name-filter-bar {{
  height: {int(filter_bar_height_px)}px;
  margin: 0;
  padding: 0.35rem 0.5rem 0.5rem 0.5rem;
  overflow: hidden;
}}
#intl-name-filter-bar label {{
  display: block;
  font-size: 0.85rem;
  margin-bottom: 0.25rem;
  color: inherit;
}}
#intl-name-filter {{
  width: 100%;
  max-width: 24rem;
  padding: 0.35rem 0.5rem;
  font-size: 0.9rem;
  background: {"#262730" if dark_mode else "#ffffff"};
  color: inherit;
  border: 1px solid {"rgba(250, 250, 250, 0.25)" if dark_mode else "rgba(49, 51, 63, 0.35)"};
  border-radius: 0.25rem;
}}
#intl-availability-legend {{
  padding: 0 0.5rem 0.35rem 0.5rem;
  font-size: 0.85rem;
}}
#intl-availability-legend span {{
  display: inline-block;
  padding: 2px 6px;
  margin-right: 0.35rem;
  border-radius: 0.15rem;
}}
#intl-availability-report-wrap {{
  height: {int(table_scroll_height_px)}px;
  overflow: auto;
  -webkit-overflow-scrolling: touch;
  padding: 0 0.5rem 1rem 0;
}}
#intl-availability-report {{
  width: max-content;
  min-width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  font-size: 0.85rem;
  --intl-official-w: 11rem;
  --intl-discipline-w: 7.5rem;
  --intl-level-w: 6.5rem;
}}
#intl-availability-report th,
#intl-availability-report td {{
  border: 1px solid {"rgba(250, 250, 250, 0.18)" if dark_mode else "rgba(49, 51, 63, 0.2)"};
  padding: 0.35rem 0.45rem;
  vertical-align: top;
  text-align: center;
}}
#intl-availability-report td:first-child,
#intl-availability-report th:first-child {{
  text-align: left;
}}
#intl-availability-report .intl-sticky-col {{
  position: sticky;
  background-color: {sticky_body_bg} !important;
  background-clip: padding-box;
  text-align: left;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
#intl-availability-report .intl-sticky-official {{
  left: 0;
  width: var(--intl-official-w);
  min-width: var(--intl-official-w);
  max-width: var(--intl-official-w);
  z-index: 5;
}}
#intl-availability-report .intl-sticky-discipline {{
  left: var(--intl-official-w);
  width: var(--intl-discipline-w);
  min-width: var(--intl-discipline-w);
  max-width: var(--intl-discipline-w);
  z-index: 4;
}}
#intl-availability-report .intl-sticky-level {{
  left: calc(var(--intl-official-w) + var(--intl-discipline-w));
  width: var(--intl-level-w);
  min-width: var(--intl-level-w);
  max-width: var(--intl-level-w);
  z-index: 3;
  box-shadow:
    1px 0 0 {border_color},
    8px 0 10px 0 {sticky_body_bg};
}}
#intl-availability-report thead .intl-sticky-col {{
  top: 0;
  background-color: {sticky_head_bg} !important;
  box-shadow: 0 1px 0 {"rgba(250, 250, 250, 0.15)" if dark_mode else "rgba(49, 51, 63, 0.25)"};
}}
#intl-availability-report thead .intl-sticky-official {{
  z-index: 16;
}}
#intl-availability-report thead .intl-sticky-discipline {{
  z-index: 15;
}}
#intl-availability-report thead .intl-sticky-level {{
  z-index: 14;
  box-shadow:
    1px 0 0 {border_color},
    8px 0 10px 0 {sticky_head_bg},
    0 1px 0 {"rgba(250, 250, 250, 0.15)" if dark_mode else "rgba(49, 51, 63, 0.25)"};
}}
#intl-availability-report thead th:hover.intl-sticky-col {{
  background-color: {sticky_head_hover_bg} !important;
}}
#intl-availability-report thead th:hover.intl-sticky-level {{
  box-shadow:
    1px 0 0 {border_color},
    8px 0 10px 0 {sticky_head_hover_bg},
    0 1px 0 {"rgba(250, 250, 250, 0.15)" if dark_mode else "rgba(49, 51, 63, 0.25)"};
}}
#intl-availability-report thead th {{
  position: sticky;
  top: 0;
  z-index: 2;
  background: {"#262730" if dark_mode else "#f0f2f6"};
  color: inherit;
  box-shadow: 0 1px 0 {"rgba(250, 250, 250, 0.15)" if dark_mode else "rgba(49, 51, 63, 0.25)"};
  cursor: pointer;
  user-select: none;
  font-size: 0.8rem;
  min-width: 4.5rem;
}}
#intl-availability-report thead th:hover {{
  background: {sticky_head_hover_bg};
}}
#intl-availability-report tbody td {{
  background: {"#0e1117" if dark_mode else "#ffffff"};
}}
#intl-availability-report td.wrap {{
  white-space: pre-wrap;
  word-break: break-word;
  min-width: 12rem;
  max-width: 28rem;
  text-align: left;
}}
#intl-availability-report td.intl-avail-yes {{
  background-color: {"#1a4d2e" if dark_mode else "#c6efce"};
  color: {"#b8f0c8" if dark_mode else "#1e4620"};
  font-weight: 600;
}}
#intl-availability-report td.intl-avail-no {{
  background-color: {"#5c1a1a" if dark_mode else "#ffc7ce"};
  color: {"#ffb3b3" if dark_mode else "#5c1a1a"};
  font-weight: 600;
}}
#intl-availability-report td.intl-avail-none {{
  background-color: {"#1c1c1c" if dark_mode else "#f3f3f3"};
  color: {"#9a9a9a" if dark_mode else "#666"};
}}
#intl-availability-report td.intl-avail-na {{
  background-color: {"#2a2a2a" if dark_mode else "#e8e8e8"};
  color: {"#aaaaaa" if dark_mode else "#555"};
}}
#intl-availability-report td.intl-avail-unknown {{
  background-color: {"#4a3f1a" if dark_mode else "#fff3cd"};
  color: {"#ffe08a" if dark_mode else "#664d03"};
}}
#intl-availability-legend .intl-avail-yes {{
  background-color: {"#1a4d2e" if dark_mode else "#c6efce"};
  color: {"#b8f0c8" if dark_mode else "#1e4620"};
}}
#intl-availability-legend .intl-avail-no {{
  background-color: {"#5c1a1a" if dark_mode else "#ffc7ce"};
  color: {"#ffb3b3" if dark_mode else "#5c1a1a"};
}}
#intl-availability-legend .intl-avail-none {{
  background-color: {"#1c1c1c" if dark_mode else "#f3f3f3"};
  color: {"#9a9a9a" if dark_mode else "#666"};
}}
</style>
"""


_SORTABLE_TABLE_SCRIPT = """
<script>
(function () {
  const table = document.getElementById("intl-availability-report");
  if (!table) return;
  const tbody = table.querySelector("tbody");
  const headers = table.querySelectorAll("thead th");
  if (!tbody || !headers.length) return;

  let nameCol = 0;
  headers.forEach((th, col) => {
    if ((th.textContent || "").trim() === "Official") {
      nameCol = col;
    }
  });
  const filterInput = document.getElementById("intl-name-filter");
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


def _availability_report_iframe_heights(row_count: int) -> tuple[int, int, int]:
    row_h = 30
    thead_h = 44
    filter_bar_h = 76
    legend_h = 28
    n = max(int(row_count), 1)
    table_scroll_h = min(720, max(320, thead_h + n * row_h + 24))
    iframe_h = filter_bar_h + legend_h + table_scroll_h
    return filter_bar_h, table_scroll_h, iframe_h


def _availability_legend_html() -> str:
    return (
        "<div id='intl-availability-legend'>"
        "Legend: "
        '<span class="intl-avail-yes">Available</span>'
        '<span class="intl-avail-no">Not available</span>'
        '<span class="intl-avail-none">No response</span>'
        "</div>"
    )


def _streamlit_dark_mode() -> bool:
    theme_type = getattr(getattr(st.context, "theme", None), "type", None)
    return theme_type == "dark"


def _render_sortable_availability_table(
    display_df: pd.DataFrame,
    *,
    availability_codes: list[list[str]] | None,
    event_labels: list[str],
    dark_mode: bool = False,
) -> None:
    filter_bar_h, table_scroll_h, iframe_h = _availability_report_iframe_heights(len(display_df))
    theme_class = "intl-theme-dark" if dark_mode else "intl-theme-light"
    payload = (
        f"<html class='{theme_class}'><body class='{theme_class}'>"
        + _availability_report_table_css(
            filter_bar_height_px=filter_bar_h,
            table_scroll_height_px=table_scroll_h,
            dark_mode=dark_mode,
        )
        + "<div id='intl-name-filter-bar'>"
        + "<label for='intl-name-filter'>Search official</label>"
        + "<input type='search' id='intl-name-filter' "
        + "placeholder='Type to filter rows by name…' autocomplete='off'>"
        + "</div>"
        + _availability_legend_html()
        + "<div id='intl-availability-report-wrap'>"
        + _availability_report_html_table(
            display_df,
            availability_codes=availability_codes,
            event_labels=event_labels,
        )
        + "</div>"
        + _SORTABLE_TABLE_SCRIPT
        + "</body></html>"
    )
    st.iframe(payload, height=iframe_h)


def render_international_availability_page(
    *,
    cache_ttl_sec: int,
    active_only: bool,
) -> None:
    """Render the availability matrix."""

    @st.cache_data(ttl=cache_ttl_sec)
    def _forms():
        return list_international_availability_forms()

    @st.cache_data(ttl=cache_ttl_sec)
    def _form_data(form_id: int, loaded_at_iso: str):
        del loaded_at_iso
        return international_form_to_dataframe(int(form_id))

    def _clear_caches() -> None:
        _forms.clear()
        _form_data.clear()

    st.title("International Judge Availability")
    st.caption(
        "Per-event availability from the international selections form, joined to active "
        "**International Judge** directory appointments at **International** or **ISU** level "
        "in **Singles/Pairs** or **Ice Dance**. "
        "Click column headers to sort; green = available, red = not available."
    )

    try:
        forms_df = _forms()
    except Exception as exc:
        st.error(f"Could not read availability forms from the database: {exc}")
        st.info(
            "Apply migration "
            "``activityAnalysis/migrations/041_international_availability_form.sql`` "
            "on PostgreSQL, then upload a workbook below or run "
            "``python scripts/load_international_availability_workbook.py path/to/file.xlsx``."
        )
        forms_df = pd.DataFrame()

    form_options: list[int] = []
    form_labels: dict[int, str] = {}
    if not forms_df.empty:
        for row in forms_df.itertuples(index=False):
            fid = int(row.form_id)
            form_options.append(fid)
            loaded = pd.Timestamp(row.loaded_at)
            form_labels[fid] = (
                f"{row.label} (loaded {loaded:%Y-%m-%d %H:%M}, "
                f"{row.source_filename or 'unknown file'})"
            )

    with st.expander("Load workbook", expanded=forms_df.empty):
        st.markdown(
            "Upload an updated form export (``.xlsx``). "
            f"Reloading label **{DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL}** "
            "replaces stored responses for that form."
        )
        form_label = st.text_input(
            "Form label",
            value=DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL,
            key="intl_avail_form_label",
        )
        uploaded = st.file_uploader(
            "Availability workbook (.xlsx)",
            type=["xlsx"],
            key="intl_avail_workbook_upload",
        )
        if uploaded is not None and st.button(
            "Save workbook to database",
            type="primary",
            key="intl_avail_save_workbook",
        ):
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            tmp.write(uploaded.getvalue())
            tmp.close()
            try:
                stats = load_international_availability_form_workbook(
                    tmp.name,
                    label=form_label.strip() or DEFAULT_INTERNATIONAL_AVAILABILITY_LABEL,
                )
            except Exception as exc:
                st.error(f"Could not load workbook: {exc}")
            else:
                _clear_caches()
                st.success(
                    f"Stored **{stats['responses_stored']}** responses "
                    f"({stats['events']} events) for form id {stats['form_id']}."
                )
                if stats.get("duplicate_rows_dropped"):
                    st.caption(
                        f"Dropped {stats['duplicate_rows_dropped']} duplicate row(s) "
                        "with the same email or name."
                    )
                st.rerun()

        bundled_path = default_availability_workbook_path()
        if forms_df.empty and os.path.isfile(bundled_path):
            if st.button(
                "Load bundled workbook into database",
                key="intl_avail_seed_bundled",
            ):
                try:
                    stats = load_international_availability_form_workbook(bundled_path)
                except Exception as exc:
                    st.error(f"Could not load bundled workbook: {exc}")
                else:
                    _clear_caches()
                    st.success(
                        f"Loaded bundled workbook: {stats['responses_stored']} responses."
                    )
                    st.rerun()

    if not form_options:
        st.info(
            "No availability data in the database yet. Upload a workbook above or run "
            "``python scripts/load_international_availability_workbook.py "
            "activityAnalysis/InternationalAvailability20262.xlsx``."
        )
        st.stop()

    default_form_id = int(form_options[0])
    pick_form = st.selectbox(
        "Stored form",
        options=form_options,
        index=0,
        format_func=lambda fid: form_labels.get(fid, str(fid)),
        key="intl_avail_form_pick",
    )
    form_id = int(pick_form or default_form_id)
    form_row = forms_df.loc[forms_df["form_id"] == form_id].iloc[0]
    loaded_at_iso = pd.Timestamp(form_row["loaded_at"]).isoformat()

    try:
        form_df, layout = _form_data(form_id, loaded_at_iso)
    except Exception as exc:
        st.error(f"Could not load form data: {exc}")
        st.stop()

    col_d, col_l = st.columns(2)
    with col_d:
        discipline_filter = st.selectbox(
            "Discipline",
            options=list(_DISCIPLINE_OPTIONS),
            key="intl_avail_discipline_filter",
        )
    with col_l:
        level_filter = st.selectbox(
            "Level",
            options=list(_LEVEL_OPTIONS),
            key="intl_avail_level_filter",
            help="Directory appointment level (International or ISU Championship).",
        )

    report_df, meta = build_international_availability_report(
        form_df,
        discipline_filter=discipline_filter,
        level_filter=level_filter,
        active_appointments_only=active_only,
        layout=layout,
    )

    if report_df.empty:
        st.info("No International Judge appointments match the current filters.")
        st.stop()

    excluded_age_out = int(meta.get("excluded_age_out") or 0)
    if excluded_age_out:
        m1, m2, m3, m4 = st.columns(4)
        m4.metric("Excluded (age 70+)", excluded_age_out)
    else:
        m1, m2, m3 = st.columns(3)
    m1.metric("Appointment rows", meta.get("appointment_count", len(report_df)))
    m2.metric("Distinct officials", meta.get("official_count", 0))
    m3.metric("Form responses matched", meta.get("form_match_count", 0))

    event_labels: list[str] = meta.get("event_short_labels") or []
    availability_codes: list[list[str]] | None = meta.get("availability_codes")

    _render_sortable_availability_table(
        report_df,
        availability_codes=availability_codes,
        event_labels=event_labels,
        dark_mode=_streamlit_dark_mode(),
    )

    st.download_button(
        "Download CSV",
        data=report_df.to_csv(index=False).encode("utf-8"),
        file_name="international_judge_availability.csv",
        mime="text/csv",
        key="intl_avail_csv_download",
    )
