"""
Streamlit UI: fuzzy-match protocol judge names to officials_analysis.officials, then approve in bulk.

Run from repo root (same DATABASE_URL as analysis_app):

    streamlit run judge_official_matcher_app.py

Ties links to `officials_analysis.officials` via `judge_official_link.official_id` (PostgreSQL FK).

Embedded from the main app's Admin page via ``render_judge_official_matcher(embedded=True)``.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

import judge_official_link_core as core


@st.cache_resource
def _engine():
    return core.make_engine()


@st.cache_data(ttl=120)
def _official_labels() -> dict[int, str]:
    eng = _engine()
    with eng.connect() as conn:
        return core.fetch_official_choices(conn)


def _load_unmapped(limit: int) -> list[dict]:
    eng = _engine()
    with eng.connect() as conn:
        rows = core.fetch_unmapped_judges(conn, limit=limit)
    return [dict(r) for r in rows]


def _build_review_table(
    judges: list[dict],
    labels: dict[int, str],
    *,
    default_link_min_score: float,
    id_to_display: dict[int, str],
) -> pd.DataFrame:
    rows_out: list[dict] = []
    for j in judges:
        jid = int(j["id"])
        name = j.get("name") or ""
        best = core.suggest_matches(name, labels, top=1, min_score=0.0)
        if best:
            oid, sc, lbl = best[0]
            official_id: int | None = int(oid)
            score = round(float(sc), 1)
            directory_name = lbl
            directory_official = id_to_display.get(official_id, lbl)
        else:
            official_id = None
            score = 0.0
            directory_name = ""
            directory_official = ""
        decision = "Link" if score >= default_link_min_score and official_id else "Skip"
        rows_out.append(
            {
                "judge_id": jid,
                "protocol_name": name,
                "match_score": score,
                "directory_name": directory_name,
                "directory_official": directory_official,
                "decision": decision,
            }
        )
    return pd.DataFrame(rows_out)


def _filter_official_select_options(
    full_options: list[str],
    needle: str,
    *,
    pinned: set[str],
) -> list[str]:
    """``full_options`` includes leading ``""``. Narrow by substring; keep ``pinned`` values visible."""
    rest = [o for o in full_options if o]
    n = needle.strip().casefold()
    if not n:
        out = list(rest)
    else:
        out = [o for o in rest if n in o.casefold()]
    for p in pinned:
        if p and p not in out:
            out.append(p)
    return [""] + sorted(set(out), key=str.casefold)


def _build_outside_bulk_table(
    judges: list[dict],
    labels: dict[int, str],
) -> pd.DataFrame:
    """Checkboxes + minimal columns for bulk ``outside_directory`` marking."""
    rows_out: list[dict] = []
    for j in judges:
        jid = int(j["id"])
        name = j.get("name") or ""
        best = core.suggest_matches(name, labels, top=1, min_score=0.0)
        if best:
            _oid, sc, lbl = best[0]
            score = round(float(sc), 1)
            suggested = lbl
        else:
            score = 0.0
            suggested = ""
        rows_out.append(
            {
                "mark_outside": False,
                "judge_id": jid,
                "protocol_name": name,
                "suggested": suggested,
                "match_score": score,
            }
        )
    return pd.DataFrame(rows_out)


def render_judge_official_matcher(*, embedded: bool = False) -> None:
    """
    Full matcher UI. When ``embedded`` is False (standalone app), configures the Streamlit page.
    """
    if not embedded:
        st.set_page_config(
            page_title="Judge ↔ directory matcher",
            page_icon="🔗",
            layout="wide",
        )

    flash = st.session_state.pop("_matcher_flash", None)
    if flash:
        st.success(flash)

    st.title("Judge ↔ US directory matcher")
    st.caption(
        "Links `public.judge` rows to **`officials_analysis.officials`** (same database as the "
        "activity tracker). International judges: set decision to **Outside** so auto-matcher "
        "stops suggesting US roster rows."
    )

    try:
        eng = _engine()
    except Exception as e:
        st.error(str(e))
        st.stop()

    try:
        core.ensure_table(eng)
    except Exception as e:
        st.error(
            f"Could not create or open `judge_official_link` (check DATABASE_URL and permissions): {e}"
        )
        st.stop()

    labels = _official_labels()
    full_select_options, display_to_id, id_to_display = core.official_select_display_maps(
        labels
    )
    st.sidebar.metric("Officials in directory (with name)", len(labels))

    max_rows = st.sidebar.number_input("Max unmapped judges to load", 10, 5000, 400, 10)
    auto_min = st.sidebar.slider("Auto-link: minimum fuzzy score", 50.0, 100.0, 92.0, 1.0)
    default_link = st.sidebar.slider(
        "Table default: pre-select Link when score ≥", 50.0, 100.0, 85.0, 1.0
    )

    judges = _load_unmapped(int(max_rows))
    st.sidebar.metric("Unmapped judges (loaded)", len(judges))

    if not judges:
        st.info("No unmapped judges — everyone has a row in `judge_official_link`.")
        st.stop()

    workflow = st.radio(
        "Workflow",
        ("Review & link", "Bulk mark outside"),
        horizontal=True,
        help="Bulk mode: tick judges who are not in the US directory (e.g. international), then apply once.",
    )

    if workflow == "Bulk mark outside":
        st.subheader("Mark non–US directory judges")
        st.write(
            "Tick **Outside** for everyone who should stop getting US roster fuzzy matches, "
            "then **Apply outside marks**. (Uncheck mistakes before applying.)"
        )
        df_out = _build_outside_bulk_table(judges, labels)
        edited_out = st.data_editor(
            df_out,
            column_config={
                "mark_outside": st.column_config.CheckboxColumn(
                    "Outside",
                    help="Mark this protocol judge as outside the US directory.",
                    default=False,
                ),
                "judge_id": st.column_config.NumberColumn("ID", disabled=True, width="small", format="%d"),
                "protocol_name": st.column_config.TextColumn(
                    "Protocol name",
                    disabled=True,
                    width="medium",
                ),
                "suggested": st.column_config.TextColumn(
                    "Suggested",
                    disabled=True,
                    width="medium",
                    help="Best fuzzy match to a US directory name (for context).",
                ),
                "match_score": st.column_config.NumberColumn(
                    "Match",
                    disabled=True,
                    width="small",
                    help="Fuzzy score for the suggested directory row.",
                    format="%.0f",
                ),
            },
            hide_index=True,
            num_rows="fixed",
            width="stretch",
            key="outside_bulk_editor",
        )
        n_marked = int(edited_out["mark_outside"].fillna(False).astype(bool).sum())
        if st.button(
            f"Apply: mark {n_marked} selected as outside directory",
            type="primary",
            disabled=n_marked == 0,
        ):
            applied = 0
            eng = _engine()
            for _, row in edited_out.iterrows():
                if not bool(row.get("mark_outside")):
                    continue
                core.upsert_outside(
                    eng,
                    int(row["judge_id"]),
                    note="matcher: outside directory (bulk)",
                )
                applied += 1
            st.session_state["_matcher_flash"] = f"Marked **{applied}** judge(s) as outside directory."
            _official_labels.clear()
            st.rerun()
        st.stop()

    st.subheader("Review and apply")
    st.write(
        "Each row is a protocol name with the **best fuzzy match** to the directory. "
        "Choose the **directory official** (sidebar filter narrows the list), set **decision**, "
        "then **Apply decisions in table**."
    )

    if st.button(
        f"Auto-link every loaded unmapped judge whose best match score ≥ {auto_min:.0f}",
        type="primary",
    ):
        with st.spinner("Writing links…"):
            linked, skipped = core.auto_link_by_score(
                eng,
                officials=labels,
                min_score=float(auto_min),
                limit_judges=int(max_rows),
            )
        st.session_state["_matcher_flash"] = (
            f"Auto-linked **{linked}** judge(s). Skipped: **{skipped}**."
        )
        _official_labels.clear()
        st.rerun()

    df = _build_review_table(
        judges,
        labels,
        default_link_min_score=float(default_link),
        id_to_display=id_to_display,
    )
    pinned = {str(x) for x in df["directory_official"].dropna() if str(x).strip()}
    name_filter = st.sidebar.text_input(
        "Filter directory names",
        value="",
        help="Narrows the **Directory official** dropdown (substring match, case-insensitive). "
        "Suggested picks for loaded rows stay in the list.",
        placeholder="e.g. Smith",
    )
    select_options = _filter_official_select_options(
        full_select_options,
        name_filter,
        pinned=pinned,
    )
    if name_filter.strip() and len(select_options) <= 1:
        st.sidebar.caption("No matches — clear or broaden the filter.")

    edited = st.data_editor(
        df,
        column_config={
            "judge_id": st.column_config.NumberColumn("ID", disabled=True, width="small", format="%d"),
            "protocol_name": st.column_config.TextColumn(
                "Protocol name",
                disabled=True,
                width="medium",
            ),
            "match_score": st.column_config.NumberColumn(
                "Match",
                disabled=True,
                width="small",
                format="%.0f",
            ),
            "directory_name": st.column_config.TextColumn(
                "Suggested",
                disabled=True,
                width="medium",
            ),
            "directory_official": st.column_config.SelectboxColumn(
                "Directory pick",
                help="Directory entry to link (duplicate names show · id …).",
                options=select_options,
                required=False,
                width="medium",
            ),
            "decision": st.column_config.SelectboxColumn(
                "Decision",
                options=["Skip", "Link", "Outside"],
                required=True,
                width="small",
            ),
        },
        hide_index=True,
        num_rows="fixed",
        width="stretch",
        key="review_editor",
    )

    if st.button("Apply decisions in table", type="secondary"):
        applied = 0
        errors: list[str] = []
        eng = _engine()
        for _, row in edited.iterrows():
            jid = int(row["judge_id"])
            dec = row["decision"]
            if dec == "Skip":
                continue
            if dec == "Outside":
                core.upsert_outside(eng, jid, note="matcher: outside directory")
                applied += 1
                continue
            if dec == "Link":
                disp = row.get("directory_official")
                if disp is None or (isinstance(disp, float) and pd.isna(disp)) or str(disp).strip() == "":
                    errors.append(
                        f"Judge {jid}: Link selected — pick a **Directory official** or change decision."
                    )
                    continue
                disp_s = str(disp).strip()
                oid_int = display_to_id.get(disp_s)
                if oid_int is None:
                    errors.append(
                        f"Judge {jid}: could not resolve directory row {disp_s!r}. "
                        "Adjust the filter so that name appears in the list, then re-select."
                    )
                    continue
                if not core.official_exists(eng, oid_int):
                    errors.append(
                        f"Judge {jid}: official_id {oid_int} not found in officials_analysis.officials."
                    )
                    continue
                core.upsert_link(eng, jid, oid_int, note="matcher: approved")
                applied += 1
        if errors:
            for e in errors:
                st.warning(e)
        st.session_state["_matcher_flash"] = f"Applied **{applied}** row(s) from the table."
        _official_labels.clear()
        st.rerun()


def main() -> None:
    render_judge_official_matcher(embedded=False)


if __name__ == "__main__":
    main()
