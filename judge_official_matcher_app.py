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
        loc = j.get("location") or ""
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
                "location": loc,
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

    st.subheader("Review and apply")
    st.write(
        "Each row is a protocol name with the **best fuzzy match** to the directory. "
        "Choose the **directory official** (search names in the sidebar filter), set **decision**, "
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
            "judge_id": st.column_config.NumberColumn("Judge ID", disabled=True, format="%d"),
            "protocol_name": st.column_config.TextColumn("Protocol name", disabled=True, width="large"),
            "location": st.column_config.TextColumn("Protocol location", disabled=True),
            "match_score": st.column_config.NumberColumn("Best match score", disabled=True, format="%.1f"),
            "directory_name": st.column_config.TextColumn("Directory (suggested)", disabled=True, width="large"),
            "directory_official": st.column_config.SelectboxColumn(
                "Directory official",
                help="Directory entry to link (same strings as US roster names; duplicate names show · id …).",
                options=select_options,
                required=False,
            ),
            "decision": st.column_config.SelectboxColumn(
                "Decision",
                options=["Skip", "Link", "Outside"],
                required=True,
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
