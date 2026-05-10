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
        else:
            official_id = None
            score = 0.0
            directory_name = ""
        decision = "Link" if score >= default_link_min_score and official_id else "Skip"
        rows_out.append(
            {
                "judge_id": jid,
                "protocol_name": name,
                "location": loc,
                "match_score": score,
                "directory_name": directory_name,
                "official_id": official_id,
                "decision": decision,
            }
        )
    df = pd.DataFrame(rows_out)
    if not df.empty:
        df["official_id"] = df["official_id"].astype("Int64")
    return df


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
        "Adjust **official_id** or **decision**, then **Apply decisions in table**."
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

    df = _build_review_table(judges, labels, default_link_min_score=float(default_link))

    edited = st.data_editor(
        df,
        column_config={
            "judge_id": st.column_config.NumberColumn("Judge ID", disabled=True, format="%d"),
            "protocol_name": st.column_config.TextColumn("Protocol name", disabled=True, width="large"),
            "location": st.column_config.TextColumn("Protocol location", disabled=True),
            "match_score": st.column_config.NumberColumn("Best match score", disabled=True, format="%.1f"),
            "directory_name": st.column_config.TextColumn("Directory (suggested)", disabled=True, width="large"),
            "official_id": st.column_config.NumberColumn(
                "Official ID (edit)",
                help="Must match an id in officials_analysis.officials (leave empty if unsure).",
                step=1,
                format="%d",
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
                oid = row["official_id"]
                if pd.isna(oid):
                    errors.append(f"Judge {jid}: Link selected but official_id is empty.")
                    continue
                oid_int = int(oid)
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
