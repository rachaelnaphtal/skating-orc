"""
Streamlit UI: fuzzy-match protocol judge names to officials_analysis.officials, then approve in bulk.

Run from repo root (same DATABASE_URL as analysis_app):

    streamlit run judge_official_matcher_app.py

Ties links to `officials_analysis.officials` via `judge_official_link.official_id` (PostgreSQL FK).

Embedded from the main app's Admin page via ``render_judge_official_matcher(embedded=True)``.
"""

from __future__ import annotations

import hashlib

import pandas as pd
import streamlit as st

import judge_official_link_core as core


@st.cache_resource
def _engine():
    eng = core.make_engine()
    core.ensure_table(eng)
    return eng


@st.cache_data(ttl=120)
def _official_labels() -> dict[int, str]:
    eng = _engine()
    with eng.connect() as conn:
        return core.fetch_official_choices(conn)


@st.cache_data(ttl=120)
def _official_labels_normalized() -> dict[int, str]:
    return core.normalize_name_choices(_official_labels())


@st.cache_data(ttl=120)
def _isu_official_labels() -> dict[int, str]:
    eng = _engine()
    with eng.connect() as conn:
        return core.fetch_isu_official_choices(conn)


@st.cache_data(ttl=120)
def _isu_official_labels_normalized() -> dict[int, str]:
    return core.normalize_name_choices(_isu_official_labels())


def _judges_editor_key(prefix: str, judges: list[dict]) -> str:
    """Stable widget key so ``st.data_editor`` resets when the loaded judge set changes."""
    ids = ",".join(str(int(j["id"])) for j in judges)
    digest = hashlib.md5(ids.encode(), usedforsecurity=False).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _embedded_matcher_layout() -> None:
    """Admin embed: use full content width (main app layout is already wide)."""
    st.markdown(
        """
        <style>
        section.main .block-container {
            max-width: 100%;
            padding-left: 1.25rem;
            padding-right: 1.25rem;
        }
        div[data-testid="stDataEditor"] {
            width: 100%;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _load_unmapped(limit: int, *, include_marked_outside: bool = False) -> list[dict]:
    eng = _engine()
    with eng.connect() as conn:
        rows = core.fetch_judges_needing_link(
            conn,
            limit=limit,
            include_marked_outside=include_marked_outside,
        )
    return [dict(r) for r in rows]


def _build_review_table(
    judges: list[dict],
    labels: dict[int, str],
    isu_labels: dict[int, str],
    *,
    default_link_min_score: float,
    default_isu_link_min_score: float,
    id_to_display: dict[int, str],
    isu_id_to_display: dict[int, str],
    us_normalized: dict[int, str] | None = None,
    isu_normalized: dict[int, str] | None = None,
) -> pd.DataFrame:
    us_norm = us_normalized or core.normalize_name_choices(labels)
    isu_norm = isu_normalized or core.normalize_name_choices(isu_labels)
    rows_out: list[dict] = []
    for j in judges:
        jid = int(j["id"])
        name = j.get("name") or ""
        best = core.suggest_matches(
            name, labels, top=1, min_score=0.0, normalized_choices=us_norm
        )
        if best:
            oid, sc, lbl = best[0]
            official_id: int | None = int(oid)
            us_score = round(float(sc), 1)
            us_directory_name = lbl
            us_directory_official = id_to_display.get(official_id, lbl)
        else:
            official_id = None
            us_score = 0.0
            us_directory_name = ""
            us_directory_official = ""

        isu_best = core.suggest_matches(
            name, isu_labels, top=1, min_score=0.0, normalized_choices=isu_norm
        )
        if isu_best:
            ioid, isu_sc, isu_lbl = isu_best[0]
            isu_official_id: int | None = int(ioid)
            isu_score = round(float(isu_sc), 1)
            isu_directory_name = isu_lbl
            isu_directory_official = isu_id_to_display.get(isu_official_id, isu_lbl)
        else:
            isu_official_id = None
            isu_score = 0.0
            isu_directory_name = ""
            isu_directory_official = ""

        if us_score >= default_link_min_score and official_id:
            decision = "Link US"
        elif isu_score >= default_isu_link_min_score and isu_official_id:
            decision = "Link ISU"
        else:
            decision = "Skip"
        rows_out.append(
            {
                "judge_id": jid,
                "protocol_name": name,
                "us_match_score": us_score,
                "us_suggested": us_directory_name,
                "us_directory_official": us_directory_official,
                "isu_match_score": isu_score,
                "isu_suggested": isu_directory_name,
                "isu_directory_official": isu_directory_official,
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
    *,
    us_normalized: dict[int, str] | None = None,
) -> pd.DataFrame:
    """Checkboxes + minimal columns for bulk ``outside_directory`` marking."""
    us_norm = us_normalized or core.normalize_name_choices(labels)
    rows_out: list[dict] = []
    for j in judges:
        jid = int(j["id"])
        name = j.get("name") or ""
        best = core.suggest_matches(
            name, labels, top=1, min_score=0.0, normalized_choices=us_norm
        )
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
    else:
        _embedded_matcher_layout()

    flash = st.session_state.pop("_matcher_flash", None)
    if flash:
        st.success(flash)

    st.title("Judge ↔ directory matcher")
    st.caption(
        "Links protocol names to the **USFS directory** (`officials_analysis.officials`) and/or "
        "the **ISU roster** (`officials_analysis.isu_official`). US matches are tried first at "
        "scrape time; ISU is used when there is no US link. International judges: use **Link ISU**, "
        "not **Outside** (outside only means “not in the US directory”). Load ISU lists with "
        "`scripts/load_isu_officials_pdf.py`."
    )

    try:
        eng = _engine()
    except Exception as e:
        st.error(
            f"Could not connect or prepare link tables (check DATABASE_URL and permissions): {e}"
        )
        st.stop()

    labels = _official_labels()
    isu_labels = _isu_official_labels()
    full_select_options, display_to_id, id_to_display = core.official_select_display_maps(
        labels
    )
    isu_select_options, isu_display_to_id, isu_id_to_display = (
        core.isu_official_select_display_maps(isu_labels)
    )
    st.sidebar.metric("US directory officials", len(labels))
    st.sidebar.metric("ISU roster officials", len(isu_labels))

    max_rows = st.sidebar.number_input("Max unmapped judges to load", 10, 5000, 400, 10)
    auto_min = st.sidebar.slider("Auto-link US: minimum fuzzy score", 50.0, 100.0, 92.0, 1.0)
    auto_isu_min = st.sidebar.slider(
        "Auto-link ISU: minimum fuzzy score", 50.0, 100.0, 92.0, 1.0
    )
    default_link = st.sidebar.slider(
        "Table default: Link US when score ≥", 50.0, 100.0, 85.0, 1.0
    )
    default_isu_link = st.sidebar.slider(
        "Table default: Link ISU when score ≥ (if US below threshold)",
        50.0,
        100.0,
        85.0,
        1.0,
    )
    include_outside = st.sidebar.checkbox(
        "Include judges marked outside",
        value=False,
        help="Show outside_directory rows so you can Link ISU (clears the outside mark) "
        "or clear via: python scripts/judge_official_admin.py clear JUDGE_ID",
    )

    judges = _load_unmapped(int(max_rows), include_marked_outside=include_outside)
    st.sidebar.metric("Unmapped judges (loaded)", len(judges))

    if not judges:
        st.info(
            "No judges need linking — everyone is US **linked**, marked **outside**, "
            "or has an ISU roster link."
        )
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
        with st.spinner("Building table…"):
            df_out = _build_outside_bulk_table(
                judges,
                labels,
                us_normalized=_official_labels_normalized(),
            )
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
            key=_judges_editor_key("outside_bulk_editor", judges),
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
            _clear_matcher_caches()
            st.rerun()
        st.stop()

    st.subheader("Review and apply")
    st.write(
        "Each row is a protocol name with the **best fuzzy match** to the directory. "
        "Choose the **directory official** (sidebar filter narrows the list), set **decision**, "
        "then **Apply decisions in table**."
    )

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button(
            f"Auto-link US (score ≥ {auto_min:.0f})",
            type="primary",
        ):
            with st.spinner("Writing US links…"):
                linked, skipped = core.auto_link_by_score(
                    eng,
                    officials=labels,
                    min_score=float(auto_min),
                    limit_judges=int(max_rows),
                    normalized_officials=_official_labels_normalized(),
                )
            st.session_state["_matcher_flash"] = (
                f"US auto-linked **{linked}** judge(s). Skipped: **{skipped}**."
            )
            _clear_matcher_caches()
            st.rerun()
    with col_b:
        if st.button(
            f"Auto-link ISU (score ≥ {auto_isu_min:.0f})",
            type="primary",
            disabled=len(isu_labels) == 0,
        ):
            with st.spinner("Writing ISU links…"):
                linked, skipped = core.auto_link_isu_by_score(
                    eng,
                    isu_officials=isu_labels,
                    min_score=float(auto_isu_min),
                    limit_judges=int(max_rows),
                    normalized_isu_officials=_isu_official_labels_normalized(),
                )
            st.session_state["_matcher_flash"] = (
                f"ISU auto-linked **{linked}** judge(s). Skipped: **{skipped}**."
            )
            _clear_matcher_caches()
            st.rerun()

    with st.spinner("Building match table (fuzzy match against directories)…"):
        df = _build_review_table(
            judges,
            labels,
            isu_labels,
            default_link_min_score=float(default_link),
            default_isu_link_min_score=float(default_isu_link),
            id_to_display=id_to_display,
            isu_id_to_display=isu_id_to_display,
            us_normalized=_official_labels_normalized(),
            isu_normalized=_isu_official_labels_normalized(),
        )
    pinned = {str(x) for x in df["us_directory_official"].dropna() if str(x).strip()}
    isu_pinned = {str(x) for x in df["isu_directory_official"].dropna() if str(x).strip()}
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
    isu_select_filtered = _filter_official_select_options(
        isu_select_options,
        name_filter,
        pinned=isu_pinned,
    )
    if name_filter.strip() and len(select_options) <= 1:
        st.sidebar.caption("No US matches — clear or broaden the filter.")

    edited = st.data_editor(
        df,
        column_config={
            "judge_id": st.column_config.NumberColumn("ID", disabled=True, width="small", format="%d"),
            "protocol_name": st.column_config.TextColumn(
                "Protocol name",
                disabled=True,
                width="medium",
            ),
            "us_match_score": st.column_config.NumberColumn(
                "US",
                disabled=True,
                width="small",
                format="%.0f",
            ),
            "us_suggested": st.column_config.TextColumn(
                "US suggested",
                disabled=True,
                width="medium",
            ),
            "us_directory_official": st.column_config.SelectboxColumn(
                "US pick",
                options=select_options,
                required=False,
                width="medium",
            ),
            "isu_match_score": st.column_config.NumberColumn(
                "ISU",
                disabled=True,
                width="small",
                format="%.0f",
            ),
            "isu_suggested": st.column_config.TextColumn(
                "ISU suggested",
                disabled=True,
                width="medium",
            ),
            "isu_directory_official": st.column_config.SelectboxColumn(
                "ISU pick",
                options=isu_select_filtered,
                required=False,
                width="medium",
            ),
            "decision": st.column_config.SelectboxColumn(
                "Decision",
                options=["Skip", "Link US", "Link ISU", "Outside"],
                required=True,
                width="small",
            ),
        },
        hide_index=True,
        num_rows="fixed",
        width="stretch",
        key=_judges_editor_key("review_editor", judges),
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
                core.upsert_outside(eng, jid, note="matcher: outside both directories")
                applied += 1
                continue
            if dec == "Link US":
                disp = row.get("us_directory_official")
                if disp is None or (isinstance(disp, float) and pd.isna(disp)) or str(disp).strip() == "":
                    errors.append(
                        f"Judge {jid}: Link US — pick a **US directory official** or change decision."
                    )
                    continue
                disp_s = str(disp).strip()
                oid_int = display_to_id.get(disp_s)
                if oid_int is None:
                    errors.append(
                        f"Judge {jid}: could not resolve US row {disp_s!r}."
                    )
                    continue
                if not core.official_exists(eng, oid_int):
                    errors.append(
                        f"Judge {jid}: official_id {oid_int} not found in officials_analysis.officials."
                    )
                    continue
                core.upsert_link(eng, jid, oid_int, note="matcher: US approved")
                applied += 1
                continue
            if dec == "Link ISU":
                disp = row.get("isu_directory_official")
                if disp is None or (isinstance(disp, float) and pd.isna(disp)) or str(disp).strip() == "":
                    errors.append(
                        f"Judge {jid}: Link ISU — pick an **ISU roster official** or change decision."
                    )
                    continue
                disp_s = str(disp).strip()
                ioid_int = isu_display_to_id.get(disp_s)
                if ioid_int is None:
                    errors.append(
                        f"Judge {jid}: could not resolve ISU row {disp_s!r}."
                    )
                    continue
                if not core.isu_official_exists(eng, ioid_int):
                    errors.append(
                        f"Judge {jid}: isu_official_id {ioid_int} not found."
                    )
                    continue
                core.upsert_isu_link(eng, jid, ioid_int, note="matcher: ISU approved")
                applied += 1
        if errors:
            for e in errors:
                st.warning(e)
        st.session_state["_matcher_flash"] = f"Applied **{applied}** row(s) from the table."
        _clear_matcher_caches()
        st.rerun()


def _clear_matcher_caches() -> None:
    _official_labels.clear()
    _official_labels_normalized.clear()
    _isu_official_labels.clear()
    _isu_official_labels_normalized.clear()


def main() -> None:
    render_judge_official_matcher(embedded=False)


if __name__ == "__main__":
    main()
