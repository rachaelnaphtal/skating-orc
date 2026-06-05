#!/usr/bin/env python3
"""
Re-parse protocol data for singles/pairs segments; apply rule errors, notes, max GOE.

Supports Swiss Timing FSM events (``index.htm`` / PDF protocols) and classic USFS IJS
events (``index.asp`` → Final page → Judge detail scores HTML). ``index.asp`` is
classic-HTML-only (no FSM PDF table). On ``index.htm``, when both HTML and PDF exist
for a segment, HTML is preferred.

Requires re-fetching protocols (info codes like F/e are not stored historically).
Going forward, new loads persist ``element.notes`` and ``element.max_goe_allowed``.

  python scripts/backfill_element_rule_errors.py --dry-run
  python scripts/backfill_element_rule_errors.py --competition-id 42
  python scripts/backfill_element_rule_errors.py --segment-id 1001

Chunked full reprocess (100 segments per batch):
  python scripts/backfill_element_rule_errors.py --scope all --offset 0 --limit 100
  python scripts/backfill_element_rule_errors.py --scope all --offset 100 --limit 100

Apply migrations first:
  psql "$DATABASE_URL" -f scripts/migrations/005_element_notes.sql
  psql "$DATABASE_URL" -f scripts/migrations/006_element_max_goe_allowed.sql

``--scope international`` (default) limits to ISU/international competition types.
``--scope all`` includes every singles/pairs segment with a results URL.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from bs4 import BeautifulSoup

from database import ensure_database_for_streamlit, get_database_url, get_db_session
from database_loader import DatabaseLoader
from downloadResults import (
    _iter_fsm_index_panel_rows,
    _scrape_http_session,
    download_pdf,
    findResultsDetailUrlAndJudgesNames,
    get_page_contents,
    iter_ijs_index_final_href_and_cover_event,
)
from ijs_results_urls import (
    competition_index_fetch_url,
    is_fsm_results_url,
    scrape_join_base,
)
from judgingParsing import (
    detect_element_rule_errors,
    element_protocol_metadata_from_parsed,
    ijs_event_label_to_db_segment_name,
    infer_panel_judge_names_from_parsed_scores,
    parse_scores,
    process_scores_html,
    find_segment_match_key,
    segment_name_match_key,
)
from models import Competition, Segment
from officials_competition_types import OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL
from rule_errors_policy import (
    segment_supports_element_rule_errors,
    should_flag_rule_errors,
)

# ``public.discipline_type.id``: Singles, Pairs
_SINGLES_PAIRS_DISCIPLINE_TYPE_IDS = frozenset({1, 2})


@dataclass(frozen=True)
class _BackfillIssue:
    competition_id: int
    competition_name: str
    kind: str  # "error" or "skipped"
    reason: str
    segment_id: int | None = None
    segment_name: str | None = None


@dataclass(frozen=True)
class _UnresolvedRuleError:
    competition_id: int
    competition_name: str
    segment_id: int
    segment_name: str
    skater: str
    element: str
    judge: str
    reason: str


def _record_issue(
    issues: list[_BackfillIssue],
    *,
    competition_id: int,
    competition_name: str,
    kind: str,
    reason: str,
    segment_id: int | None = None,
    segment_name: str | None = None,
) -> None:
    issues.append(
        _BackfillIssue(
            competition_id=competition_id,
            competition_name=competition_name,
            kind=kind,
            reason=reason,
            segment_id=segment_id,
            segment_name=segment_name,
        )
    )


def _print_issue_summary(issues: list[_BackfillIssue]) -> None:
    if not issues:
        return
    errors = [i for i in issues if i.kind == "error"]
    skipped = [i for i in issues if i.kind == "skipped"]
    print("", flush=True)
    print(
        f"=== Issue summary ({len(issues)} total: "
        f"{len(errors)} error(s), {len(skipped)} skipped) ===",
        flush=True,
    )
    by_comp: dict[int, list[_BackfillIssue]] = {}
    comp_names: dict[int, str] = {}
    for issue in issues:
        by_comp.setdefault(issue.competition_id, []).append(issue)
        comp_names[issue.competition_id] = issue.competition_name
    for comp_id in sorted(by_comp):
        comp_issues = by_comp[comp_id]
        name = comp_names[comp_id]
        print(f"\nCompetition {comp_id} ({name})", flush=True)
        for issue in comp_issues:
            if issue.segment_id is not None:
                label = f"  [{issue.segment_id}] {issue.segment_name!r}"
            else:
                label = "  (competition)"
            tag = "ERROR" if issue.kind == "error" else "SKIP"
            print(f"{label}: [{tag}] {issue.reason}", flush=True)


def _print_unresolved_rule_errors(unresolved: list[_UnresolvedRuleError]) -> None:
    if not unresolved:
        return
    print("", flush=True)
    print(
        f"=== Unresolved rule errors ({len(unresolved)} parsed, not flagged) ===",
        flush=True,
    )
    by_comp: dict[int, list[_UnresolvedRuleError]] = {}
    comp_names: dict[int, str] = {}
    for row in unresolved:
        by_comp.setdefault(row.competition_id, []).append(row)
        comp_names[row.competition_id] = row.competition_name
    for comp_id in sorted(by_comp):
        print(f"\nCompetition {comp_id} ({comp_names[comp_id]})", flush=True)
        for row in by_comp[comp_id]:
            print(
                f"  [{row.segment_id}] {row.segment_name!r}: "
                f"{row.skater!r} | {row.element!r} | {row.judge!r} "
                f"— {row.reason}",
                flush=True,
            )


def _fsm_scores_rows(page_contents: str, join_base: str) -> list[dict]:
    """FSM index rows with PDF protocol URLs (no per-segment panel page fetches)."""
    rows: list[dict] = []
    for row in _iter_fsm_index_panel_rows(page_contents):
        rows.append({
            "cover_label": row["cover_label"],
            "scores_url": f"{join_base}/{row['scores_href'].lstrip('/')}",
            "parse_mode": "fsm_pdf",
        })
    return rows


def _classic_scores_rows(
    page_contents: str, join_base: str, http_session
) -> list[dict]:
    """Classic ``index.asp`` rows: Final page → Judge detail scores HTML."""
    rows: list[dict] = []
    for href, cover_label in iter_ijs_index_final_href_and_cover_event(page_contents):
        details_link, judges, h1_label = findResultsDetailUrlAndJudgesNames(
            join_base, href, session=http_session
        )
        if not details_link:
            continue
        rows.append({
            "cover_label": cover_label,
            "h1_label": (h1_label or "").strip(),
            "scores_url": f"{join_base}/{details_link.lstrip('/')}",
            "parse_mode": "html",
            "judges": judges,
        })
    return rows


def _index_fetch_candidates(stored_url: str, join_base: str) -> list[str]:
    candidates = [competition_index_fetch_url(stored_url)]
    if not is_fsm_results_url(stored_url):
        candidates.append(f"{join_base.rstrip('/')}/index.htm")
    seen: set[str] = set()
    out: list[str] = []
    for url in candidates:
        if url and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _load_index_html(stored_url: str, join_base: str, http_session) -> tuple[str | None, str | None]:
    for index_url in _index_fetch_candidates(stored_url, join_base):
        html = get_page_contents(index_url, session=http_session)
        if html:
            return html, index_url
    return None, None


def _score_row_match_keys(row: dict) -> set[str]:
    keys: set[str] = set()
    for label in _row_label_variants(row):
        db_name = ijs_event_label_to_db_segment_name(label)
        if db_name:
            keys.add(segment_name_match_key(db_name))
    return keys


def _prefer_score_row(existing: dict | None, new: dict) -> bool:
    """Prefer HTML judge-detail protocols over PDF when both exist."""
    if existing is None:
        return True
    if new.get("parse_mode") == "html" and existing.get("parse_mode") != "html":
        return True
    return False


def _merge_score_rows_prefer_html(
    html_rows: list[dict],
    pdf_rows: list[dict],
) -> list[dict]:
    """Merge index rows; HTML wins over PDF for the same segment match key."""
    by_key: dict[str, dict] = {}
    for row in pdf_rows:
        for key in _score_row_match_keys(row):
            if key not in by_key:
                by_key[key] = row
    for row in html_rows:
        for key in _score_row_match_keys(row):
            if _prefer_score_row(by_key.get(key), row):
                by_key[key] = row
    seen_urls: set[str] = set()
    out: list[dict] = []
    for row in by_key.values():
        url = str(row.get("scores_url") or "")
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        out.append(row)
    return out


def _load_score_rows(
    stored_url: str,
    join_base: str,
    index_html: str,
    http_session,
) -> list[dict]:
    """
    ``index.asp``: classic Final → Judge detail HTML only (no PDF merge).

    ``index.htm``: merge classic HTML and FSM PDF when both exist; prefer HTML.
    """
    if not is_fsm_results_url(stored_url):
        rows = _classic_scores_rows(index_html, join_base, http_session)
        if rows:
            return rows
        return _fsm_scores_rows(index_html, join_base)
    html_rows = _classic_scores_rows(index_html, join_base, http_session)
    pdf_rows = _fsm_scores_rows(index_html, join_base)
    merged = _merge_score_rows_prefer_html(html_rows, pdf_rows)
    if merged:
        return merged
    return html_rows or pdf_rows


def _row_label_variants(row: dict) -> list[str]:
    labels: list[str] = []
    # Index cover often includes segment (Short/Free); Final h1 may omit it.
    for field in ("cover_label", "h1_label"):
        text = (row.get(field) or "").strip()
        if text and text not in labels:
            labels.append(text)
    return labels


def _build_segment_link_lookups(
    score_rows: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Exact DB name → row, and canonical match key → row."""
    by_db_name: dict[str, dict] = {}
    by_match_key: dict[str, dict] = {}
    for row in score_rows:
        primary_db_name = None
        for label in _row_label_variants(row):
            db_name = ijs_event_label_to_db_segment_name(label)
            if not db_name:
                continue
            if primary_db_name is None or len(db_name) > len(primary_db_name):
                primary_db_name = db_name
            enriched = {**row, "db_name": primary_db_name}
            if _prefer_score_row(by_db_name.get(db_name), enriched):
                by_db_name[db_name] = enriched
            match_key = segment_name_match_key(db_name)
            if _prefer_score_row(by_match_key.get(match_key), enriched):
                by_match_key[match_key] = enriched
    return by_db_name, by_match_key


def _parse_protocol(
    scores_url: str,
    parse_mode: str,
    *,
    http_session,
    pdf_dir: Path,
    segment_id: int,
):
    if parse_mode == "html":
        html = get_page_contents(scores_url, session=http_session)
        if not html:
            raise ValueError(f"empty judge detail HTML from {scores_url}")
        soup = BeautifulSoup(html, "html.parser")
        return process_scores_html(soup, event_regex=".*")
    pdf_path = pdf_dir / f"segment_{segment_id}.pdf"
    try:
        download_pdf(scores_url, str(pdf_path), session=http_session)
        return parse_scores(str(pdf_path), event_regex=".*", isFSM=True)
    finally:
        if pdf_path.exists():
            pdf_path.unlink()


def _resolve_segment_link(
    segment_name: str,
    by_db_name: dict[str, dict],
    by_match_key: dict[str, dict],
) -> tuple[dict | None, str | None]:
    row = by_db_name.get(segment_name)
    if row is not None:
        return row, None
    target_key = segment_name_match_key(segment_name)
    row = by_match_key.get(target_key)
    if row is not None:
        return row, None
    fuzzy_key = find_segment_match_key(segment_name, by_match_key.keys())
    if fuzzy_key is None:
        return None, None
    return by_match_key.get(fuzzy_key), fuzzy_key


def backfill_one_segment(
    loader: DatabaseLoader,
    *,
    segment: Segment,
    competition: Competition,
    scores_url: str,
    parse_mode: str,
    panel_judges: list[str] | None,
    dry_run: bool,
    pdf_dir: Path,
    http_session,
    score_maps: (
        tuple[dict[str, int], dict[int, int], dict[tuple[int, str], int]] | None
    ) = None,
    apply_rule_errors: bool | None = None,
) -> dict:
    out = {
        "segment_id": segment.id,
        "segment_name": segment.name,
        "rule_errors_flagged": 0,
        "metadata_updated": 0,
        "parsed_rule_errors": 0,
        "skipped": None,
        "error": None,
    }
    if not segment_supports_element_rule_errors(segment.name):
        out["skipped"] = "not_singles_or_pairs_segment_name"
        return out
    if segment.discipline_type_id not in _SINGLES_PAIRS_DISCIPLINE_TYPE_IDS:
        out["skipped"] = "discipline_not_singles_or_pairs"
        return out

    try:
        elements_per_skater, pcs_per_skater, _skater_details, event_name = _parse_protocol(
            scores_url,
            parse_mode,
            http_session=http_session,
            pdf_dir=pdf_dir,
            segment_id=segment.id,
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"parse failed: {exc}"
        return out

    if not elements_per_skater:
        out["skipped"] = "empty_protocol"
        return out

    judges = infer_panel_judge_names_from_parsed_scores(
        elements_per_skater, pcs_per_skater, panel_judges or []
    )
    if apply_rule_errors is None:
        apply_rule_errors = should_flag_rule_errors(
            competition.start_date, competition.end_date
        )
    rule_errors: list = []
    if apply_rule_errors:
        rule_errors = detect_element_rule_errors(
            elements_per_skater,
            judges,
            event_name or segment.name,
            competition_start_date=competition.start_date,
            competition_end_date=competition.end_date,
        )
    out["parsed_rule_errors"] = len(rule_errors)
    out["rule_errors_legacy_skipped"] = not apply_rule_errors
    parsed_event = event_name or segment.name
    element_metadata = element_protocol_metadata_from_parsed(
        elements_per_skater, parsed_event
    )

    if dry_run:
        out["metadata_updated"] = sum(
            1
            for meta in element_metadata.values()
            if meta.get("notes") is not None or meta.get("max_goe_allowed") is not None
        )
        if apply_rule_errors and rule_errors:
            preview = loader.preview_rule_errors_for_segment(
                segment.id,
                rule_errors,
                panel_judge_names=judges,
                score_maps=score_maps,
            )
            out["rule_errors_flagged"] = int(preview["flagged"])
            out["unresolved_rule_errors"] = list(preview["unresolved"])
        else:
            out["rule_errors_flagged"] = 0
            out["unresolved_rule_errors"] = []
        return out

    out["metadata_updated"] = loader.update_element_protocol_metadata_for_segment(
        segment.id,
        element_metadata,
        score_maps=score_maps,
    )
    refresh = loader.refresh_element_rule_errors(
        segment.id,
        rule_errors,
        panel_judge_names=judges,
        score_maps=score_maps,
        apply_rule_errors=apply_rule_errors,
    )
    out["rule_errors_flagged"] = int(refresh["flagged"])
    out["unresolved_rule_errors"] = list(refresh["unresolved"])
    return out


def _build_segment_query(session, args):
    q = (
        session.query(Segment, Competition)
        .join(Competition, Segment.competition_id == Competition.id)
        .filter(Segment.discipline_type_id.in_(sorted(_SINGLES_PAIRS_DISCIPLINE_TYPE_IDS)))
    )
    if args.scope == "international":
        q = q.filter(
            Competition.officials_analysis_competition_type_id.in_(
                sorted(OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL)
            )
        )
    if args.competition_id is not None:
        q = q.filter(Competition.id == args.competition_id)
    if args.segment_id is not None:
        q = q.filter(Segment.id == args.segment_id)
    if args.year is not None:
        q = q.filter(Competition.year == str(args.year))
    return q.order_by(Competition.id, Segment.id)


def _chunk_hint(args, total: int, processed: int) -> None:
    if args.limit is None:
        return
    next_offset = (args.offset or 0) + processed
    if next_offset >= total:
        print("No further chunks remain.", flush=True)
        return
    cmd = (
        f"python scripts/backfill_element_rule_errors.py "
        f"--scope {args.scope} --offset {next_offset} --limit {args.limit}"
    )
    if args.year is not None:
        cmd += f" --year {args.year}"
    if args.dry_run:
        cmd += " --dry-run"
    print(f"Next chunk: {cmd}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--competition-id", type=int, default=None)
    parser.add_argument("--segment-id", type=int, default=None)
    parser.add_argument(
        "--scope",
        choices=("international", "all"),
        default="international",
        help="international = ISU/intl types only; all = every singles/pairs segment.",
    )
    parser.add_argument(
        "--year",
        default=None,
        help="Filter by competition.year (e.g. 2526).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip this many segments after filters (for chunked runs).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max segments to process in this chunk (after offset).",
    )
    args = parser.parse_args()

    ensure_database_for_streamlit()
    db_url = get_database_url()
    host_hint = db_url.split("@")[-1].split("/")[0] if "@" in db_url else "(local)"
    print(f"Database host: {host_hint}", flush=True)

    session = get_db_session()
    loader = DatabaseLoader(session, defer_commits=True)
    http_session = _scrape_http_session()
    exit_code = 0
    try:
        base_q = _build_segment_query(session, args)
        total_matching = base_q.count()
        q = base_q
        if args.offset:
            q = q.offset(args.offset)
        if args.limit is not None:
            q = q.limit(args.limit)
        rows = q.all()
        if not rows:
            print(
                f"No matching segments for scope={args.scope!r} "
                f"(offset={args.offset}, limit={args.limit}, total={total_matching}).",
                flush=True,
            )
            return 0

        chunk_end = args.offset + len(rows)
        print(
            f"Chunk: segments {args.offset + 1}-{chunk_end} of {total_matching} "
            f"(scope={args.scope}, limit={args.limit})",
            flush=True,
        )

        by_competition: dict[int, tuple[Competition, list[Segment]]] = {}
        for segment, competition in rows:
            by_competition.setdefault(competition.id, (competition, []))[1].append(
                segment
            )

        issues: list[_BackfillIssue] = []
        unresolved_rows: list[_UnresolvedRuleError] = []
        totals = {
            "segments": 0,
            "flagged": 0,
            "metadata": 0,
            "parsed_errors": 0,
            "unresolved": 0,
            "legacy_skipped": 0,
            "skipped": 0,
            "errors": 0,
        }
        chunk_processed = 0

        with tempfile.TemporaryDirectory(prefix="rule_error_backfill_") as tmp:
            pdf_dir = Path(tmp)
            for comp_id, (competition, segments) in sorted(by_competition.items()):
                stored_url = (competition.results_url or "").strip()
                if not stored_url:
                    reason = "missing results_url"
                    print(
                        f"Skip competition {comp_id} ({competition.name!r}): {reason}",
                        flush=True,
                    )
                    for segment in segments:
                        _record_issue(
                            issues,
                            competition_id=comp_id,
                            competition_name=competition.name,
                            kind="skipped",
                            reason=reason,
                            segment_id=segment.id,
                            segment_name=segment.name,
                        )
                    totals["skipped"] += len(segments)
                    continue

                join_base = scrape_join_base(stored_url).rstrip("/")
                index_html, index_url = _load_index_html(
                    stored_url, join_base, http_session
                )
                if not index_html:
                    reason = (
                        f"failed to fetch index "
                        f"({', '.join(_index_fetch_candidates(stored_url, join_base))})"
                    )
                    print(
                        f"Skip competition {comp_id}: {reason}",
                        flush=True,
                    )
                    for segment in segments:
                        _record_issue(
                            issues,
                            competition_id=comp_id,
                            competition_name=competition.name,
                            kind="error",
                            reason=reason,
                            segment_id=segment.id,
                            segment_name=segment.name,
                        )
                    totals["errors"] += len(segments)
                    continue

                score_rows = _load_score_rows(
                    stored_url, join_base, index_html, http_session
                )
                by_db_name, by_match_key = _build_segment_link_lookups(score_rows)
                n_html = sum(1 for r in score_rows if r.get("parse_mode") == "html")
                n_pdf = sum(1 for r in score_rows if r.get("parse_mode") == "fsm_pdf")

                print(
                    f"Competition {comp_id} ({competition.name!r}): "
                    f"{len(segments)} segment(s), {len(by_db_name)} protocol link(s) "
                    f"({n_html} HTML, {n_pdf} PDF) via {index_url}",
                    flush=True,
                )

                comp_score_maps = loader.segment_element_score_maps_for_competition(
                    comp_id
                )
                comp_apply_rule_errors = should_flag_rule_errors(
                    competition.start_date, competition.end_date
                )
                comp_dirty = False
                for segment in segments:
                    if args.limit is not None and chunk_processed >= args.limit:
                        break
                    totals["segments"] += 1
                    chunk_processed += 1
                    link, fuzzy_key = _resolve_segment_link(
                        segment.name, by_db_name, by_match_key
                    )
                    if not link:
                        keys = sorted(by_match_key)
                        reason = (
                            f"no protocol link (match key "
                            f"{segment_name_match_key(segment.name)!r}; "
                            f"index keys: {keys})"
                        )
                        print(
                            f"  [{segment.id}] {segment.name!r}: {reason}",
                            flush=True,
                        )
                        _record_issue(
                            issues,
                            competition_id=comp_id,
                            competition_name=competition.name,
                            kind="skipped",
                            reason=reason,
                            segment_id=segment.id,
                            segment_name=segment.name,
                        )
                        totals["skipped"] += 1
                        continue

                    if fuzzy_key is not None:
                        print(
                            f"  [{segment.id}] {segment.name!r}: fuzzy-matched index "
                            f"key {fuzzy_key!r}",
                            flush=True,
                        )
                    elif link["db_name"] != segment.name:
                        print(
                            f"  [{segment.id}] {segment.name!r}: matched index "
                            f"{link['cover_label']!r} → {link['db_name']!r}",
                            flush=True,
                        )

                    result = backfill_one_segment(
                        loader,
                        segment=segment,
                        competition=competition,
                        scores_url=link["scores_url"],
                        parse_mode=link.get("parse_mode") or "fsm_pdf",
                        panel_judges=link.get("judges"),
                        dry_run=args.dry_run,
                        pdf_dir=pdf_dir,
                        http_session=http_session,
                        score_maps=comp_score_maps.get(segment.id),
                        apply_rule_errors=comp_apply_rule_errors,
                    )
                    if result.get("error"):
                        print(
                            f"  [{segment.id}] {segment.name!r}: {result['error']}",
                            flush=True,
                        )
                        _record_issue(
                            issues,
                            competition_id=comp_id,
                            competition_name=competition.name,
                            kind="error",
                            reason=result["error"],
                            segment_id=segment.id,
                            segment_name=segment.name,
                        )
                        totals["errors"] += 1
                    elif result.get("skipped"):
                        reason = str(result["skipped"])
                        print(
                            f"  [{segment.id}] {segment.name!r}: skipped ({reason})",
                            flush=True,
                        )
                        _record_issue(
                            issues,
                            competition_id=comp_id,
                            competition_name=competition.name,
                            kind="skipped",
                            reason=reason,
                            segment_id=segment.id,
                            segment_name=segment.name,
                        )
                        totals["skipped"] += 1
                    else:
                        comp_dirty = True
                        totals["flagged"] += int(result["rule_errors_flagged"])
                        totals["metadata"] += int(result["metadata_updated"])
                        totals["parsed_errors"] += int(result["parsed_rule_errors"])
                        if result.get("rule_errors_legacy_skipped"):
                            totals["legacy_skipped"] += 1
                        for row in result.get("unresolved_rule_errors") or []:
                            unresolved_rows.append(
                                _UnresolvedRuleError(
                                    competition_id=comp_id,
                                    competition_name=competition.name,
                                    segment_id=segment.id,
                                    segment_name=segment.name,
                                    skater=str(row.get("skater", "")),
                                    element=str(row.get("element", "")),
                                    judge=str(row.get("judge", "")),
                                    reason=str(row.get("reason", "")),
                                )
                            )
                        totals["unresolved"] += len(
                            result.get("unresolved_rule_errors") or []
                        )
                        line = (
                            f"  [{segment.id}] {segment.name!r}: "
                            f"{result['parsed_rule_errors']} parsed, "
                            f"{result['rule_errors_flagged']} flagged, "
                            f"{result['metadata_updated']} element metadata updated"
                        )
                        if result.get("rule_errors_legacy_skipped"):
                            line += " (pre-2018-19: rule errors skipped)"
                        elif result.get("unresolved_rule_errors"):
                            line += (
                                f" ({len(result['unresolved_rule_errors'])} unresolved)"
                            )
                        print(line, flush=True)

                if comp_dirty and not args.dry_run:
                    loader.commit()

                if args.limit is not None and chunk_processed >= args.limit:
                    break

        mode = "dry-run" if args.dry_run else "applied"
        print(
            f"Done ({mode}): {totals['segments']} segment(s), "
            f"{totals['parsed_errors']} rule error(s) parsed, "
            f"{totals['flagged']} flagged, "
            f"{totals['unresolved']} unresolved, "
            f"{totals['legacy_skipped']} pre-2018-19 segment(s) (rule errors skipped), "
            f"{totals['metadata']} element metadata updated, "
            f"{totals['skipped']} skipped, {totals['errors']} error(s)",
            flush=True,
        )
        _print_issue_summary(issues)
        _print_unresolved_rule_errors(unresolved_rows)
        _chunk_hint(args, total_matching, chunk_processed)
        exit_code = 1 if totals["errors"] > 0 else 0
    finally:
        session.close()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
