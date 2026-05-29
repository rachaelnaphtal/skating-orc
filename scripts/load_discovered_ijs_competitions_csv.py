#!/usr/bin/env python3
"""
Import USFS IJS competitions listed in a ``discover_usfs_ijs_competitions.py`` CSV.

**Default: full scrape** — for each eligible row, runs ``downloadResults.scrape`` with
``write_to_database=True``, ``write_excel=False``, ``use_gcp=False`` — the same pattern as
the **Load Competition** page in ``analysis_app.py`` (segments, scores, officials, etc.).

**Metadata-only** — ``--metadata-only`` only upserts ``public.competition`` (shell row +
dates/location/name); no segment/score ingest.

URL handling matches the rest of the repo: ``competition.results_url`` is the full
results entry URL (typically ending in ``/index.asp`` for classic events). FSM events
are any URL that does not end with ``/index.asp`` (no assumed ``/index.htm``).

Required CSV columns (discover script):

  year, competition_id, url, http_status, competition_name, start_date, end_date,
  location, fetched_at_utc, probe_error

Optional CSV columns:

  season_year — season code for that row (e.g. 2526). Overrides ``--season-year`` when set.
  officials_analysis_competition_type_id or competition_type_id — per-row FK to
    ``officials_analysis.competition_type`` (same as Load Competition selector).
  qualifying, nqs — if both are set on a row, they override flags derived from the type id.

Full scrape requires an officials competition type id for each row: pass
``--officials-analysis-competition-type-id`` and/or put the id column on every row that
needs it.

Examples::

  python scripts/load_discovered_ijs_competitions_csv.py 2025_hits.csv --dry-run \\
      --officials-analysis-competition-type-id 11

  python scripts/load_discovered_ijs_competitions_csv.py 2025_hits.csv \\
      --officials-analysis-competition-type-id 11 --season-year 2526

  python scripts/load_discovered_ijs_competitions_csv.py 2025_hits.csv --metadata-only

Requires ``DATABASE_URL`` (or PG* env vars). Chromium may start for PDF-mode segments when
not using HTML-only paths (same as a normal scrape).

Full usage, flags, and workflow: scripts/README.md

Full scrape reuses one HTTP session and DB session for the whole CSV run, passes discover
dates/location into ``scrape()`` (skips an extra index fetch when both dates are present),
and commits once per competition instead of after every segment. One-off loads from the
apps are unchanged (they do not pass those options).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import date, datetime

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from database import get_db_session  # noqa: E402
from database_loader import DatabaseLoader  # noqa: E402
from event_regex_presets import (  # noqa: E402
    DISCIPLINE_CHOICES,
    LEVEL_CHOICES,
    effective_event_regex,
)
from officials_competition_types import (  # noqa: E402
    competition_load_flags_from_officials_type_id,
)
from ijs_scrape_log import (  # noqa: E402
    configure as configure_scrape_logging,
    pop_warnings,
    print_batch_summary,
)


from ijs_results_urls import (  # noqa: E402
    is_fsm_results_url,
    results_url_for_storage,
)


def parse_mdy_or_iso(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_bool_cell(val: str | None) -> bool | None:
    if val is None:
        return None
    t = str(val).strip().lower()
    if not t:
        return None
    if t in ("1", "true", "t", "yes", "y"):
        return True
    if t in ("0", "false", "f", "no", "n"):
        return False
    return None


def _fmt_elapsed(seconds: float) -> str:
    s = max(0, int(seconds))
    if s < 60:
        return f"{s}s"
    m, rem = divmod(s, 60)
    if m < 60:
        return f"{m}m {rem}s"
    h, rem_m = divmod(m, 60)
    return f"{h}h {rem_m}m {rem}s"


def parse_type_id(val: str | None) -> int | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(str(val).strip())
    except ValueError:
        return None


def row_ok_for_load(row: dict[str, str]) -> bool:
    if str(row.get("http_status", "")).strip() != "200":
        return False
    if str(row.get("probe_error", "")).strip():
        return False
    if not str(row.get("competition_name", "")).strip():
        return False
    url = results_url_for_storage(str(row.get("url", "")))
    if not url:
        return False
    return True


def _resolved_season_year(row: dict[str, str], default: str) -> str:
    s = str(row.get("season_year") or "").strip()
    if s:
        return s
    d = str(default or "").strip()
    if d:
        return d
    return str(row.get("year", "") or "").strip()


def _resolved_oa_type_id(row: dict[str, str], cli_default: int | None) -> int | None:
    t = parse_type_id(row.get("officials_analysis_competition_type_id"))
    if t is None:
        t = parse_type_id(row.get("competition_type_id"))
    if t is not None:
        return t
    return cli_default


def _flags_for_row(
    row: dict[str, str], oa_id: int
) -> tuple[bool, bool, bool]:
    q0, n0, i0 = competition_load_flags_from_officials_type_id(oa_id)
    if i0:
        return False, False, True
    q_raw = row.get("qualifying")
    n_raw = row.get("nqs")
    q = parse_bool_cell(q_raw) if q_raw is not None and str(q_raw).strip() != "" else None
    n = parse_bool_cell(n_raw) if n_raw is not None and str(n_raw).strip() != "" else None
    if q is not None and n is not None:
        return q, n, False
    return (
        q if q is not None else q0,
        n if n is not None else n0,
        False,
    )


def _parse_csv_choices(arg: str, choices: tuple[str, ...], label: str) -> tuple[str, ...]:
    if not (arg or "").strip():
        return ()
    parts = [x.strip() for x in arg.split(",") if x.strip()]
    bad = [p for p in parts if p not in choices]
    if bad:
        raise SystemExit(
            f"Invalid {label} value(s): {bad!r}; allowed: {list(choices)}"
        )
    return tuple(parts)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("csv_path", help="Path to discovered IJS competitions CSV")
    p.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only upsert competition shell rows (no scrape).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions only; do not scrape or write.",
    )
    p.add_argument(
        "--officials-analysis-competition-type-id",
        type=int,
        default=None,
        metavar="ID",
        help=(
            "Default ``officials_analysis.competition_type`` id for each row "
            "(required for full scrape unless every row has its own id column)."
        ),
    )
    p.add_argument(
        "--season-year",
        type=str,
        default="2526",
        metavar="CODE",
        help="Season year stored on ``competition.year`` (e.g. 2526). CSV ``season_year`` overrides per row.",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.0,
        metavar="SEC",
        help="Seconds to wait after each full scrape (0 = no delay).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N eligible rows (after --start-offset).",
    )
    p.add_argument(
        "--start-offset",
        type=int,
        default=0,
        metavar="N",
        help="Skip the first N eligible rows.",
    )
    p.add_argument(
        "--pdf-folder",
        type=str,
        default="",
        metavar="PATH",
        help="Passed to ``scrape`` when non-empty (otherwise scrape default).",
    )
    p.add_argument(
        "--event-regex-custom",
        type=str,
        default="",
        help="If set, used as event_regex for every scrape (replaces level/discipline presets).",
    )
    p.add_argument(
        "--event-levels",
        type=str,
        default="",
        help=f"Comma-separated subset of {list(LEVEL_CHOICES)}; ignored if --event-regex-custom set.",
    )
    p.add_argument(
        "--event-disciplines",
        type=str,
        default="",
        help=f"Comma-separated subset of {list(DISCIPLINE_CHOICES)}; ignored if --event-regex-custom set.",
    )
    p.add_argument(
        "--judge-filter",
        type=str,
        default="",
        help="Optional judge filter string (same as Load Competition).",
    )
    p.add_argument(
        "--specific-exclude",
        type=str,
        default="",
        help="Exclude events matching this string.",
    )
    p.add_argument(
        "--only-rule-errors",
        action="store_true",
        help="Forward to scrape(only_rule_errors=True).",
    )
    p.add_argument(
        "--default-qualifying",
        type=parse_bool_cell,
        default=None,
        metavar="BOOL",
        help="Metadata-only: default qualifying when CSV omits it.",
    )
    p.add_argument(
        "--default-nqs",
        type=parse_bool_cell,
        default=None,
        metavar="BOOL",
        help="Metadata-only: default nqs when CSV omits it.",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Console logging WARNING and above only (per-segment detail at DEBUG).",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Console DEBUG logging (very noisy).",
    )
    p.add_argument(
        "--log-file",
        type=str,
        default="",
        metavar="PATH",
        help="Also write DEBUG logs to this file.",
    )
    args = p.parse_args(argv)

    if args.quiet and args.verbose:
        print("Use only one of --quiet and --verbose.", file=sys.stderr)
        return 2

    try:
        level_tup = _parse_csv_choices(args.event_levels, LEVEL_CHOICES, "event level")
        disc_tup = _parse_csv_choices(
            args.event_disciplines, DISCIPLINE_CHOICES, "event discipline"
        )
    except SystemExit as e:
        return int(e.code) if isinstance(e.code, int) else 2

    event_regex = effective_event_regex(
        args.event_regex_custom, level_tup, disc_tup
    )

    path = args.csv_path
    if not os.path.isfile(path):
        print(f"Not a file: {path}", file=sys.stderr)
        return 2

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    eligible = [r for r in rows if row_ok_for_load(r)]
    if args.start_offset:
        eligible = eligible[args.start_offset :]
    if args.limit is not None:
        eligible = eligible[: max(0, args.limit)]

    print(f"Rows in file: {len(rows)}; eligible to process: {len(eligible)}")

    if not args.metadata_only:
        missing_type = [
            i
            for i, r in enumerate(eligible)
            if _resolved_oa_type_id(r, args.officials_analysis_competition_type_id)
            is None
        ]
        if missing_type:
            print(
                "Full scrape requires ``officials_analysis_competition_type_id`` for each row "
                "(CLI --officials-analysis-competition-type-id and/or CSV column). "
                f"Missing on {len(missing_type)} row(s).",
                file=sys.stderr,
            )
            return 2

    if args.dry_run:
        for r in eligible:
            base = results_url_for_storage(r["url"])
            name = str(r.get("competition_name", "")).strip()[:60]
            sy = _resolved_season_year(r, args.season_year)
            if args.metadata_only:
                print(f"metadata: {base} | {name!r} | year={sy!r}")
            else:
                oid = _resolved_oa_type_id(r, args.officials_analysis_competition_type_id)
                print(
                    f"scrape: {base} | {name!r} | season_year={sy!r} | oa_type_id={oid} | "
                    f"event_regex={event_regex!r}"
                )
        return 0

    if args.metadata_only:
        session = get_db_session()
        loader = DatabaseLoader(session)
        loaded = 0
        errors: list[str] = []

        for r in eligible:
            base_url = results_url_for_storage(r["url"])
            sy = _resolved_season_year(r, args.season_year)
            name = str(r.get("competition_name", "")).strip()
            location = str(r.get("location", "")).strip() or None
            start_d = parse_mdy_or_iso(str(r.get("start_date", "")))
            end_d = parse_mdy_or_iso(str(r.get("end_date", "")))

            type_id = _resolved_oa_type_id(r, args.officials_analysis_competition_type_id)
            qualifying, nqs, international = competition_load_flags_from_officials_type_id(
                type_id
            )
            q_col = r.get("qualifying")
            n_col = r.get("nqs")
            if q_col is not None and str(q_col).strip() != "":
                qualifying = parse_bool_cell(q_col)
            if n_col is not None and str(n_col).strip() != "":
                nqs = parse_bool_cell(n_col)

            try:
                loader.insert_competition(
                    name,
                    base_url,
                    sy,
                    qualifying=qualifying,
                    nqs=nqs,
                    international=international,
                    officials_analysis_competition_type_id=type_id,
                )
                loader.updateCompetition(
                    base_url,
                    location=location,
                    start_date=start_d,
                    end_date=end_d,
                    name=name,
                    qualifying=qualifying,
                    nqs=nqs,
                    international=international,
                    officials_analysis_competition_type_id=type_id,
                    update_officials_competition_type=type_id is not None,
                )
                loaded += 1
            except Exception as ex:  # noqa: BLE001
                errors.append(f"{base_url}: {ex}")

        print(f"Upserted {loaded} competition metadata row(s).")
        if errors:
            print(f"{len(errors)} error(s):", file=sys.stderr)
            for e in errors[:20]:
                print(f"  {e}", file=sys.stderr)
            return 1
        return 0

    import downloadResults as download_results  # noqa: E402

    configure_scrape_logging(
        quiet=args.quiet,
        verbose=args.verbose,
        log_file=args.log_file.strip() or None,
    )

    http_session = download_results._scrape_http_session()
    db_session = get_db_session()
    database_loader = DatabaseLoader(db_session, defer_commits=True)

    ok = 0
    errors: list[tuple[str, str]] = []
    warn_by_url: dict[str, list[str]] = {}
    try:
        for i, r in enumerate(eligible, start=1):
            raw_url = str(r.get("url", "")).strip()
            stored_url = results_url_for_storage(raw_url)
            name = str(r.get("competition_name", "")).strip()
            sy = _resolved_season_year(r, args.season_year)
            oa_id = _resolved_oa_type_id(r, args.officials_analysis_competition_type_id)
            assert oa_id is not None
            qualifying, nqs, international = _flags_for_row(r, oa_id)

            isFSM = is_fsm_results_url(stored_url)

            competition_metadata = {
                "start_date": parse_mdy_or_iso(str(r.get("start_date", ""))),
                "end_date": parse_mdy_or_iso(str(r.get("end_date", ""))),
                "location": str(r.get("location", "")).strip() or None,
            }

            scrape_kw: dict = dict(
                base_url=stored_url,
                report_name=name,
                event_regex=event_regex,
                only_rule_errors=args.only_rule_errors,
                use_gcp=False,
                write_excel=False,
                write_to_database=True,
                year=sy,
                judge_filter=args.judge_filter.strip(),
                specific_exclude=args.specific_exclude.strip(),
                use_html=True,
                isFSM=isFSM,
                qualifying=qualifying,
                nqs=nqs,
                international=international,
                officials_analysis_competition_type_id=oa_id,
                update_officials_competition_type=True,
                http_session=http_session,
                database_loader=database_loader,
                competition_metadata=competition_metadata,
                commit_per_segment=False,
                quiet=args.quiet,
                verbose=args.verbose,
                configure_logging=False,
            )
            if args.pdf_folder.strip():
                scrape_kw["pdf_folder"] = args.pdf_folder.strip()

            started_at = time.time()
            if args.quiet:
                print(
                    f"[{i}/{len(eligible)}] {datetime.now():%H:%M:%S} start "
                    f"{base_url} | {name[:70]!r}",
                    file=sys.stderr,
                    flush=True,
                )
            else:
                print(
                    f"[{i}/{len(eligible)}] scrape {base_url} | {name[:70]!r}",
                    file=sys.stderr,
                    flush=True,
                )
            try:
                download_results.scrape(**scrape_kw)
                ok += 1
                w = pop_warnings()
                if w:
                    warn_by_url[base_url] = w
                if args.quiet:
                    print(
                        f"[{i}/{len(eligible)}] {datetime.now():%H:%M:%S} done "
                        f"({_fmt_elapsed(time.time() - started_at)}) {base_url}",
                        file=sys.stderr,
                        flush=True,
                    )
            except Exception as ex:  # noqa: BLE001
                errors.append((base_url, str(ex)))
                pop_warnings()
                if args.quiet:
                    print(
                        f"[{i}/{len(eligible)}] {datetime.now():%H:%M:%S} failed "
                        f"({_fmt_elapsed(time.time() - started_at)}) {base_url}: {ex}",
                        file=sys.stderr,
                        flush=True,
                    )
            if args.delay > 0:
                time.sleep(args.delay)
    finally:
        db_session.close()

    print_batch_summary(ok=ok, failed=errors, warn_by_url=warn_by_url)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
