"""
Logging helpers for IJS ``scrape()`` and batch CSV loads.

Console defaults to INFO; batch ``--quiet`` uses WARNING. Optional log file captures DEBUG.
Warnings are collected per scrape for end-of-run summaries.
"""

from __future__ import annotations

import logging
import sys
from collections import defaultdict
from contextvars import ContextVar
from typing import TextIO

_LOG = logging.getLogger("ijs.scrape")
_PARSING_LOG = logging.getLogger("ijs.parsing")

_warnings_var: ContextVar[list[str] | None] = ContextVar(
    "ijs_scrape_warnings", default=None
)

# Per-event parse noise (missing judge cells, etc.) rolled up before emitting.
_parsing_issues_var: ContextVar[dict[str, dict[str, list[tuple[str, int]]]] | None] = (
    ContextVar("ijs_parsing_issues", default=None)
)

_ISSUE_LABELS = {
    "missing_element_score": "missing element judge scores",
    "missing_pcs_score": "missing PCS judge scores",
    "missing_pcs_columns": "skaters with fewer than 3 PCS components",
}

_configured = False
_file_handler: logging.FileHandler | None = None


def reset_warnings() -> list[str]:
    """Start a fresh warning list for one ``scrape()`` call."""
    _warnings_var.set([])
    _parsing_issues_var.set(defaultdict(dict))
    return _warnings_var.get()  # type: ignore[return-value]


def _parsing_issues_bucket() -> dict[str, dict[str, list[tuple[str, int]]]]:
    bucket = _parsing_issues_var.get()
    if bucket is None:
        bucket = defaultdict(dict)
        _parsing_issues_var.set(bucket)
    return bucket


def record_parsing_issue(
    category: str,
    event_name: str,
    *,
    skater: str = "",
    judge_number: int = 0,
) -> None:
    """Count a repetitive parse issue; summarized in ``flush_parsing_issues``."""
    if not event_name:
        event_name = "(unknown event)"
    bucket = _parsing_issues_bucket()
    issues = bucket[event_name]
    if category not in issues:
        issues[category] = []
    issues[category].append((skater or "", int(judge_number or 0)))


def flush_parsing_issues(event_name: str) -> None:
    """Emit one WARNING per issue category for this segment (detail at DEBUG)."""
    if not event_name:
        return
    bucket = _parsing_issues_bucket()
    categories = bucket.pop(event_name, None)
    if not categories:
        return
    for category, entries in categories.items():
        label = _ISSUE_LABELS.get(category, category.replace("_", " "))
        n = len(entries)
        skaters = sorted({s for s, _ in entries if s})
        judges = {j for _, j in entries if j}
        msg = f"{event_name}: {n} {label}"
        if skaters:
            sample = skaters[:5]
            msg += f" ({len(skaters)} skater(s): {', '.join(sample)}"
            if len(skaters) > 5:
                msg += f", +{len(skaters) - 5} more"
            msg += ")"
        if judges:
            msg += f"; {len(judges)} judge column(s) affected"
        note_warning(msg)
        if _PARSING_LOG.isEnabledFor(logging.DEBUG):
            for skater, judge_num in entries[:80]:
                if judge_num:
                    _PARSING_LOG.debug(
                        "  %s: skater=%r judge=%s", category, skater, judge_num
                    )
                else:
                    _PARSING_LOG.debug("  %s: skater=%r", category, skater)
            if len(entries) > 80:
                _PARSING_LOG.debug(
                    "  … %d more %s entries", len(entries) - 80, category
                )


def note_warning(message: str) -> None:
    """Record a warning and emit at WARNING level."""
    msg = (message or "").strip()
    if not msg:
        return
    bucket = _warnings_var.get()
    if bucket is not None:
        bucket.append(msg)
    _LOG.warning(msg)


def pop_warnings() -> list[str]:
    """Return and clear warnings for the current scrape."""
    issues = _parsing_issues_var.get()
    if issues:
        for event_name in list(issues.keys()):
            flush_parsing_issues(event_name)
    bucket = _warnings_var.get()
    if not bucket:
        return []
    out = list(bucket)
    bucket.clear()
    return out


def configure(
    *,
    quiet: bool = False,
    verbose: bool = False,
    log_file: str | None = None,
) -> None:
    """Configure ``ijs.scrape`` / ``ijs.parsing`` loggers (idempotent per process)."""
    global _configured, _file_handler

    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.WARNING
    else:
        level = logging.INFO

    for name in ("ijs.scrape", "ijs.parsing"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(level)
        logger.propagate = False

        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(level)
        sh.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(sh)

    if _file_handler is not None:
        for name in ("ijs.scrape", "ijs.parsing"):
            logging.getLogger(name).removeHandler(_file_handler)
        _file_handler.close()
        _file_handler = None

    if log_file:
        _file_handler = logging.FileHandler(log_file, encoding="utf-8")
        _file_handler.setLevel(logging.DEBUG)
        _file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        )
        for name in ("ijs.scrape", "ijs.parsing"):
            logging.getLogger(name).addHandler(_file_handler)

    _configured = True


def log_competition_summary(
    base_url: str,
    *,
    segments_written: int = 0,
    segments_skipped: int = 0,
    warnings: list[str] | None = None,
) -> None:
    """One INFO line per competition (visible unless ``--quiet``)."""
    parts = [f"{base_url}"]
    if segments_written or segments_skipped:
        parts.append(
            f"segments written={segments_written} skipped={segments_skipped}"
        )
    w = warnings or []
    if w:
        parts.append(f"warnings={len(w)}")
    _LOG.info("Finished: %s", " | ".join(parts))


def print_batch_summary(
    *,
    ok: int,
    failed: list[tuple[str, str]],
    warn_by_url: dict[str, list[str]],
    stream: TextIO | None = None,
) -> None:
    """End-of-run summary for ``load_discovered_ijs_competitions_csv.py``."""
    out = stream or sys.stderr
    total_warn = sum(len(v) for v in warn_by_url.values())
    print(
        f"\nBatch load finished: {ok} competition(s) ok, "
        f"{len(failed)} failed, {total_warn} warning(s) across "
        f"{len(warn_by_url)} competition(s).",
        file=out,
    )
    for url, err in failed[:25]:
        print(f"  FAIL {url}: {err}", file=out)
    if len(failed) > 25:
        print(f"  … and {len(failed) - 25} more failure(s)", file=out)
    shown = 0
    for url, warns in warn_by_url.items():
        if not warns:
            continue
        if shown >= 20:
            print(
                f"  … warnings on {len(warn_by_url) - shown} more competition(s)",
                file=out,
            )
            break
        print(f"  WARN {url} ({len(warns)}):", file=out)
        for w in warns[:3]:
            print(f"    {w}", file=out)
        if len(warns) > 3:
            print(f"    … +{len(warns) - 3} more", file=out)
        shown += 1
