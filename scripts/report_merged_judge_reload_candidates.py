#!/usr/bin/env python3
"""
After duplicate judge merges, list US-directory officials whose scores were split
across ids and competitions that may need rule-error backfill.

Uses segment_official panel names (duplicate spelling) and current score rows
(canonical judge id) to find affected competitions.

  python scripts/report_merged_judge_reload_candidates.py
  python scripts/report_merged_judge_reload_candidates.py --csv analysisTemp/merged_judge_reload_candidates.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text

from database import ensure_database_for_streamlit, get_database_url, get_db_session
from database_loader import judge_person_match_key
from officials_competition_types import OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL

_SNAPSHOT_PATH = _ROOT / "analysisTemp" / "merged_judge_groups_snapshot.json"
_SINGLES_PAIRS = (1, 2)
_RULE_ERRORS_FROM = "2018-07-01"


@dataclass(frozen=True)
class MergeMember:
    judge_id: int
    name: str
    element_scores: int
    pcs_scores: int
    is_canonical: bool

    @property
    def total_scores(self) -> int:
        return self.element_scores + self.pcs_scores


@dataclass(frozen=True)
class MergeGroup:
    match_key: str
    canonical_id: int
    members: tuple[MergeMember, ...]

    @property
    def canonical(self) -> MergeMember:
        for m in self.members:
            if m.is_canonical:
                return m
        raise ValueError(f"no canonical in {self.match_key!r}")

    @property
    def duplicates_with_scores(self) -> list[MergeMember]:
        return [
            m
            for m in self.members
            if not m.is_canonical and m.total_scores > 0
        ]

    @property
    def duplicate_element_scores(self) -> int:
        return sum(m.element_scores for m in self.members if not m.is_canonical)


def _parse_report_line_groups(report_text: str) -> list[MergeGroup]:
    """Parse ``report_duplicate_judges.py``-style text blocks."""
    groups: list[MergeGroup] = []
    current_key: str | None = None
    current_canonical: int | None = None
    members: list[MergeMember] = []
    header_re = re.compile(
        r"^match_key='([^']+)'\s+canonical_id=(\d+)\s*$"
    )
    member_re = re.compile(
        r"^\s+\[(\d+)\]\s+'([^']*)'\s+\((canonical|duplicate);\s+"
        r"element=(\d+),\s+pcs=(\d+),\s+total=(\d+)\)\s*$"
    )
    for raw in report_text.splitlines():
        line = raw.rstrip()
        hm = header_re.match(line)
        if hm:
            if current_key is not None:
                groups.append(
                    MergeGroup(current_key, int(current_canonical), tuple(members))
                )
            current_key = hm.group(1)
            current_canonical = int(hm.group(2))
            members = []
            continue
        mm = member_re.match(line)
        if mm and current_key is not None:
            members.append(
                MergeMember(
                    judge_id=int(mm.group(1)),
                    name=mm.group(2),
                    element_scores=int(mm.group(4)),
                    pcs_scores=int(mm.group(5)),
                    is_canonical=mm.group(3) == "canonical",
                )
            )
    if current_key is not None:
        groups.append(
            MergeGroup(current_key, int(current_canonical), tuple(members))
        )
    return groups


def load_merge_groups() -> list[MergeGroup]:
    if _SNAPSHOT_PATH.is_file():
        raw = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
        out: list[MergeGroup] = []
        for g in raw:
            members = tuple(
                MergeMember(
                    judge_id=int(m["judge_id"]),
                    name=str(m["name"]),
                    element_scores=int(m["element_scores"]),
                    pcs_scores=int(m["pcs_scores"]),
                    is_canonical=bool(m["is_canonical"]),
                )
                for m in g["members"]
            )
            out.append(
                MergeGroup(
                    str(g["match_key"]),
                    int(g["canonical_id"]),
                    members,
                )
            )
        return out
    raise FileNotFoundError(
        f"Missing {_SNAPSHOT_PATH}; save pre-merge report_duplicate_judges output there."
    )


def _us_official_info(session, canonical_ids: list[int]) -> dict[int, dict]:
    if not canonical_ids:
        return {}
    rows = session.execute(
        text(
            """
            SELECT jol.judge_id, jol.status, o.id AS official_id, o.full_name
            FROM judge_official_link jol
            LEFT JOIN officials_analysis.officials o ON o.id = jol.official_id
            WHERE jol.judge_id IN :ids
            """
        ).bindparams(__import__("sqlalchemy").bindparam("ids", expanding=True)),
        {"ids": canonical_ids},
    ).all()
    return {
        int(r.judge_id): {
            "status": str(r.status),
            "official_id": int(r.official_id) if r.official_id is not None else None,
            "directory_name": str(r.full_name) if r.full_name else None,
        }
        for r in rows
    }


def _competitions_for_reload(
    session,
    *,
    match_key: str,
    canonical_id: int,
    duplicate_names: list[str],
) -> list[dict]:
    from sqlalchemy import bindparam

    intl = sorted(OFFICIALS_COMPETITION_TYPE_IDS_INTERNATIONAL) or [-1]
    rows = session.execute(
        text(
            """
            WITH panel_comps AS (
                SELECT DISTINCT c.id AS competition_id
                FROM segment_official so
                JOIN segment s ON s.id = so.segment_id
                JOIN competition c ON c.id = s.competition_id
                WHERE s.discipline_type_id IN :disc_ids
                  AND so.role ILIKE 'Judge%%'
                  AND lower(regexp_replace(trim(so.official_name), '\\s+', ' ', 'g')) = :match_key
                  AND (
                    c.start_date IS NULL
                    OR c.start_date >= CAST(:rule_from AS date)
                    OR c.end_date >= CAST(:rule_from AS date)
                  )
                  AND (
                    c.officials_analysis_competition_type_id IS NULL
                    OR c.officials_analysis_competition_type_id NOT IN :intl_types
                  )
            ),
            score_comps AS (
                SELECT DISTINCT c.id AS competition_id
                FROM element_score_per_judge espj
                JOIN element e ON e.id = espj.element_id
                JOIN skater_segment ss ON ss.id = e.skater_segment_id
                JOIN segment s ON s.id = ss.segment_id
                JOIN competition c ON c.id = s.competition_id
                WHERE espj.judge_id = :canonical_id
                  AND s.discipline_type_id IN :disc_ids
                  AND (
                    c.start_date IS NULL
                    OR c.start_date >= CAST(:rule_from AS date)
                    OR c.end_date >= CAST(:rule_from AS date)
                  )
                  AND (
                    c.officials_analysis_competition_type_id IS NULL
                    OR c.officials_analysis_competition_type_id NOT IN :intl_types
                  )
            ),
            unioned AS (
                SELECT competition_id FROM panel_comps
                UNION
                SELECT competition_id FROM score_comps
            )
            SELECT
                c.id AS competition_id,
                c.name AS competition_name,
                c.year,
                c.results_url,
                c.start_date,
                COALESCE(re.rule_errors, 0) AS current_rule_errors,
                COALESCE(seg.segment_count, 0) AS singles_pairs_segments
            FROM unioned u
            JOIN competition c ON c.id = u.competition_id
            LEFT JOIN (
                SELECT
                    s3.competition_id,
                    COUNT(DISTINCT s3.id) AS segment_count
                FROM segment s3
                JOIN skater_segment ss3 ON ss3.segment_id = s3.id
                JOIN element e3 ON e3.skater_segment_id = ss3.id
                JOIN element_score_per_judge espj3
                    ON espj3.element_id = e3.id AND espj3.judge_id = :canonical_id
                WHERE s3.discipline_type_id IN :disc_ids
                GROUP BY s3.competition_id
            ) seg ON seg.competition_id = c.id
            LEFT JOIN (
                SELECT
                    s4.competition_id,
                    SUM(CASE WHEN espj4.is_rule_error THEN 1 ELSE 0 END) AS rule_errors
                FROM segment s4
                JOIN skater_segment ss4 ON ss4.segment_id = s4.id
                JOIN element e4 ON e4.skater_segment_id = ss4.id
                JOIN element_score_per_judge espj4
                    ON espj4.element_id = e4.id AND espj4.judge_id = :canonical_id
                WHERE s4.discipline_type_id IN :disc_ids
                GROUP BY s4.competition_id
            ) re ON re.competition_id = c.id
            ORDER BY c.start_date NULLS LAST, c.id
            """
        ).bindparams(
            bindparam("disc_ids", expanding=True),
            bindparam("intl_types", expanding=True),
        ),
        {
            "match_key": match_key,
            "canonical_id": canonical_id,
            "disc_ids": list(_SINGLES_PAIRS),
            "rule_from": _RULE_ERRORS_FROM,
            "intl_types": intl,
        },
    ).all()

    dup_name_set = {n.strip() for n in duplicate_names if n.strip()}
    dup_panel_rows = []
    if dup_name_set:
        dup_panel_rows = session.execute(
            text(
                """
                SELECT DISTINCT c.id, so.official_name
                FROM segment_official so
                JOIN segment s ON s.id = so.segment_id
                JOIN competition c ON c.id = s.competition_id
                WHERE so.role ILIKE 'Judge%%'
                  AND s.discipline_type_id IN :disc_ids
                  AND so.official_name = ANY(:dup_names)
                """
            ).bindparams(
                __import__("sqlalchemy").bindparam("disc_ids", expanding=True),
            ),
            {"disc_ids": list(_SINGLES_PAIRS), "dup_names": list(dup_name_set)},
        ).all()
    dup_panel_by_comp = {}
    for comp_id, off_name in dup_panel_rows:
        dup_panel_by_comp.setdefault(int(comp_id), []).append(str(off_name))

    out = []
    for r in rows:
        comp_id = int(r.competition_id)
        out.append({
            "competition_id": comp_id,
            "competition_name": str(r.competition_name),
            "year": str(r.year),
            "results_url": str(r.results_url or ""),
            "start_date": str(r.start_date) if r.start_date else "",
            "duplicate_panel_spelling": "; ".join(sorted(set(dup_panel_by_comp.get(comp_id, [])))),
            "likely_duplicate_id_load": comp_id in dup_panel_by_comp,
            "current_rule_errors": int(r.current_rule_errors or 0),
            "singles_pairs_segments": int(r.singles_pairs_segments or 0),
        })
    return out


def build_reload_report(groups: list[MergeGroup], session) -> tuple[list[dict], list[dict]]:
    """Return (official_rows, competition_rows)."""
    candidates = [
        g for g in groups
        if g.duplicates_with_scores and g.duplicate_element_scores > 0
    ]
    canonical_ids = [g.canonical_id for g in candidates]
    us_info = _us_official_info(session, canonical_ids)

    official_rows: list[dict] = []
    competition_rows: list[dict] = []

    for g in candidates:
        info = us_info.get(g.canonical_id)
        if not info or info.get("status") != "linked" or not info.get("official_id"):
            continue
        canon = g.canonical
        dups = g.duplicates_with_scores
        dup_names = [d.name for d in g.members if not d.is_canonical]
        comps = _competitions_for_reload(
            session,
            match_key=g.match_key,
            canonical_id=g.canonical_id,
            duplicate_names=dup_names,
        )
        official_rows.append({
            "match_key": g.match_key,
            "canonical_judge_id": g.canonical_id,
            "canonical_name": canon.name,
            "directory_name": info.get("directory_name"),
            "official_id": info.get("official_id"),
            "duplicate_judge_ids": "; ".join(str(d.judge_id) for d in dups),
            "duplicate_names": "; ".join(d.name for d in dups),
            "duplicate_element_scores": g.duplicate_element_scores,
            "competitions_to_recheck": len(comps),
            "competitions_duplicate_spelling": sum(
                1 for c in comps if c["likely_duplicate_id_load"]
            ),
        })
        for c in comps:
            competition_rows.append({
                "match_key": g.match_key,
                "canonical_judge_id": g.canonical_id,
                "canonical_name": canon.name,
                "directory_name": info.get("directory_name"),
                "duplicate_judge_ids": "; ".join(str(d.judge_id) for d in dups),
                **c,
            })
    return official_rows, competition_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="", help="Write competition rows to CSV.")
    parser.add_argument(
        "--officials-csv",
        default="",
        help="Write one row per US official to CSV.",
    )
    args = parser.parse_args()

    ensure_database_for_streamlit()
    print(f"Database: {get_database_url().split('@')[-1].split('/')[0]}", flush=True)
    groups = load_merge_groups()
    session = get_db_session()
    official_rows, competition_rows = build_reload_report(groups, session)

    print(f"\nUS officials with split duplicate scores: {len(official_rows)}", flush=True)
    for row in official_rows:
        print(
            f"  {row['directory_name']!r} (judge {row['canonical_judge_id']}, "
            f"dup ids {row['duplicate_judge_ids']}, "
            f"{row['duplicate_element_scores']} element scores on duplicate id, "
            f"{row['competitions_to_recheck']} competition(s) to recheck, "
            f"{row['competitions_duplicate_spelling']} with duplicate panel spelling)",
            flush=True,
        )

    print(f"\nCompetition reload candidates: {len(competition_rows)}", flush=True)
    by_official: dict[str, list[dict]] = {}
    for row in competition_rows:
        by_official.setdefault(row["directory_name"] or row["canonical_name"], []).append(row)
    for name in sorted(by_official, key=str.lower):
        print(f"\n## {name}", flush=True)
        for c in by_official[name]:
            flag = " [dup spelling on panel]" if c["likely_duplicate_id_load"] else ""
            print(
                f"  - {c['competition_id']}: {c['competition_name']} ({c['year']}) "
                f"rule_errors={c['current_rule_errors']}{flag}",
                flush=True,
            )
            if c["results_url"]:
                print(f"    {c['results_url']}", flush=True)

    if args.csv:
        path = Path(args.csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as fh:
            if competition_rows:
                writer = csv.DictWriter(fh, fieldnames=list(competition_rows[0].keys()))
                writer.writeheader()
                writer.writerows(competition_rows)
        print(f"\nWrote {len(competition_rows)} competition row(s) to {path}", flush=True)

    if args.officials_csv:
        path = Path(args.officials_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as fh:
            if official_rows:
                writer = csv.DictWriter(fh, fieldnames=list(official_rows[0].keys()))
                writer.writeheader()
                writer.writerows(official_rows)
        print(f"Wrote {len(official_rows)} official row(s) to {path}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
