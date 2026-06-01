#!/usr/bin/env python3
"""
CSV helper: classify international segments from a pasted export.

Prefer ``segment_level.classify_segment_level`` in application code and
``scripts/backfill_segment_level.py`` for the database.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from segment_level import classify_segment_level

INPUT_DEFAULT = _ROOT / "analysisTemp/SegmentsWithCompsInternational.csv"
OUTPUT_DEFAULT = _ROOT / "analysisTemp/output_with_level.csv"


def main() -> None:
    input_file = Path(sys.argv[1]) if len(sys.argv) > 1 else INPUT_DEFAULT
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else OUTPUT_DEFAULT

    with open(input_file, encoding="utf-8") as fin, open(
        output_file, "w", encoding="utf-8", newline=""
    ) as fout:
        writer = csv.writer(fout)
        writer.writerow(
            ["segment_name", "discipline", "Competition", "Level", "level_source"]
        )
        for raw in fin:
            if raw.strip() == "" or raw.upper().startswith("SEGMENT_NAME"):
                continue
            parts = raw.rstrip("\n").split(",", 2)
            if len(parts) < 3:
                parts += [""] * (3 - len(parts))
            seg, disc, comp = parts
            result = classify_segment_level(seg, competition_name=comp, international=True)
            writer.writerow([seg, disc, comp, result.level, result.source])

    print("Done. Output saved to", output_file)


if __name__ == "__main__":
    main()
