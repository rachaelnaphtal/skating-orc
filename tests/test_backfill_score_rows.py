import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "backfill_element_rule_errors",
    _ROOT / "scripts" / "backfill_element_rule_errors.py",
)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)


def test_merge_score_rows_prefers_html_over_pdf():
    html_row = {
        "cover_label": "Intermediate Women Grp C - Short Program",
        "scores_url": "https://example.test/SEGM001.html",
        "parse_mode": "html",
        "judges": ["Judge A"],
    }
    pdf_row = {
        "cover_label": "Intermediate Women Grp C - Short Program",
        "scores_url": "https://example.test/scores.pdf",
        "parse_mode": "fsm_pdf",
    }
    merged = _mod._merge_score_rows_prefer_html([html_row], [pdf_row])
    assert len(merged) == 1
    assert merged[0]["parse_mode"] == "html"
    assert merged[0]["scores_url"].endswith(".html")


def test_merge_score_rows_keeps_pdf_when_html_missing():
    pdf_row = {
        "cover_label": "Men - Short Program",
        "scores_url": "https://example.test/scores.pdf",
        "parse_mode": "fsm_pdf",
    }
    merged = _mod._merge_score_rows_prefer_html([], [pdf_row])
    assert len(merged) == 1
    assert merged[0]["parse_mode"] == "fsm_pdf"
