"""
Classify ISU figure skating competitions (``public.competition.name``) into major
event types for the international officials activity matrix.

Competition type ids 15–16 do not distinguish Worlds, Europeans, Four Continents,
Junior Worlds, Olympics, or Grand Prix Final — name-based classification fills that gap.
"""

from __future__ import annotations

import re

MAJOR_ISU_EVENT_WORLDS = "worlds"
MAJOR_ISU_EVENT_OLYMPICS = "olympics"
MAJOR_ISU_EVENT_JUNIOR_WORLDS = "junior_worlds"
MAJOR_ISU_EVENT_FOUR_CONTINENTS = "four_continents"
MAJOR_ISU_EVENT_EUROPEANS = "europeans"
MAJOR_ISU_EVENT_GRAND_PRIX_FINAL = "grand_prix_final"

MAJOR_ISU_EVENT_LABELS: dict[str, str] = {
    MAJOR_ISU_EVENT_WORLDS: "World Championships",
    MAJOR_ISU_EVENT_OLYMPICS: "Olympic Winter Games",
    MAJOR_ISU_EVENT_JUNIOR_WORLDS: "World Junior Championships",
    MAJOR_ISU_EVENT_FOUR_CONTINENTS: "Four Continents Championships",
    MAJOR_ISU_EVENT_EUROPEANS: "European Championships",
    MAJOR_ISU_EVENT_GRAND_PRIX_FINAL: "Grand Prix Final",
}

MAJOR_ISU_EVENT_KEYS: tuple[str, ...] = tuple(MAJOR_ISU_EVENT_LABELS.keys())

_GP_FINAL = re.compile(r"\b(?:grand\s+prix\s+final|gp\s+final)\b", re.I)
_OLYMPIC_WINTER_GAMES = re.compile(r"\bolympic\s+winter\s+games\b|\bowg\b", re.I)
_SYNCHRO = re.compile(r"\bsynchro(?:nized)?\b", re.I)
_JUNIOR_WORLDS = re.compile(
    r"\b(?:world\s+junior|junior\s+world)\b(?:\s+figure\s+skating)?\s+championships?\b",
    re.I,
)
_TITLE_YEAR = re.compile(r"\b(19|20)\d{2}\b")

# Normalized substrings from ISU results / directory competition titles.
_JUNIOR_WORLDS_PHRASES: tuple[str, ...] = (
    "world junior figure skating championships",
    "world junior championships",
    "junior world championships",
    "isu world junior figure skating championships",
    "isu figure skating junior world championships",
)


def normalize_competition_name(value: str) -> str:
    return " ".join((value or "").lower().split())


def year_from_competition_name(competition_name: str) -> int | None:
    """Extract a 4-digit calendar year from the competition title, if present."""
    match = _TITLE_YEAR.search(competition_name or "")
    if not match:
        return None
    return int(match.group(0))


def _is_figure_skating_event(name: str) -> bool:
    """Exclude synchronized-only championships from SPD major-event buckets."""
    if _SYNCHRO.search(name):
        return False
    return True


def _is_junior_world_championship(name: str) -> bool:
    """World Junior Championships (SPD) — ``world junior`` or ``junior world`` title forms."""
    if _SYNCHRO.search(name):
        return False
    if any(phrase in name for phrase in _JUNIOR_WORLDS_PHRASES):
        return True
    if _JUNIOR_WORLDS.search(name):
        return True
    if "world championships" in name or "world championship" in name:
        return "junior" in name and ("junior world" in name or "world junior" in name)
    return False


def _is_olympic_winter_games(name: str) -> bool:
    """Olympic Winter Games (OWG) only — not EYOF, YOG, or other ``olympic`` festivals."""
    if any(
        phrase in name
        for phrase in (
            "youth olympic",
            "youth olympic festival",
            "european youth olympic festival",
            "eyof",
        )
    ):
        return False
    return bool(_OLYMPIC_WINTER_GAMES.search(name))


def classify_isu_major_event(competition_name: str) -> str | None:
    """
    Map a competition display name to a major ISU event key, or ``None``.

  Worlds, Olympics, Junior Worlds, Four Continents, Europeans, and Grand Prix Final.
    """
    name = normalize_competition_name(competition_name)
    if not name:
        return None

    if _GP_FINAL.search(name):
        return MAJOR_ISU_EVENT_GRAND_PRIX_FINAL

    if not _is_figure_skating_event(name):
        return None

    if _is_olympic_winter_games(name):
        return MAJOR_ISU_EVENT_OLYMPICS

    if "four continents" in name:
        return MAJOR_ISU_EVENT_FOUR_CONTINENTS

    if "european" in name and "championship" in name:
        return MAJOR_ISU_EVENT_EUROPEANS

    if _is_junior_world_championship(name):
        return MAJOR_ISU_EVENT_JUNIOR_WORLDS

    if "world" in name and "championship" in name and "junior" not in name:
        return MAJOR_ISU_EVENT_WORLDS

    if name in ("worlds",) or re.search(r"\bworlds\b", name):
        if "junior" not in name:
            return MAJOR_ISU_EVENT_WORLDS

    return None


def competition_matches_major_event(competition_name: str, event_key: str) -> bool:
    return classify_isu_major_event(competition_name) == event_key
