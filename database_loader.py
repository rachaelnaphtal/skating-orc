from __future__ import annotations

import streamlit as st
from sqlalchemy.sql import text
import pandas as pd
from sqlalchemy.orm import Session
from models import Judge, Competition, Segment, Skater, SkaterSegment, Element, ElementScorePerJudge, PcsScorePerJudge, PcsType, ElementType, DisciplineType, SegmentOfficial
from database import get_db_session, test_connection

try:
    from judge_official_link_core import normalize_name, suggest_matches
except ImportError:  # pragma: no cover
    normalize_name = None  # type: ignore[misc, assignment]
    suggest_matches = None  # type: ignore[misc, assignment]


def _normalize_person_name(name: str | None) -> str:
    """Lowercase + collapse whitespace; matches judge_official_link_core.normalize_name."""
    if not name:
        return ""
    return " ".join(name.lower().split()).strip()

# Initialize connection.
# conn = st.connection("postgresql", type="sql")

# IJS / USFS results panel role → officials_analysis.appointment_types.id
# (values from directory; see user specification.)
APPOINTMENT_TYPE_ID_JUDGE = 1
APPOINTMENT_TYPE_ID_REFEREE = 4
APPOINTMENT_TYPE_ID_TECH_SPECIALIST = 9
APPOINTMENT_TYPE_ID_TECH_CONTROLLER = 11


def appointment_type_id_for_ijs_role(role_label: str) -> int | None:
    r = (role_label or "").strip()
    if r.startswith("Judge"):
        return APPOINTMENT_TYPE_ID_JUDGE
    if "Referee" in r:
        return APPOINTMENT_TYPE_ID_REFEREE
    if "Technical Controller" in r:
        return APPOINTMENT_TYPE_ID_TECH_CONTROLLER
    if "Assistant Technical Specialist" in r or "Technical Specialist" in r:
        return APPOINTMENT_TYPE_ID_TECH_SPECIALIST
    return None


class DatabaseLoader:
    def __init__(self, session: Session):
        self.session = session

    def replace_segment_officials(self, segment_id: int, rows: list) -> None:
        """
        Persist IJS segment officials. Replaces prior rows for this segment.
        ``official_id`` is set from (in order): ``judge_official_link`` for the judge
        row from ``insert_judge``; else ``public.official_name_alias`` on the
        normalized scraped name; else exact match on ``officials.full_name``;
        else high-confidence fuzzy match. ``appointment_type_id`` follows role labels.
        """
        if not rows:
            return
        self.session.query(SegmentOfficial).filter(
            SegmentOfficial.segment_id == segment_id
        ).delete(synchronize_session=False)
        choices_cache: dict[int, str] | None = None
        for r in rows:
            official_name = r["name"]
            role = r["role"]
            judge_id = self.insert_judge(official_name)
            appt_type_id = appointment_type_id_for_ijs_role(role)
            oid = self._official_id_from_judge_id(judge_id)
            if oid is None:
                oid = self._official_id_from_name_alias(official_name)
            if oid is None:
                oid = self._official_id_from_exact_directory_name(official_name)
            if oid is None:
                if choices_cache is None:
                    choices_cache = self._load_official_directory_choices()
                oid = self._official_id_from_fuzzy_directory_name(
                    official_name, choices_cache
                )
            self.session.add(
                SegmentOfficial(
                    segment_id=segment_id,
                    official_name=official_name,
                    official_id=oid,
                    role=role,
                    appointment_type_id=appt_type_id,
                )
            )
        self.session.commit()

    def _load_official_directory_choices(self) -> dict[int, str]:
        try:
            rows = self.session.execute(
                text("""
                    SELECT id, TRIM(full_name) AS full_name
                    FROM officials_analysis.officials
                    WHERE full_name IS NOT NULL AND TRIM(full_name) <> ''
                """)
            ).mappings().all()
        except Exception:
            return {}
        return {int(r["id"]): str(r["full_name"]) for r in rows}

    def _official_id_from_exact_directory_name(self, official_name: str) -> int | None:
        norm_fn = normalize_name or _normalize_person_name
        norm = norm_fn(official_name)
        if not norm:
            return None
        try:
            rows = self.session.execute(
                text("""
                    SELECT id FROM officials_analysis.officials
                    WHERE full_name IS NOT NULL AND TRIM(full_name) <> ''
                      AND lower(regexp_replace(trim(full_name), '\\s+', ' ', 'g')) = :norm
                    LIMIT 2
                """),
                {"norm": norm},
            ).fetchall()
        except Exception:
            return None
        if len(rows) != 1:
            return None
        return int(rows[0][0])

    def _official_id_from_fuzzy_directory_name(
        self, official_name: str, choices: dict[int, str]
    ) -> int | None:
        if not suggest_matches or not choices:
            return None
        matches = suggest_matches(official_name, choices, top=3, min_score=88)
        if not matches:
            return None
        best_id, best_score, _ = matches[0]
        if best_score < 92:
            return None
        if len(matches) > 1 and (best_score - matches[1][1]) < 3:
            return None
        return int(best_id)

    def _official_id_from_judge_id(self, judge_id: int) -> int | None:
        try:
            row = self.session.execute(
                text(
                    "SELECT official_id FROM judge_official_link "
                    "WHERE judge_id = :jid AND status = 'linked' LIMIT 1"
                ),
                {"jid": judge_id},
            ).first()
        except Exception:
            return None
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _official_id_from_name_alias(self, official_name: str) -> int | None:
        norm = _normalize_person_name(official_name)
        if not norm:
            return None
        try:
            row = self.session.execute(
                text(
                    "SELECT official_id FROM public.official_name_alias "
                    "WHERE alias_normalized = :n LIMIT 1"
                ),
                {"n": norm},
            ).first()
        except Exception:
            return None
        if not row or row[0] is None:
            return None
        return int(row[0])

    def getCompetitionUrlsWithNoLocation(self):
        competitions = (self.session.query(Competition).filter(Competition.location == None).all())
        return [competition.results_url for competition in competitions]
    
    def updateCompetition(self, url, location, start_date, end_date):
        existing = self.session.query(Competition).filter_by(results_url=url).first()
        existing.location = location
        existing.start_date = start_date
        existing.end_date = end_date
        self.session.commit()
    
    def getSegmentNamesForCompetition(self, url):
        segments = (self.session.query(Segment).join(Competition, Segment.competition_id == Competition.id).filter(Competition.results_url == url).all())
        return [segment.name for segment in segments]

    def insert_competition(self, name, url, year):
        existing = self.session.query(Competition).filter_by(results_url=url).first()
        if not existing:
            new = Competition(name=name, results_url=url, year=year)
            self.session.add(new)
            self.session.commit()
            return new.id
        return existing.id
        
    def insert_discipline_type(self, segment_name):
        type = "Uncategorized"
        if "women" in segment_name.lower() or "girl" in segment_name.lower() or "ladies" in segment_name.lower():
            type="Singles"
        elif "men" in segment_name.lower() or "boy" in segment_name.lower() or "excel" in segment_name.lower():
            type = "Singles"
        elif "solo" in segment_name.lower() or "shadow" in segment_name.lower():
            type = "Solo Dance"
        elif "pair" in segment_name.lower():
            type = "Pairs"
        elif "dance" in segment_name.lower():
            type = "Ice Dance"
        elif "showcase" in segment_name.lower():
            type = "Showcase"
        elif "team" in segment_name.lower():
            type="Synchronized"
        elif "choreographic excercise" in segment_name.lower():
            type="Theater On Ice"

        existing = self.session.query(DisciplineType).filter_by(name=type).first()
        if not existing:
            new = DisciplineType(name=type)
            self.session.add(new)
            self.session.commit()
            return new.id
        return existing.id

    def insert_segment(self, segment_name, competition_id):
        is_freeskate = "free" in segment_name.lower()
        discipline_type_id = self.insert_discipline_type(segment_name)
        existing = self.session.query(Segment).filter_by(name=segment_name, competition_id=competition_id).first()
        if not existing:
            new = Segment(name=segment_name, competition_id=competition_id, freeskate=is_freeskate, discipline_type_id=discipline_type_id)
            self.session.add(new)
            self.session.commit()
            return new.id
        existing.discipline_type_id = discipline_type_id
        existing.freeskate = is_freeskate
        self.session.commit()
        return existing.id


    def insert_judge(self, judge_name):
        existing_judge = self.session.query(Judge).filter_by(name=judge_name).first()
        if not existing_judge:
            new_judge = Judge(name=judge_name)
            self.session.add(new_judge)
            self.session.commit()
            return new_judge.id
        return existing_judge.id

    def insert_skater(self, skater_name):
        existing = self.session.query(Skater).filter_by(name=skater_name).first()
        if not existing:
            new = Skater(name=skater_name)
            self.session.add(new)
            self.session.commit()
            return new.id
        return existing.id


    def insert_skater_segment(self, segment_id, skater_id):
        existing = self.session.query(SkaterSegment).filter_by(segment_id=segment_id, skater_id=skater_id).first()
        if not existing:
            new = SkaterSegment(segment_id=segment_id, skater_id=skater_id)
            self.session.add(new)
            self.session.commit()
            return new.id
        return existing.id

    def insert_element_type(self, element_name):
        existing = self.session.query(ElementType).filter_by(name=element_name).first()
        if not existing:
            new = ElementType(name=element_name)
            self.session.add(new)
            self.session.commit()
            return new.id
        return existing.id
        
    def insert_pcs_type(self, pcs_name):
        existing = self.session.query(PcsType).filter_by(name=pcs_name).first()
        if not existing:
            new = PcsType(name=pcs_name)
            self.session.add(new)
            self.session.commit()
            return new.id
        return existing.id

    def insert_element(self,element_name, element_type, skater_segment_id):
        element_type_id= self.insert_element_type(element_type)
        existing = self.session.query(Element).filter_by(name=element_name, skater_segment_id=skater_segment_id).first()
        if not existing:
            new = Element(name=element_name, element_type_id = element_type_id, element_type=element_type, skater_segment_id=skater_segment_id)
            self.session.add(new)
            self.session.commit()
            return new.id
        else:
            existing.element_type_id = element_type_id
            existing.element_type = element_type
            self.session.commit()
        return existing.id

    def insert_element_score_per_judge(self, element_id, judge_id, score, panel_average, deviation, thrown_out):
        existing = self.session.query(ElementScorePerJudge).filter_by(element_id=element_id, judge_id=judge_id).first()
        if not existing:
            new = ElementScorePerJudge(
                judge_id=judge_id,
                element_id=element_id,
                judge_score=score,
                panel_average=panel_average,
                deviation=deviation,
                thrown_out=thrown_out
            )
            self.session.add(new)
            self.session.commit()
            return new.id
        return existing.id
        
    def insert_pcs_score_per_judge(self,skater_segment_id, pcs_type_id, judge_id, score, panel_average, deviation, thrown_out):
        existing = self.session.query(PcsScorePerJudge).filter_by(skater_segment_id=skater_segment_id, judge_id=judge_id, pcs_type_id=pcs_type_id).first()
        if not existing:
            new = PcsScorePerJudge(
                judge_id=judge_id,
                pcs_type_id = pcs_type_id,
                skater_segment_id = skater_segment_id,
                judge_score=score,
                panel_average=panel_average,
                deviation=deviation,
                thrown_out=thrown_out
            )
            self.session.add(new)
            self.session.commit()
            return new.id
        elif existing.judge_score!=score:
            raise NameError("Scores do not align")
        return existing.id

    def insert_element_scores(self, judgesNames, all_element_dict, segment_id, rule_errors):
        if len(all_element_dict)==0:
            return
        judge_dict = {judge: self.insert_judge(judge) for judge in judgesNames}
        all_element_df = pd.DataFrame.from_dict(all_element_dict)
        skater_dict = {skater: self.insert_skater(skater) for skater in all_element_df["Skater"].unique()}
        for row in all_element_df.itertuples():
            skater_id = skater_dict[row.Skater]
            skater_segment_id = self.insert_skater_segment(segment_id, skater_id)
            element_id = self.insert_element(row.Element, row._4, skater_segment_id)
            self.insert_element_score_per_judge(element_id, judge_dict[row._6], row.Score, row._5, row.Deviation, row._10)
        self.insert_rule_errors(rule_errors, segment_id)
        # self.session.commit()

    def insert_rule_errors(self,rule_errors, segment_id):
        for rule_error in rule_errors:
            skater_id = self.session.query(Skater).filter_by(name=rule_error["Skater"]).first().id
            skater_segment_id = self.session.query(SkaterSegment).filter_by(segment_id=segment_id, skater_id=skater_id).first().id
            # Rule errors add on the attention column
            converted_name = rule_error["Element"].split(" ")[0]
            element_id = self.session.query(Element).filter_by(name=converted_name, skater_segment_id=skater_segment_id).first().id
            judge_id = self.session.query(Judge).filter_by(name=rule_error["Judge Name"]).first().id
            score = self.session.query(ElementScorePerJudge).filter_by(element_id=element_id, judge_id=judge_id).first()
            score.is_rule_error = True
            self.session.commit()

    def insert_pcs_scores(self, judgesNames, all_pcs_dict, segment_id):
        judge_dict = {judge: self.insert_judge(judge) for judge in judgesNames}
        all_pcs_df = pd.DataFrame.from_dict(all_pcs_dict)
        if len(all_pcs_df)==0:
            return
        skater_dict = {skater: self.insert_skater(skater) for skater in all_pcs_df["Skater"].unique()}
        for row in all_pcs_df.itertuples():
            skater_id = skater_dict[row.Skater]
            skater_segment_id = self.insert_skater_segment(segment_id, skater_id)
            pcs_type_id = self.insert_pcs_type(row.Component)
            self.insert_pcs_score_per_judge(skater_segment_id, pcs_type_id, judge_dict[row._5], row.Score, row._4, row.Deviation, row._9)
        self.session.commit()

# session = get_db_session()
# database_obj = DatabaseLoader(session)
# print(database_obj.getSegmentNamesForCompetition("https://ijs.usfigureskating.org/leaderboard/results/2025/35645"))
# print(insert_competition("test", "https://ijs.usfigureskating.org/leaderboard/results/2024/33458",""))


