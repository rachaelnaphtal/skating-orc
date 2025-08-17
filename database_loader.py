import streamlit as st
from sqlalchemy.sql import text
import pandas as pd
from sqlalchemy.orm import Session
from models import Judge, Competition, Segment, Skater, SkaterSegment, Element, ElementScorePerJudge, PcsScorePerJudge, PcsType, ElementType, DisciplineType
from database import get_db_session, test_connection


# Initialize connection.
# conn = st.connection("postgresql", type="sql")

class DatabaseLoader:
    def __init__(self, session: Session):
        self.session = session

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
        if "women" in segment_name.lower() or "girl" in segment_name.lower():
            type="Singles"
        elif "men" in segment_name.lower() or "boy" in segment_name.lower():
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


