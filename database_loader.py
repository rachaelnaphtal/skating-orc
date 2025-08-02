import streamlit as st
from sqlalchemy.sql import text
import pandas as pd
from models import Judge, Competition, Segment, Skater, SkaterSegment, Element, ElementScorePerJudge, PcsScorePerJudge, PcsType, ElementType, DisciplineType


# Initialize connection.
conn = st.connection("postgresql", type="sql")

def insert_competition(name, url, year):
    with conn.session as s:
        existing = s.query(Competition).filter_by(results_url=url).first()
        if not existing:
            new = Competition(name=name, results_url=url, year=year)
            s.add(new)
            s.commit()
            return new.id
        return existing.id
    
def insert_discipline_type(segment_name):
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

    with conn.session as s:
        existing = s.query(DisciplineType).filter_by(name=type).first()
        if not existing:
            new = DisciplineType(name=type)
            s.add(new)
            s.commit()
            return new.id
        return existing.id

def insert_segment(segment_name, competition_id):
    is_freeskate = "free" in segment_name.lower()
    discipline_type_id = insert_discipline_type(segment_name)
    with conn.session as s:
        existing = s.query(Segment).filter_by(name=segment_name, competition_id=competition_id).first()
        if not existing:
            new = Segment(name=segment_name, competition_id=competition_id, freeskate=is_freeskate, discipline_type_id=discipline_type_id)
            s.add(new)
            s.commit()
            return new.id
        existing.discipline_type_id = discipline_type_id
        existing.freeskate = is_freeskate
        s.commit()
        return existing.id


def insert_judge(judge_name):
    with conn.session as s:
        existing_judge = s.query(Judge).filter_by(name=judge_name).first()
        if not existing_judge:
            new_judge = Judge(name=judge_name)
            s.add(new_judge)
            s.commit()
            return new_judge.id
        return existing_judge.id

def insert_skater(skater_name):
    with conn.session as s:
        existing = s.query(Skater).filter_by(name=skater_name).first()
        if not existing:
            new = Skater(name=skater_name)
            s.add(new)
            s.commit()
            return new.id
        return existing.id


def insert_skater_segment(segment_id, skater_id):
    with conn.session as s:
        existing = s.query(SkaterSegment).filter_by(segment_id=segment_id, skater_id=skater_id).first()
        if not existing:
            new = SkaterSegment(segment_id=segment_id, skater_id=skater_id)
            s.add(new)
            s.commit()
            return new.id
        return existing.id

def insert_element_type(element_name):
    with conn.session as s:
        existing = s.query(ElementType).filter_by(name=element_name).first()
        if not existing:
            new = ElementType(name=element_name)
            s.add(new)
            s.commit()
            return new.id
        return existing.id
    
def insert_pcs_type(pcs_name):
    with conn.session as s:
        existing = s.query(PcsType).filter_by(name=pcs_name).first()
        if not existing:
            new = PcsType(name=pcs_name)
            s.add(new)
            s.commit()
            return new.id
        return existing.id

def insert_element(element_name, element_type, skater_segment_id):
    with conn.session as s:
        element_type_id= insert_element_type(element_type)
        existing = s.query(Element).filter_by(name=element_name, skater_segment_id=skater_segment_id).first()
        if not existing:
            new = Element(name=element_name, element_type_id = element_type_id, element_type=element_type, skater_segment_id=skater_segment_id)
            s.add(new)
            s.commit()
            return new.id
        else:
            existing.element_type_id = element_type_id
            existing.element_type = element_type
            s.commit()
        return existing.id

def insert_element_score_per_judge(element_id, judge_id, score, panel_average, deviation, thrown_out):
    with conn.session as s:
        existing = s.query(ElementScorePerJudge).filter_by(element_id=element_id, judge_id=judge_id).first()
        if not existing:
            new = ElementScorePerJudge(
                judge_id=judge_id,
                element_id=element_id,
                judge_score=score,
                panel_average=panel_average,
                deviation=deviation,
                thrown_out=thrown_out
            )
            s.add(new)
            s.commit()
            return new.id
        return existing.id
    
def insert_pcs_score_per_judge(skater_segment_id, pcs_type_id, judge_id, score, panel_average, deviation, thrown_out):
    with conn.session as s:
        existing = s.query(PcsScorePerJudge).filter_by(skater_segment_id=skater_segment_id, judge_id=judge_id, pcs_type_id=pcs_type_id).first()
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
            s.add(new)
            s.commit()
            return new.id
        elif existing.judge_score!=score:
            raise NameError("Scores do not align")
        return existing.id

def insert_element_scores(judgesNames, all_element_dict, segment_id, rule_errors):
    judge_dict = {judge: insert_judge(judge) for judge in judgesNames}
    # print(all_element_dict)
    all_element_df = pd.DataFrame.from_dict(all_element_dict)
    skater_dict = {skater: insert_skater(skater) for skater in all_element_df["Skater"].unique()}
    # database_element_dict={} all_element_df[["Skater", "Element"]].drop_duplicates()
    for row in all_element_df.itertuples():
        skater_id = skater_dict[row.Skater]
        skater_segment_id = insert_skater_segment(segment_id, skater_id)
        element_id = insert_element(row.Element, row._4, skater_segment_id)
        insert_element_score_per_judge(element_id, judge_dict[row._6], row.Score, row._5, row.Deviation, row._10)
    insert_rule_errors(rule_errors, segment_id)

def insert_rule_errors(rule_errors, segment_id):
    with conn.session as s:
     for rule_error in rule_errors:
        skater_id = s.query(Skater).filter_by(name=rule_error["Skater"]).first().id
        skater_segment_id = s.query(SkaterSegment).filter_by(segment_id=segment_id, skater_id=skater_id).first().id
        # Rule errors add on the attention column
        converted_name = rule_error["Element"].split(" ")[0]
        element_id = s.query(Element).filter_by(name=converted_name, skater_segment_id=skater_segment_id).first().id
        judge_id = s.query(Judge).filter_by(name=rule_error["Judge Name"]).first().id
        score = s.query(ElementScorePerJudge).filter_by(element_id=element_id, judge_id=judge_id).first()
        score.is_rule_error = True
        s.commit()

def insert_pcs_scores(judgesNames, all_pcs_dict, segment_id):
    judge_dict = {judge: insert_judge(judge) for judge in judgesNames}
    all_pcs_df = pd.DataFrame.from_dict(all_pcs_dict)
    skater_dict = {skater: insert_skater(skater) for skater in all_pcs_df["Skater"].unique()}
    for row in all_pcs_df.itertuples():
        skater_id = skater_dict[row.Skater]
        skater_segment_id = insert_skater_segment(segment_id, skater_id)
        pcs_type_id = insert_pcs_type(row.Component)
        insert_pcs_score_per_judge(skater_segment_id, pcs_type_id, judge_dict[row._5], row.Score, row._4, row.Deviation, row._9)

# insert_competition("Test", "www.test.com", "2456")
# insert_competition("Test", "www.test.com", "2456")
# pet_owners = conn.query('select * from competition')
# print(st.dataframe(pet_owners))
# print(insert_judge("O'Connor"))