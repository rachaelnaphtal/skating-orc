from typing import List, Optional

from sqlalchemy import BigInteger, Boolean, Date, ForeignKeyConstraint, Identity, Integer, Numeric, PrimaryKeyConstraint, String, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import decimal

class Base(DeclarativeBase):
    pass


class Competition(Base):
    __tablename__ = 'competition'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='competition_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=32767, cycle=False, cache=1), primary_key=True)
    year: Mapped[str] = mapped_column(String)
    qualifying: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    results_url: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    singles: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    pairs: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    dance: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    synchronized: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    location: Mapped[Optional[str]] = mapped_column(String)

    segment: Mapped[List['Segment']] = relationship('Segment', back_populates='competition')


class DisciplineType(Base):
    __tablename__ = 'discipline_type'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='discipline_type_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=32767, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)

    segment: Mapped[List['Segment']] = relationship('Segment', back_populates='discipline_type')


class ElementType(Base):
    __tablename__ = 'element_type'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='element_type_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=32767, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)

    element: Mapped[List['Element']] = relationship('Element', back_populates='element_type_')


class Judge(Base):
    __tablename__ = 'judge'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='judge_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    location: Mapped[Optional[str]] = mapped_column(String)

    pcs_score_per_judge: Mapped[List['PcsScorePerJudge']] = relationship('PcsScorePerJudge', back_populates='judge')
    element_score_per_judge: Mapped[List['ElementScorePerJudge']] = relationship('ElementScorePerJudge', back_populates='judge')


class PcsType(Base):
    __tablename__ = 'pcs_type'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='pcs_type_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=32767, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)

    pcs_score_per_judge: Mapped[List['PcsScorePerJudge']] = relationship('PcsScorePerJudge', back_populates='pcs_type')


class Skater(Base):
    __tablename__ = 'skater'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='skater_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    club: Mapped[Optional[str]] = mapped_column(String)

    skater_segment: Mapped[List['SkaterSegment']] = relationship('SkaterSegment', back_populates='skater')


class Segment(Base):
    __tablename__ = 'segment'
    __table_args__ = (
        ForeignKeyConstraint(['competition_id'], ['competition.id'], name='segment_competition_id_fkey'),
        ForeignKeyConstraint(['discipline_type_id'], ['discipline_type.id'], name='segment_discipline_type_id_fkey'),
        PrimaryKeyConstraint('id', name='segment_pkey')
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    competition_id: Mapped[int] = mapped_column(Integer)
    freeskate: Mapped[Optional[bool]] = mapped_column(Boolean)
    discipline_type_id: Mapped[Optional[int]] = mapped_column(Integer)

    competition: Mapped['Competition'] = relationship('Competition', back_populates='segment')
    discipline_type: Mapped[Optional['DisciplineType']] = relationship('DisciplineType', back_populates='segment')
    skater_segment: Mapped[List['SkaterSegment']] = relationship('SkaterSegment', back_populates='segment')


class SkaterSegment(Base):
    __tablename__ = 'skater_segment'
    __table_args__ = (
        ForeignKeyConstraint(['segment_id'], ['segment.id'], name='skater_segment_segment_id_fkey'),
        ForeignKeyConstraint(['skater_id'], ['skater.id'], name='skater_segment_skater_id_fkey'),
        PrimaryKeyConstraint('id', name='skater_segment_pkey')
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    skater_id: Mapped[int] = mapped_column(Integer)
    segment_id: Mapped[int] = mapped_column(Integer)
    start_number: Mapped[Optional[int]] = mapped_column(Integer)

    segment: Mapped['Segment'] = relationship('Segment', back_populates='skater_segment')
    skater: Mapped['Skater'] = relationship('Skater', back_populates='skater_segment')
    element: Mapped[List['Element']] = relationship('Element', back_populates='skater_segment')
    pcs_score_per_judge: Mapped[List['PcsScorePerJudge']] = relationship('PcsScorePerJudge', back_populates='skater_segment')


class Element(Base):
    __tablename__ = 'element'
    __table_args__ = (
        ForeignKeyConstraint(['element_type_id'], ['element_type.id'], name='element_element_type_id_fkey'),
        ForeignKeyConstraint(['skater_segment_id'], ['skater_segment.id'], name='element_skater_segment_id_fkey'),
        PrimaryKeyConstraint('id', name='element_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    skater_segment_id: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String)
    element_type: Mapped[str] = mapped_column(String)
    element_type_id: Mapped[Optional[int]] = mapped_column(Integer)
    base_value: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)

    element_type_: Mapped[Optional['ElementType']] = relationship('ElementType', back_populates='element')
    skater_segment: Mapped['SkaterSegment'] = relationship('SkaterSegment', back_populates='element')
    element_score_per_judge: Mapped[List['ElementScorePerJudge']] = relationship('ElementScorePerJudge', back_populates='element')


class PcsScorePerJudge(Base):
    __tablename__ = 'pcs_score_per_judge'
    __table_args__ = (
        ForeignKeyConstraint(['judge_id'], ['judge.id'], name='pcs_score_per_judge_judge_id_fkey'),
        ForeignKeyConstraint(['pcs_type_id'], ['pcs_type.id'], name='pcs_score_per_judge_pcs_type_id_fkey'),
        ForeignKeyConstraint(['skater_segment_id'], ['skater_segment.id'], name='pcs_score_per_judge_skater_segment_id_fkey'),
        PrimaryKeyConstraint('id', name='pcs_score_per_judge_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    skater_segment_id: Mapped[int] = mapped_column(Integer)
    pcs_type_id: Mapped[int] = mapped_column(Integer)
    judge_id: Mapped[int] = mapped_column(Integer)
    judge_score: Mapped[decimal.Decimal] = mapped_column(Numeric)
    panel_average: Mapped[decimal.Decimal] = mapped_column(Numeric)
    deviation: Mapped[decimal.Decimal] = mapped_column(Numeric)
    thrown_out: Mapped[bool] = mapped_column(Boolean)
    is_rule_error: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))

    judge: Mapped['Judge'] = relationship('Judge', back_populates='pcs_score_per_judge')
    pcs_type: Mapped['PcsType'] = relationship('PcsType', back_populates='pcs_score_per_judge')
    skater_segment: Mapped['SkaterSegment'] = relationship('SkaterSegment', back_populates='pcs_score_per_judge')


class ElementScorePerJudge(Base):
    __tablename__ = 'element_score_per_judge'
    __table_args__ = (
        ForeignKeyConstraint(['element_id'], ['element.id'], name='element_score_per_judge_element_id_fkey'),
        ForeignKeyConstraint(['judge_id'], ['judge.id'], name='element_score_per_judge_judge_id_fkey'),
        PrimaryKeyConstraint('id', name='element_score_per_judge_pkey')
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=9223372036854775807, cycle=False, cache=1), primary_key=True)
    element_id: Mapped[int] = mapped_column(Integer)
    judge_id: Mapped[int] = mapped_column(Integer)
    judge_score: Mapped[decimal.Decimal] = mapped_column(Numeric)
    panel_average: Mapped[decimal.Decimal] = mapped_column(Numeric)
    deviation: Mapped[decimal.Decimal] = mapped_column(Numeric)
    thrown_out: Mapped[bool] = mapped_column(Boolean)
    is_rule_error: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))

    element: Mapped['Element'] = relationship('Element', back_populates='element_score_per_judge')
    judge: Mapped['Judge'] = relationship('Judge', back_populates='element_score_per_judge')
