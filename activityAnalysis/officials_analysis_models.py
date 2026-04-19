from typing import List, Optional

from sqlalchemy import Boolean, Date, ForeignKeyConstraint, Identity, Integer, PrimaryKeyConstraint, String, Text, UniqueConstraint, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime

class Base(DeclarativeBase):
    pass


class AppointmentTypes(Base):
    __tablename__ = 'appointment_types'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='appointment_types_pkey'),
        UniqueConstraint('name', name='appointment_types_name_key'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)

    appointments: Mapped[List['Appointments']] = relationship('Appointments', back_populates='appointment_type')
    assignment: Mapped[List['Assignment']] = relationship('Assignment', back_populates='appointment_type')


class CompetitionType(Base):
    __tablename__ = 'competition_type'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='competition_type_pkey'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)

    competition: Mapped[List['Competition']] = relationship('Competition', back_populates='competition_type')


class Disciplines(Base):
    __tablename__ = 'disciplines'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='disciplines_pkey'),
        UniqueConstraint('name', name='disciplines_name_key'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)

    appointments: Mapped[List['Appointments']] = relationship('Appointments', back_populates='discipline')
    assignment: Mapped[List['Assignment']] = relationship('Assignment', back_populates='discipline')


class Levels(Base):
    __tablename__ = 'levels'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='levels_pkey'),
        UniqueConstraint('name', name='levels_name_key'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)

    appointments: Mapped[List['Appointments']] = relationship('Appointments', back_populates='level')


class Officials(Base):
    __tablename__ = 'officials'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='officials_pkey'),
        UniqueConstraint('mbr_number', name='officials_mbr_number_key'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mbr_number: Mapped[Optional[str]] = mapped_column(Text)
    first_name: Mapped[Optional[str]] = mapped_column(Text)
    last_name: Mapped[Optional[str]] = mapped_column(Text)
    full_name: Mapped[Optional[str]] = mapped_column(Text)
    is_coach: Mapped[Optional[bool]] = mapped_column(Boolean)
    email: Mapped[Optional[str]] = mapped_column(Text)
    phone: Mapped[Optional[str]] = mapped_column(Text)
    city: Mapped[Optional[str]] = mapped_column(Text)
    state: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[Optional[str]] = mapped_column(Text)

    appointments: Mapped[List['Appointments']] = relationship('Appointments', back_populates='official')
    assignment: Mapped[List['Assignment']] = relationship('Assignment', back_populates='official')


class Appointments(Base):
    __tablename__ = 'appointments'
    __table_args__ = (
        ForeignKeyConstraint(['appointment_type_id'], ['officials_analysis.appointment_types.id'], name='appointments_appointment_type_id_fkey'),
        ForeignKeyConstraint(['discipline_id'], ['officials_analysis.disciplines.id'], name='appointments_discipline_id_fkey'),
        ForeignKeyConstraint(['level_id'], ['officials_analysis.levels.id'], name='appointments_level_id_fkey'),
        ForeignKeyConstraint(['official_id'], ['officials_analysis.officials.id'], name='appointments_official_id_fkey'),
        PrimaryKeyConstraint('id', name='appointments_pkey'),
        UniqueConstraint('official_id', 'appointment_type_id', 'discipline_id', 'level_id', name='appointments_unique'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    official_id: Mapped[Optional[int]] = mapped_column(Integer)
    appointment_type_id: Mapped[Optional[int]] = mapped_column(Integer)
    discipline_id: Mapped[Optional[int]] = mapped_column(Integer)
    level_id: Mapped[Optional[int]] = mapped_column(Integer)
    appointed_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    achieved_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    mentor: Mapped[Optional[str]] = mapped_column(Text)

    appointment_type: Mapped[Optional['AppointmentTypes']] = relationship('AppointmentTypes', back_populates='appointments')
    discipline: Mapped[Optional['Disciplines']] = relationship('Disciplines', back_populates='appointments')
    level: Mapped[Optional['Levels']] = relationship('Levels', back_populates='appointments')
    official: Mapped[Optional['Officials']] = relationship('Officials', back_populates='appointments')


class Competition(Base):
    __tablename__ = 'competition'
    __table_args__ = (
        ForeignKeyConstraint(['competition_type_id'], ['officials_analysis.competition_type.id'], name='competition_competition_type_id_fkey'),
        PrimaryKeyConstraint('id', name='competition_pkey'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    year: Mapped[int] = mapped_column(Integer)
    competition_type_id: Mapped[int] = mapped_column(Integer)

    competition_type: Mapped['CompetitionType'] = relationship('CompetitionType', back_populates='competition')
    assignment: Mapped[List['Assignment']] = relationship('Assignment', back_populates='competition')


class Assignment(Base):
    __tablename__ = 'assignment'
    __table_args__ = (
        ForeignKeyConstraint(['appointment_type_id'], ['officials_analysis.appointment_types.id'], name='assignment_appointment_types_id_fkey'),
        ForeignKeyConstraint(['competition_id'], ['officials_analysis.competition.id'], name='assignment_competition_id_fkey'),
        ForeignKeyConstraint(['discipline_id'], ['officials_analysis.disciplines.id'], name='assignment_discipline_id_fkey'),
        ForeignKeyConstraint(['official_id'], ['officials_analysis.officials.id'], name='assignment_official_id_fkey'),
        PrimaryKeyConstraint('id', name='assignment_pkey'),
        UniqueConstraint('competition_id', 'official_id', 'discipline_id', 'appointment_type_id', name='assignment_competition_id_official_id_discipline_id_appoint_key'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    competition_id: Mapped[int] = mapped_column(Integer)
    official_id: Mapped[int] = mapped_column(Integer)
    discipline_id: Mapped[int] = mapped_column(Integer)
    appointment_type_id: Mapped[int] = mapped_column(Integer)
    chief: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    lower_levels_only: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))

    appointment_type: Mapped['AppointmentTypes'] = relationship('AppointmentTypes', back_populates='assignment')
    competition: Mapped['Competition'] = relationship('Competition', back_populates='assignment')
    discipline: Mapped['Disciplines'] = relationship('Disciplines', back_populates='assignment')
    official: Mapped['Officials'] = relationship('Officials', back_populates='assignment')
