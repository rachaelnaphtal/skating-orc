from typing import Any, List, Optional

from sqlalchemy import JSON, Boolean, Date, DateTime, ForeignKeyConstraint, Identity, Integer, PrimaryKeyConstraint, String, Text, UniqueConstraint, text
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
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

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
    international: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

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
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

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
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

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
    date_of_birth: Mapped[Optional[datetime.date]] = mapped_column(Date)
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

    appointments: Mapped[List['Appointments']] = relationship('Appointments', back_populates='official')
    assignment: Mapped[List['Assignment']] = relationship('Assignment', back_populates='official')
    qualifying_form_responses: Mapped[List['QualifyingOfficialFormResponse']] = relationship(
        'QualifyingOfficialFormResponse', back_populates='official'
    )
    isu_seminars: Mapped[List['IsuOfficialSeminar']] = relationship(
        'IsuOfficialSeminar', back_populates='official'
    )


class IsuOfficial(Base):
    __tablename__ = 'isu_official'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='isu_official_pkey'),
        UniqueConstraint(
            'federation_code',
            'name_normalized',
            name='isu_official_roster_unique',
        ),
        {'schema': 'officials_analysis'},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
        primary_key=True,
    )
    federation_code: Mapped[str] = mapped_column(Text)
    federation_name: Mapped[Optional[str]] = mapped_column(Text)
    full_name: Mapped[str] = mapped_column(Text)
    first_name: Mapped[Optional[str]] = mapped_column(Text)
    last_name: Mapped[Optional[str]] = mapped_column(Text)
    name_normalized: Mapped[str] = mapped_column(Text)
    season: Mapped[str] = mapped_column(Text)
    communication_ref: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text('now()')
    )
    last_modified: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text('now()')
    )


class IsuOfficialAppointment(Base):
    __tablename__ = 'isu_official_appointment'
    __table_args__ = (
        ForeignKeyConstraint(
            ['isu_official_id'],
            ['officials_analysis.isu_official.id'],
            ondelete='CASCADE',
            name='isu_official_appointment_isu_official_id_fkey',
        ),
        PrimaryKeyConstraint('id', name='isu_official_appointment_pkey'),
        UniqueConstraint(
            'isu_official_id',
            'discipline',
            'appointment_type',
            'level',
            'season',
            name='isu_official_appointment_unique',
        ),
        {'schema': 'officials_analysis'},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
        primary_key=True,
    )
    isu_official_id: Mapped[int] = mapped_column(Integer)
    discipline: Mapped[str] = mapped_column(Text, server_default=text("''"))
    appointment_type: Mapped[str] = mapped_column(Text, server_default=text("''"))
    level: Mapped[str] = mapped_column(Text, server_default=text("''"))
    season: Mapped[str] = mapped_column(Text)
    communication_ref: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text('now()')
    )
    last_modified: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text('now()')
    )


class Appointments(Base):
    __tablename__ = 'appointments'
    __table_args__ = (
        ForeignKeyConstraint(['appointment_type_id'], ['officials_analysis.appointment_types.id'], name='appointments_appointment_type_id_fkey'),
        ForeignKeyConstraint(['discipline_id'], ['officials_analysis.disciplines.id'], name='appointments_discipline_id_fkey'),
        ForeignKeyConstraint(['level_id'], ['officials_analysis.levels.id'], name='appointments_level_id_fkey'),
        ForeignKeyConstraint(['official_id'], ['officials_analysis.officials.id'], name='appointments_official_id_fkey'),
        PrimaryKeyConstraint('id', name='appointments_pkey'),
        UniqueConstraint(
            'official_id',
            'appointment_type_id',
            'discipline_id',
            'level_id',
            name='appointments_unique',
            postgresql_nulls_not_distinct=True,
        ),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    active: Mapped[bool] = mapped_column(Boolean, server_default=text('true'))
    official_id: Mapped[Optional[int]] = mapped_column(Integer)
    appointment_type_id: Mapped[Optional[int]] = mapped_column(Integer)
    discipline_id: Mapped[Optional[int]] = mapped_column(Integer)
    level_id: Mapped[Optional[int]] = mapped_column(Integer)
    appointed_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    achieved_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    mentor: Mapped[Optional[str]] = mapped_column(Text)
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

    appointment_type: Mapped[Optional['AppointmentTypes']] = relationship('AppointmentTypes', back_populates='appointments')
    discipline: Mapped[Optional['Disciplines']] = relationship('Disciplines', back_populates='appointments')
    level: Mapped[Optional['Levels']] = relationship('Levels', back_populates='appointments')
    official: Mapped[Optional['Officials']] = relationship('Officials', back_populates='appointments')


class Competition(Base):
    __tablename__ = 'competition'
    __table_args__ = (
        ForeignKeyConstraint(['competition_type_id'], ['officials_analysis.competition_type.id'], name='competition_competition_type_id_fkey'),
        PrimaryKeyConstraint('id', name='competition_pkey'),
        UniqueConstraint('year', 'competition_type_id', name='competition_unique'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    year: Mapped[int] = mapped_column(Integer)
    competition_type_id: Mapped[int] = mapped_column(Integer)
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

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
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('CURRENT_TIMESTAMP'))

    appointment_type: Mapped['AppointmentTypes'] = relationship('AppointmentTypes', back_populates='assignment')
    competition: Mapped['Competition'] = relationship('Competition', back_populates='assignment')
    discipline: Mapped['Disciplines'] = relationship('Disciplines', back_populates='assignment')
    official: Mapped['Officials'] = relationship('Officials', back_populates='assignment')


class QualifyingAvailabilityForm(Base):
    """One uploaded qualifying availability workbook (e.g. 2027 SPD synchro adults)."""

    __tablename__ = 'qualifying_availability_form'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='qualifying_availability_form_pkey'),
        {'schema': 'officials_analysis'},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
        primary_key=True,
    )
    label: Mapped[str] = mapped_column(Text)
    source_filename: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    loaded_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text('CURRENT_TIMESTAMP')
    )

    competitions: Mapped[List['QualifyingAvailabilityCompetition']] = relationship(
        'QualifyingAvailabilityCompetition', back_populates='form', cascade='all, delete-orphan'
    )
    responses: Mapped[List['QualifyingOfficialFormResponse']] = relationship(
        'QualifyingOfficialFormResponse', back_populates='form', cascade='all, delete-orphan'
    )


class QualifyingAvailabilityCompetition(Base):
    """A per-competition column from the form (availability prompt header)."""

    __tablename__ = 'qualifying_availability_competition'
    __table_args__ = (
        ForeignKeyConstraint(
            ['form_id'],
            ['officials_analysis.qualifying_availability_form.id'],
            name='qualifying_availability_competition_form_id_fkey',
            ondelete='CASCADE',
        ),
        PrimaryKeyConstraint('id', name='qualifying_availability_competition_pkey'),
        UniqueConstraint(
            'form_id',
            'prompt_key',
            name='qualifying_availability_competition_form_prompt_key',
        ),
        {'schema': 'officials_analysis'},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
        primary_key=True,
    )
    form_id: Mapped[int] = mapped_column(Integer)
    prompt_key: Mapped[str] = mapped_column(Text)
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    location: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    event_dates: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    season_year: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, server_default=text('0'))
    competition_group: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    form: Mapped['QualifyingAvailabilityForm'] = relationship(
        'QualifyingAvailabilityForm', back_populates='competitions'
    )
    criteria: Mapped[List['QualifyingCompetitionCriteria']] = relationship(
        'QualifyingCompetitionCriteria', back_populates='competition', cascade='all, delete-orphan'
    )


class QualifyingCompetitionCriteria(Base):
    """Directory appointment type × discipline × optional level relevant to a form competition."""

    __tablename__ = 'qualifying_competition_criteria'
    __table_args__ = (
        ForeignKeyConstraint(
            ['competition_id'],
            ['officials_analysis.qualifying_availability_competition.id'],
            name='qualifying_competition_criteria_competition_id_fkey',
            ondelete='CASCADE',
        ),
        ForeignKeyConstraint(
            ['appointment_type_id'],
            ['officials_analysis.appointment_types.id'],
            name='qualifying_competition_criteria_appointment_type_id_fkey',
        ),
        ForeignKeyConstraint(
            ['discipline_id'],
            ['officials_analysis.disciplines.id'],
            name='qualifying_competition_criteria_discipline_id_fkey',
        ),
        ForeignKeyConstraint(
            ['level_id'],
            ['officials_analysis.levels.id'],
            name='qualifying_competition_criteria_level_id_fkey',
        ),
        PrimaryKeyConstraint('id', name='qualifying_competition_criteria_pkey'),
        UniqueConstraint(
            'competition_id',
            'appointment_type_id',
            'discipline_id',
            'level_id',
            name='qualifying_competition_criteria_unique',
            postgresql_nulls_not_distinct=True,
        ),
        {'schema': 'officials_analysis'},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
        primary_key=True,
    )
    competition_id: Mapped[int] = mapped_column(Integer)
    appointment_type_id: Mapped[int] = mapped_column(Integer)
    discipline_id: Mapped[int] = mapped_column(Integer)
    level_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    competition: Mapped['QualifyingAvailabilityCompetition'] = relationship(
        'QualifyingAvailabilityCompetition', back_populates='criteria'
    )
    appointment_type: Mapped['AppointmentTypes'] = relationship('AppointmentTypes')
    discipline: Mapped['Disciplines'] = relationship('Disciplines')
    level: Mapped[Optional['Levels']] = relationship('Levels')


class QualifyingOfficialFormResponse(Base):
    """Entire form row for one official (all columns in ``response_json``)."""

    __tablename__ = 'qualifying_official_form_response'
    __table_args__ = (
        ForeignKeyConstraint(
            ['form_id'],
            ['officials_analysis.qualifying_availability_form.id'],
            name='qualifying_official_form_response_form_id_fkey',
            ondelete='CASCADE',
        ),
        ForeignKeyConstraint(
            ['official_id'],
            ['officials_analysis.officials.id'],
            name='qualifying_official_form_response_official_id_fkey',
        ),
        PrimaryKeyConstraint('id', name='qualifying_official_form_response_pkey'),
        UniqueConstraint(
            'form_id',
            'official_id',
            name='qualifying_official_form_response_form_official_unique',
        ),
        {'schema': 'officials_analysis'},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
        primary_key=True,
    )
    form_id: Mapped[int] = mapped_column(Integer)
    official_id: Mapped[int] = mapped_column(Integer)
    member_number: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    response_json: Mapped[dict[str, Any]] = mapped_column(JSON)

    form: Mapped['QualifyingAvailabilityForm'] = relationship(
        'QualifyingAvailabilityForm', back_populates='responses'
    )
    official: Mapped['Officials'] = relationship('Officials', back_populates='qualifying_form_responses')


class QualifyingOfficialCompetitionAvailability(Base):
    """Per-competition availability extracted from the form (for reporting)."""

    __tablename__ = 'qualifying_official_competition_availability'
    __table_args__ = (
        ForeignKeyConstraint(
            ['form_id'],
            ['officials_analysis.qualifying_availability_form.id'],
            name='qualifying_official_comp_avail_form_id_fkey',
            ondelete='CASCADE',
        ),
        ForeignKeyConstraint(
            ['official_id'],
            ['officials_analysis.officials.id'],
            name='qualifying_official_comp_avail_official_id_fkey',
        ),
        ForeignKeyConstraint(
            ['competition_id'],
            ['officials_analysis.qualifying_availability_competition.id'],
            name='qualifying_official_comp_avail_competition_id_fkey',
            ondelete='CASCADE',
        ),
        PrimaryKeyConstraint(
            'form_id',
            'official_id',
            'competition_id',
            name='qualifying_official_competition_availability_pkey',
        ),
        {'schema': 'officials_analysis'},
    )

    form_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    official_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    competition_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    availability_code: Mapped[str] = mapped_column(Text)
    raw_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class IsuOfficialSeminar(Base):
    """ISU seminar attendance for international listing requirement evaluation."""

    __tablename__ = 'isu_official_seminar'
    __table_args__ = (
        ForeignKeyConstraint(
            ['official_id'],
            ['officials_analysis.officials.id'],
            name='isu_official_seminar_official_id_fkey',
        ),
        ForeignKeyConstraint(
            ['appointment_type_id'],
            ['officials_analysis.appointment_types.id'],
            name='isu_official_seminar_appointment_type_id_fkey',
        ),
        ForeignKeyConstraint(
            ['discipline_id'],
            ['officials_analysis.disciplines.id'],
            name='isu_official_seminar_discipline_id_fkey',
        ),
        PrimaryKeyConstraint('id', name='isu_official_seminar_pkey'),
        {'schema': 'officials_analysis'},
    )

    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1),
        primary_key=True,
    )
    official_id: Mapped[int] = mapped_column(Integer)
    appointment_type_id: Mapped[int] = mapped_column(Integer)
    discipline_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    seminar_date: Mapped[datetime.date] = mapped_column(Date)
    season_code: Mapped[int] = mapped_column(Integer)
    in_person: Mapped[bool] = mapped_column(Boolean)
    place: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    at_event: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text('now()')
    )
    last_modified: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text('now()')
    )

    official: Mapped['Officials'] = relationship('Officials', back_populates='isu_seminars')
    appointment_type: Mapped['AppointmentTypes'] = relationship('AppointmentTypes')
    discipline: Mapped[Optional['Disciplines']] = relationship('Disciplines')
