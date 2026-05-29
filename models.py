from typing import List, Optional

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, Date, DateTime, Double, ForeignKey, ForeignKeyConstraint, Identity, Index, Integer, LargeBinary, Numeric, PrimaryKeyConstraint, String, Table, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import OID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import decimal

class Base(DeclarativeBase):
    pass


class Competition(Base):
    __tablename__ = 'competition'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='competition_pkey'),
        UniqueConstraint('results_url', name='competition_unique')
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
    nqs: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    international: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))
    officials_analysis_competition_type_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey(
            "officials_analysis.competition_type.id",
            name="competition_officials_analysis_competition_type_id_fkey",
        ),
        nullable=True,
    )
    start_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    location: Mapped[Optional[str]] = mapped_column(String)
    international: Mapped[bool] = mapped_column(Boolean, server_default=text('false'))

    segment: Mapped[List['Segment']] = relationship('Segment', back_populates='competition')


class CompetitionType(Base):
    """``officials_analysis.competition_type`` — target of ``Competition.officials_analysis_competition_type_id``."""

    __tablename__ = 'competition_type'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='competition_type_pkey'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('now()'))


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

    judge_excess_anomalies_cache: Mapped[List['JudgeExcessAnomaliesCache']] = relationship('JudgeExcessAnomaliesCache', back_populates='judge')
    pcs_score_per_judge: Mapped[List['PcsScorePerJudge']] = relationship('PcsScorePerJudge', back_populates='judge')
    element_score_per_judge: Mapped[List['ElementScorePerJudge']] = relationship('ElementScorePerJudge', back_populates='judge')


class JudgeEmailList(Base):
    __tablename__ = 'judge_email_list'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='judge_email_list_pkey'),
        UniqueConstraint('judge_name', name='judge_email_list_judge_name_key')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    judge_name: Mapped[str] = mapped_column(Text)
    email: Mapped[str] = mapped_column(Text)


class JudgeSummaryCache(Base):
    __tablename__ = 'judge_summary_cache'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='judge_summary_cache_pkey'),
        UniqueConstraint('judge_id', 'year_filter', 'competition_ids', 'discipline_type_ids', 'score_type', name='judge_summary_cache_judge_id_year_filter_competition_ids_di_key'),
        Index('idx_judge_summary_filters', 'year_filter', 'score_type'),
        Index('idx_judge_summary_judge', 'judge_id')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    judge_id: Mapped[int] = mapped_column(Integer)
    score_type: Mapped[str] = mapped_column(String(20))
    year_filter: Mapped[Optional[int]] = mapped_column(Integer)
    competition_ids: Mapped[Optional[str]] = mapped_column(Text)
    discipline_type_ids: Mapped[Optional[str]] = mapped_column(Text)
    total_scores: Mapped[Optional[int]] = mapped_column(Integer, server_default=text('0'))
    pcs_scores: Mapped[Optional[int]] = mapped_column(Integer, server_default=text('0'))
    element_scores: Mapped[Optional[int]] = mapped_column(Integer, server_default=text('0'))
    throwout_rate: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(5, 2), server_default=text('0'))
    anomaly_rate: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(5, 2), server_default=text('0'))
    rule_error_rate: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(5, 2), server_default=text('0'))
    avg_deviation: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric(8, 4), server_default=text('0'))
    total_excess_anomalies: Mapped[Optional[int]] = mapped_column(Integer, server_default=text('0'))
    computed_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, server_default=text('CURRENT_TIMESTAMP'))


class CrossJudgeCompetitionShard(Base):
    """Per-competition judge×discipline score aggregates for cross-judge benchmarking."""

    __tablename__ = "cross_judge_competition_shard"
    __table_args__ = (
        PrimaryKeyConstraint(
            "competition_id",
            "discipline_type_id",
            "judge_id",
            name="cross_judge_competition_shard_pkey",
        ),
        Index("idx_cross_judge_shard_competition", "competition_id"),
        Index("idx_cross_judge_shard_year", "competition_year"),
    )

    competition_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    discipline_type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    judge_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    competition_year: Mapped[str] = mapped_column(String(8))
    pcs_total: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    pcs_throwouts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    pcs_anomalies: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    pcs_rule_errors: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    pcs_sum_deviation: Mapped[float] = mapped_column(
        Double, server_default=text("0")
    )
    pcs_sum_abs_deviation: Mapped[float] = mapped_column(
        Double, server_default=text("0")
    )
    elem_total: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    elem_throwouts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    elem_anomalies: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    elem_rule_errors: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    elem_sum_deviation: Mapped[float] = mapped_column(
        Double, server_default=text("0")
    )
    elem_sum_abs_deviation: Mapped[float] = mapped_column(
        Double, server_default=text("0")
    )
    computed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )


class ElementDeviationRankingShardCache(Base):
    """Per-season, per-discipline element marks (assembled into full rankings on read)."""

    __tablename__ = "element_deviation_ranking_shard_cache"
    __table_args__ = (
        PrimaryKeyConstraint("shard_key", name="element_deviation_ranking_shard_cache_pkey"),
        Index(
            "idx_element_ranking_shard_season_disc",
            "season_year",
            "discipline_type_id",
        ),
    )

    shard_key: Mapped[str] = mapped_column(String(24), primary_key=True)
    season_year: Mapped[str] = mapped_column(String(8))
    discipline_type_id: Mapped[int] = mapped_column(Integer)
    competition_scope: Mapped[str] = mapped_column(String(32))
    event_start_iso: Mapped[Optional[str]] = mapped_column(String(10))
    event_end_iso: Mapped[Optional[str]] = mapped_column(String(10))
    data_fingerprint: Mapped[str] = mapped_column(String(64))
    marks_payload: Mapped[bytes] = mapped_column(LargeBinary)
    n_marks: Mapped[Optional[int]] = mapped_column(Integer)
    computed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(True), server_default=text("now()")
    )


class ElementDeviationRankingSigmaCache(Base):
    """Fitted σ̂ bin parameters for a benchmark mark pool (separate from ranking scope)."""

    __tablename__ = "element_deviation_ranking_sigma_cache"
    __table_args__ = (
        PrimaryKeyConstraint("sigma_key", name="element_deviation_ranking_sigma_cache_pkey"),
        Index(
            "idx_element_ranking_sigma_seasons",
            "benchmark_start_season_year",
            "benchmark_end_season_year",
        ),
    )

    sigma_key: Mapped[str] = mapped_column(String(24), primary_key=True)
    benchmark_start_season_year: Mapped[Optional[str]] = mapped_column(String(8))
    benchmark_end_season_year: Mapped[Optional[str]] = mapped_column(String(8))
    scope_json: Mapped[str] = mapped_column(Text)
    data_fingerprint: Mapped[str] = mapped_column(String(64))
    params_payload: Mapped[bytes] = mapped_column(LargeBinary)
    floor_sigma: Mapped[Optional[float]] = mapped_column(Numeric(8, 4))
    min_bin_count: Mapped[Optional[int]] = mapped_column(Integer)
    n_marks: Mapped[Optional[int]] = mapped_column(Integer)
    computed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(True), server_default=text("now()")
    )


class ElementDeviationRankingCache(Base):
    """Precomputed element deviation ranking payloads (pickled, keyed by filter tuple)."""

    __tablename__ = "element_deviation_ranking_cache"
    __table_args__ = (
        PrimaryKeyConstraint("cache_key", name="element_deviation_ranking_cache_pkey"),
        Index(
            "idx_element_ranking_cache_seasons",
            "start_season_year",
            "end_season_year",
        ),
    )

    cache_key: Mapped[str] = mapped_column(String(24), primary_key=True)
    start_season_year: Mapped[Optional[str]] = mapped_column(String(8))
    end_season_year: Mapped[Optional[str]] = mapped_column(String(8))
    run_params_json: Mapped[str] = mapped_column(Text)
    data_fingerprint: Mapped[str] = mapped_column(String(64))
    result_payload: Mapped[bytes] = mapped_column(LargeBinary)
    ctrl_payload: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    params_payload: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    n_raw_marks: Mapped[Optional[int]] = mapped_column(Integer)
    n_judges: Mapped[Optional[int]] = mapped_column(Integer)
    computed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(True), server_default=text("now()")
    )


t_officials = Table(
    'officials', Base.metadata,
    Column('mbr_number', BigInteger),
    Column('first_name', Text),
    Column('last_name', Text),
    Column('full_name', Text),
    Column('is_coach', Boolean),
    Column('email', Text),
    Column('phone', Text),
    Column('city', Text),
    Column('state', Text),
    Column('region', Text)
)


class AppointmentTypes(Base):
    __tablename__ = 'appointment_types'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='appointment_types_pkey'),
        UniqueConstraint('name', name='appointment_types_name_key'),
        {'schema': 'officials_analysis'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('now()'))

    segment_official: Mapped[List['SegmentOfficial']] = relationship('SegmentOfficial', back_populates='appointment_type')


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
    last_modified: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('now()'))

    judge_official_link: Mapped[List['JudgeOfficialLink']] = relationship('JudgeOfficialLink', back_populates='official')
    official_name_alias: Mapped[List['OfficialNameAlias']] = relationship('OfficialNameAlias', back_populates='official')
    segment_official: Mapped[List['SegmentOfficial']] = relationship('SegmentOfficial', back_populates='official')


class IsuOfficial(Base):
    """ISU Communication roster (separate from USFS ``officials``)."""

    __tablename__ = 'isu_official'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='isu_official_pkey'),
        UniqueConstraint(
            'federation_code',
            'name_normalized',
            'season',
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

    isu_official_name_alias: Mapped[List['IsuOfficialNameAlias']] = relationship(
        'IsuOfficialNameAlias', back_populates='isu_official'
    )
    judge_isu_official_link: Mapped[List['JudgeIsuOfficialLink']] = relationship(
        'JudgeIsuOfficialLink', back_populates='isu_official'
    )
    segment_official: Mapped[List['SegmentOfficial']] = relationship(
        'SegmentOfficial', back_populates='isu_official'
    )


class PcsType(Base):
    __tablename__ = 'pcs_type'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='pcs_type_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=32767, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)

    pcs_score_per_judge: Mapped[List['PcsScorePerJudge']] = relationship('PcsScorePerJudge', back_populates='pcs_type')


t_pg_stat_statements = Table(
    'pg_stat_statements', Base.metadata,
    Column('userid', OID),
    Column('dbid', OID),
    Column('toplevel', Boolean),
    Column('queryid', BigInteger),
    Column('query', Text),
    Column('plans', BigInteger),
    Column('total_plan_time', Double(53)),
    Column('min_plan_time', Double(53)),
    Column('max_plan_time', Double(53)),
    Column('mean_plan_time', Double(53)),
    Column('stddev_plan_time', Double(53)),
    Column('calls', BigInteger),
    Column('total_exec_time', Double(53)),
    Column('min_exec_time', Double(53)),
    Column('max_exec_time', Double(53)),
    Column('mean_exec_time', Double(53)),
    Column('stddev_exec_time', Double(53)),
    Column('rows', BigInteger),
    Column('shared_blks_hit', BigInteger),
    Column('shared_blks_read', BigInteger),
    Column('shared_blks_dirtied', BigInteger),
    Column('shared_blks_written', BigInteger),
    Column('local_blks_hit', BigInteger),
    Column('local_blks_read', BigInteger),
    Column('local_blks_dirtied', BigInteger),
    Column('local_blks_written', BigInteger),
    Column('temp_blks_read', BigInteger),
    Column('temp_blks_written', BigInteger),
    Column('shared_blk_read_time', Double(53)),
    Column('shared_blk_write_time', Double(53)),
    Column('local_blk_read_time', Double(53)),
    Column('local_blk_write_time', Double(53)),
    Column('temp_blk_read_time', Double(53)),
    Column('temp_blk_write_time', Double(53)),
    Column('wal_records', BigInteger),
    Column('wal_fpi', BigInteger),
    Column('wal_bytes', Numeric),
    Column('jit_functions', BigInteger),
    Column('jit_generation_time', Double(53)),
    Column('jit_inlining_count', BigInteger),
    Column('jit_inlining_time', Double(53)),
    Column('jit_optimization_count', BigInteger),
    Column('jit_optimization_time', Double(53)),
    Column('jit_emission_count', BigInteger),
    Column('jit_emission_time', Double(53)),
    Column('jit_deform_count', BigInteger),
    Column('jit_deform_time', Double(53)),
    Column('stats_since', DateTime(True)),
    Column('minmax_stats_since', DateTime(True))
)


t_pg_stat_statements_info = Table(
    'pg_stat_statements_info', Base.metadata,
    Column('dealloc', BigInteger),
    Column('stats_reset', DateTime(True))
)


class Skater(Base):
    __tablename__ = 'skater'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='skater_pkey'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    club: Mapped[Optional[str]] = mapped_column(String)

    skater_segment: Mapped[List['SkaterSegment']] = relationship('SkaterSegment', back_populates='skater')


class JudgeOfficialLink(Judge):
    __tablename__ = 'judge_official_link'
    __table_args__ = (
        CheckConstraint("status = 'linked'::text AND official_id IS NOT NULL OR status = 'outside_directory'::text AND official_id IS NULL", name='judge_official_link_linked_requires_official'),
        CheckConstraint("status = ANY (ARRAY['linked'::text, 'outside_directory'::text])", name='judge_official_link_status_check'),
        ForeignKeyConstraint(['judge_id'], ['judge.id'], ondelete='CASCADE', name='judge_official_link_judge_id_fkey'),
        ForeignKeyConstraint(['official_id'], ['officials_analysis.officials.id'], ondelete='SET NULL', name='judge_official_link_official_id_fkey'),
        PrimaryKeyConstraint('judge_id', name='judge_official_link_pkey'),
        Index('idx_judge_official_link_official_id', 'official_id')
    )

    judge_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    official_id: Mapped[Optional[int]] = mapped_column(Integer)
    note: Mapped[Optional[str]] = mapped_column(Text)

    official: Mapped[Optional['Officials']] = relationship('Officials', back_populates='judge_official_link')


class IsuOfficialNameAlias(Base):
    __tablename__ = 'isu_official_name_alias'
    __table_args__ = (
        ForeignKeyConstraint(
            ['isu_official_id'],
            ['officials_analysis.isu_official.id'],
            ondelete='CASCADE',
            name='isu_official_name_alias_isu_official_id_fkey',
        ),
        PrimaryKeyConstraint('id', name='isu_official_name_alias_pkey'),
        UniqueConstraint('alias_normalized', name='isu_official_name_alias_alias_normalized_key'),
        Index('ix_isu_official_name_alias_isu_official_id', 'isu_official_id'),
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    alias_normalized: Mapped[str] = mapped_column(Text)
    isu_official_id: Mapped[int] = mapped_column(Integer)
    note: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    isu_official: Mapped['IsuOfficial'] = relationship(
        'IsuOfficial', back_populates='isu_official_name_alias'
    )


class JudgeIsuOfficialLink(Base):
    __tablename__ = 'judge_isu_official_link'
    __table_args__ = (
        ForeignKeyConstraint(['judge_id'], ['judge.id'], ondelete='CASCADE', name='judge_isu_official_link_judge_id_fkey'),
        ForeignKeyConstraint(
            ['isu_official_id'],
            ['officials_analysis.isu_official.id'],
            ondelete='CASCADE',
            name='judge_isu_official_link_isu_official_id_fkey',
        ),
        PrimaryKeyConstraint('judge_id', name='judge_isu_official_link_pkey'),
        Index('idx_judge_isu_official_link_isu_official_id', 'isu_official_id'),
    )

    judge_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    isu_official_id: Mapped[int] = mapped_column(Integer)
    note: Mapped[Optional[str]] = mapped_column(Text)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))

    isu_official: Mapped['IsuOfficial'] = relationship(
        'IsuOfficial', back_populates='judge_isu_official_link'
    )


class OfficialNameAlias(Base):
    __tablename__ = 'official_name_alias'
    __table_args__ = (
        ForeignKeyConstraint(['official_id'], ['officials_analysis.officials.id'], ondelete='CASCADE', name='official_name_alias_official_id_fkey'),
        PrimaryKeyConstraint('id', name='official_name_alias_pkey'),
        UniqueConstraint('alias_normalized', name='official_name_alias_alias_normalized_key'),
        Index('ix_official_name_alias_official_id', 'official_id')
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    alias_normalized: Mapped[str] = mapped_column(Text)
    official_id: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    note: Mapped[Optional[str]] = mapped_column(Text)

    official: Mapped['Officials'] = relationship('Officials', back_populates='official_name_alias')


class Segment(Base):
    __tablename__ = 'segment'
    __table_args__ = (
        ForeignKeyConstraint(['competition_id'], ['competition.id'], ondelete='CASCADE', name='segment_competition_id_fkey'),
        ForeignKeyConstraint(['discipline_type_id'], ['discipline_type.id'], name='segment_discipline_type_id_fkey'),
        PrimaryKeyConstraint('id', name='segment_pkey'),
        UniqueConstraint('competition_id', 'name', name='segment_unique')
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    competition_id: Mapped[int] = mapped_column(Integer)
    freeskate: Mapped[Optional[bool]] = mapped_column(Boolean)
    discipline_type_id: Mapped[Optional[int]] = mapped_column(Integer)

    competition: Mapped['Competition'] = relationship('Competition', back_populates='segment')
    discipline_type: Mapped[Optional['DisciplineType']] = relationship('DisciplineType', back_populates='segment')
    judge_excess_anomalies_cache: Mapped[List['JudgeExcessAnomaliesCache']] = relationship('JudgeExcessAnomaliesCache', back_populates='segment')
    segment_official: Mapped[List['SegmentOfficial']] = relationship('SegmentOfficial', back_populates='segment')
    skater_segment: Mapped[List['SkaterSegment']] = relationship('SkaterSegment', back_populates='segment')


class JudgeExcessAnomaliesCache(Base):
    __tablename__ = 'judge_excess_anomalies_cache'
    __table_args__ = (
        ForeignKeyConstraint(['judge_id'], ['judge.id'], name='judge_excess_anomalies_cache_judge_id_fkey'),
        ForeignKeyConstraint(['segment_id'], ['segment.id'], name='judge_excess_anomalies_cache_segment_id_fkey'),
        PrimaryKeyConstraint('id', name='judge_excess_anomalies_cache_pkey'),
        UniqueConstraint('judge_id', 'segment_id', 'score_type', name='judge_excess_anomalies_cache_judge_id_segment_id_score_type_key'),
        Index('idx_excess_anomalies_judge', 'judge_id'),
        Index('idx_excess_anomalies_segment', 'segment_id')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    judge_id: Mapped[int] = mapped_column(Integer)
    segment_id: Mapped[int] = mapped_column(Integer)
    score_type: Mapped[str] = mapped_column(String(20))
    skater_count: Mapped[int] = mapped_column(Integer)
    allowed_errors: Mapped[int] = mapped_column(Integer)
    total_anomalies: Mapped[int] = mapped_column(Integer)
    excess_anomalies: Mapped[int] = mapped_column(Integer)
    pcs_anomalies: Mapped[Optional[int]] = mapped_column(Integer, server_default=text('0'))
    element_anomalies: Mapped[Optional[int]] = mapped_column(Integer, server_default=text('0'))
    computed_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

    judge: Mapped['Judge'] = relationship('Judge', back_populates='judge_excess_anomalies_cache')
    segment: Mapped['Segment'] = relationship('Segment', back_populates='judge_excess_anomalies_cache')


class SegmentOfficial(Base):
    __tablename__ = 'segment_official'
    __table_args__ = (
        ForeignKeyConstraint(['appointment_type_id'], ['officials_analysis.appointment_types.id'], ondelete='SET NULL', name='segment_official_appointment_type_id_fkey'),
        ForeignKeyConstraint(['official_id'], ['officials_analysis.officials.id'], ondelete='SET NULL', name='segment_official_official_id_fkey'),
        ForeignKeyConstraint(['isu_official_id'], ['officials_analysis.isu_official.id'], ondelete='SET NULL', name='segment_official_isu_official_id_fkey'),
        ForeignKeyConstraint(['segment_id'], ['segment.id'], ondelete='CASCADE', name='segment_official_segment_id_fkey'),
        PrimaryKeyConstraint('id', name='segment_official_pkey'),
        UniqueConstraint('segment_id', 'role', name='segment_official_segment_role_uniq'),
        Index('ix_segment_official_appointment_type_id', 'appointment_type_id'),
        Index('ix_segment_official_official_id', 'official_id'),
        Index('ix_segment_official_isu_official_id', 'isu_official_id'),
        Index('ix_segment_official_segment_id', 'segment_id')
    )

    id: Mapped[int] = mapped_column(Integer, Identity(always=True, start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    segment_id: Mapped[int] = mapped_column(Integer)
    official_name: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(True), server_default=text('now()'))
    official_id: Mapped[Optional[int]] = mapped_column(Integer)
    isu_official_id: Mapped[Optional[int]] = mapped_column(Integer)
    appointment_type_id: Mapped[Optional[int]] = mapped_column(Integer)

    appointment_type: Mapped[Optional['AppointmentTypes']] = relationship('AppointmentTypes', back_populates='segment_official')
    official: Mapped[Optional['Officials']] = relationship('Officials', back_populates='segment_official')
    isu_official: Mapped[Optional['IsuOfficial']] = relationship(
        'IsuOfficial', back_populates='segment_official'
    )
    segment: Mapped['Segment'] = relationship('Segment', back_populates='segment_official')


class SkaterSegment(Base):
    __tablename__ = 'skater_segment'
    __table_args__ = (
        ForeignKeyConstraint(['segment_id'], ['segment.id'], ondelete='CASCADE', name='skater_segment_segment_id_fkey'),
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
        ForeignKeyConstraint(['skater_segment_id'], ['skater_segment.id'], ondelete='CASCADE', name='element_skater_segment_id_fkey'),
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
        ForeignKeyConstraint(['skater_segment_id'], ['skater_segment.id'], ondelete='CASCADE', name='pcs_score_per_judge_skater_segment_id_fkey'),
        PrimaryKeyConstraint('id', name='pcs_score_per_judge_pkey'),
        UniqueConstraint('skater_segment_id', 'pcs_type_id', 'judge_id', name='pcs_score_per_judge_unique')
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
        ForeignKeyConstraint(['element_id'], ['element.id'], ondelete='CASCADE', name='element_score_per_judge_element_id_fkey'),
        ForeignKeyConstraint(['judge_id'], ['judge.id'], name='element_score_per_judge_judge_id_fkey'),
        PrimaryKeyConstraint('id', name='element_score_per_judge_pkey'),
        UniqueConstraint('element_id', 'judge_id', name='element_score_per_judge_unique'),
        Index('element_score_per_judge_is_rule_error_idx', 'is_rule_error'),
        Index('element_score_per_judge_thrown_out_idx', 'thrown_out')
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
