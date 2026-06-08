-- ISU seminar attendance records for international officials requirement evaluation.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/035_isu_official_seminar.sql

CREATE TABLE IF NOT EXISTS officials_analysis.isu_official_seminar (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    official_id integer NOT NULL,
    appointment_type_id integer NOT NULL,
    discipline_id integer,
    seminar_date date NOT NULL,
    season_code integer NOT NULL,
    in_person boolean NOT NULL,
    place text,
    at_event boolean NOT NULL DEFAULT false,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_modified timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT isu_official_seminar_official_id_fkey
        FOREIGN KEY (official_id)
        REFERENCES officials_analysis.officials (id),
    CONSTRAINT isu_official_seminar_appointment_type_id_fkey
        FOREIGN KEY (appointment_type_id)
        REFERENCES officials_analysis.appointment_types (id),
    CONSTRAINT isu_official_seminar_discipline_id_fkey
        FOREIGN KEY (discipline_id)
        REFERENCES officials_analysis.disciplines (id)
);

COMMENT ON TABLE officials_analysis.isu_official_seminar IS
    'ISU seminar attendance per official (appointment type, discipline, delivery mode).';

COMMENT ON COLUMN officials_analysis.isu_official_seminar.season_code IS
    'USFS season code (e.g. 2526) for the seminar; should align with seminar_date.';

COMMENT ON COLUMN officials_analysis.isu_official_seminar.in_person IS
    'True when attended in person; false for online / remote delivery.';

COMMENT ON COLUMN officials_analysis.isu_official_seminar.at_event IS
    'True when the seminar was held at a competition or ISU event; false when standalone.';

CREATE INDEX IF NOT EXISTS ix_isu_official_seminar_official_season
    ON officials_analysis.isu_official_seminar (official_id, season_code);

CREATE INDEX IF NOT EXISTS ix_isu_official_seminar_official_appt
    ON officials_analysis.isu_official_seminar (official_id, appointment_type_id, discipline_id);
