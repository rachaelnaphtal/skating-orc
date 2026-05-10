-- One row per official function on a results segment (Judge 1..n, Referee, TC, TS, …).
-- Requires public.segment and officials_analysis.officials / appointment_types.
--
-- Role → appointment_type_id (application mapping, documented in database_loader):
--   Judges: 1, Referees (incl. assistant): 4, Technical Controller: 11,
--   Technical / Assistant Technical Specialist: 9
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/002_public_segment_official.sql

CREATE TABLE IF NOT EXISTS public.segment_official (
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    segment_id integer NOT NULL,
    official_name text NOT NULL,
    official_id integer,
    role text NOT NULL,
    appointment_type_id integer,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT segment_official_pkey PRIMARY KEY (id),
    CONSTRAINT segment_official_segment_id_fkey
        FOREIGN KEY (segment_id) REFERENCES public.segment(id) ON DELETE CASCADE,
    CONSTRAINT segment_official_official_id_fkey
        FOREIGN KEY (official_id)
        REFERENCES officials_analysis.officials(id) ON DELETE SET NULL,
    CONSTRAINT segment_official_appointment_type_id_fkey
        FOREIGN KEY (appointment_type_id)
        REFERENCES officials_analysis.appointment_types(id) ON DELETE SET NULL,
    CONSTRAINT segment_official_segment_role_uniq UNIQUE (segment_id, role)
);

CREATE INDEX IF NOT EXISTS ix_segment_official_segment_id
    ON public.segment_official (segment_id);

CREATE INDEX IF NOT EXISTS ix_segment_official_official_id
    ON public.segment_official (official_id)
    WHERE official_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_segment_official_appointment_type_id
    ON public.segment_official (appointment_type_id)
    WHERE appointment_type_id IS NOT NULL;
