-- ISU Communication official roster (separate from USFS directory).
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/013_isu_official_directory.sql

CREATE TABLE IF NOT EXISTS officials_analysis.isu_official (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    federation_code text NOT NULL,
    full_name text NOT NULL,
    first_name text,
    last_name text,
    name_normalized text NOT NULL,
    season text NOT NULL,
    communication_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_modified timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT isu_official_roster_unique
        UNIQUE (federation_code, name_normalized, season)
);

CREATE INDEX IF NOT EXISTS ix_isu_official_season
    ON officials_analysis.isu_official (season);

CREATE INDEX IF NOT EXISTS ix_isu_official_name_normalized
    ON officials_analysis.isu_official (name_normalized);

COMMENT ON TABLE officials_analysis.isu_official IS
    'ISU Communication lists of officials (separate from USFS officials_analysis.officials).';

-- Protocol / scrape spelling → ISU roster row (same normalization as US official_name_alias).
CREATE TABLE IF NOT EXISTS public.isu_official_name_alias (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alias_normalized text NOT NULL,
    isu_official_id integer NOT NULL,
    note text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT isu_official_name_alias_alias_normalized_key UNIQUE (alias_normalized),
    CONSTRAINT isu_official_name_alias_isu_official_id_fkey
        FOREIGN KEY (isu_official_id)
        REFERENCES officials_analysis.isu_official (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_isu_official_name_alias_isu_official_id
    ON public.isu_official_name_alias (isu_official_id);

-- Optional judge ↔ ISU roster link (US link remains in judge_official_link).
CREATE TABLE IF NOT EXISTS public.judge_isu_official_link (
    judge_id integer NOT NULL PRIMARY KEY
        REFERENCES public.judge (id) ON DELETE CASCADE,
    isu_official_id integer NOT NULL
        REFERENCES officials_analysis.isu_official (id) ON DELETE CASCADE,
    note text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_judge_isu_official_link_isu_official_id
    ON public.judge_isu_official_link (isu_official_id);

ALTER TABLE public.segment_official
    ADD COLUMN IF NOT EXISTS isu_official_id integer;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'segment_official_isu_official_id_fkey'
    ) THEN
        ALTER TABLE public.segment_official
            ADD CONSTRAINT segment_official_isu_official_id_fkey
            FOREIGN KEY (isu_official_id)
            REFERENCES officials_analysis.isu_official (id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_segment_official_isu_official_id
    ON public.segment_official (isu_official_id)
    WHERE isu_official_id IS NOT NULL;
