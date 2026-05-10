-- Link public judging competitions to officials_analysis.competition_type (nullable).
-- Apply on the judging DB (schema public + officials_analysis present):
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/006_public_competition_officials_competition_type.sql

ALTER TABLE public.competition
    ADD COLUMN IF NOT EXISTS officials_analysis_competition_type_id integer NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'competition_officials_analysis_competition_type_id_fkey'
    ) THEN
        ALTER TABLE public.competition
            ADD CONSTRAINT competition_officials_analysis_competition_type_id_fkey
            FOREIGN KEY (officials_analysis_competition_type_id)
            REFERENCES officials_analysis.competition_type (id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_competition_officials_analysis_competition_type_id
    ON public.competition (officials_analysis_competition_type_id);

COMMENT ON COLUMN public.competition.officials_analysis_competition_type_id IS
    'Optional FK to officials_analysis.competition_type for analytics / activity alignment.';
