-- Segment skill level (Senior/Junior/Novice tiers, USFS Preliminary, etc.)
-- Apply: psql "$DATABASE_URL" -f activityAnalysis/migrations/016_public_segment_level.sql

ALTER TABLE public.segment
    ADD COLUMN IF NOT EXISTS level text,
    ADD COLUMN IF NOT EXISTS level_source text;

COMMENT ON COLUMN public.segment.level IS
    'Standardized skill level (Senior, Junior, Advanced Novice, Preliminary, Excel Juvenile, …).';

COMMENT ON COLUMN public.segment.level_source IS
    'How level was set: segment_token, competition_name, default_international, unspecified.';

CREATE INDEX IF NOT EXISTS ix_segment_level ON public.segment (level)
    WHERE level IS NOT NULL;
