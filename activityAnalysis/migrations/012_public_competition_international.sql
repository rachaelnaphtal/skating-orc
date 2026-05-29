-- International (ISU / non-USFS) competitions on public.competition.
-- Apply after officials_analysis.competition_type has ``international`` (types 15–17).
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/012_public_competition_international.sql

ALTER TABLE public.competition
    ADD COLUMN IF NOT EXISTS international boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.competition.international IS
    'When true, competition is ISU / international (not US qualifying or NQS).';

UPDATE public.competition
SET international = true,
    qualifying = false,
    nqs = false
WHERE officials_analysis_competition_type_id IN (15, 16, 17);
