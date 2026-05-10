-- Mark competitions that count toward National Qualifying Series (NQS) activity reporting.
-- Apply to the judging database (schema public):
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/005_public_competition_nqs.sql

ALTER TABLE public.competition
    ADD COLUMN IF NOT EXISTS nqs boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.competition.nqs IS
    'When true, competition is treated as NQS for protocol-based activity reports.';
