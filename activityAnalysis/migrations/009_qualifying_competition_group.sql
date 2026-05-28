-- Per form competition: S/P/D vs Synchronized for assignment-history lookups.
-- Apply: psql "$DATABASE_URL" -f activityAnalysis/migrations/009_qualifying_competition_group.sql

ALTER TABLE officials_analysis.qualifying_availability_competition
    ADD COLUMN IF NOT EXISTS competition_group TEXT;
