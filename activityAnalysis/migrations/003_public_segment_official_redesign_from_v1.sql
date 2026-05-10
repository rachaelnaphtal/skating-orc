-- Run once if you already created public.segment_official using the first version of 002
-- (with judge_id and appointment_id). This drops that table and recreates the current schema.
-- Backup first if you have data to preserve.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/003_public_segment_official_redesign_from_v1.sql
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/002_public_segment_official.sql

DROP TABLE IF EXISTS public.segment_official CASCADE;
