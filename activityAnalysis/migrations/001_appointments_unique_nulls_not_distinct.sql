-- PostgreSQL 15+  Fix duplicate appointments when discipline_id or level_id is NULL.
-- Default UNIQUE allows multiple (official_id, type, NULL, level) because NULL != NULL
-- in unique checks. This replaces the constraint with NULLS NOT DISTINCT so ON CONFLICT
-- in officials_directory_loader.py matches Excel upserts to a single row.
--
-- Run once after backing up, e.g.:
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/001_appointments_unique_nulls_not_distinct.sql
--
-- Optional: run deduplication in SQL before this if you have historical duplicates, e.g. keep
-- the row with max(last_modified) per (official_id, appointment_type_id, discipline_id, level_id).

ALTER TABLE officials_analysis.appointments
    DROP CONSTRAINT IF EXISTS appointments_unique;

ALTER TABLE officials_analysis.appointments
    ADD CONSTRAINT appointments_unique
    UNIQUE NULLS NOT DISTINCT (official_id, appointment_type_id, discipline_id, level_id);
