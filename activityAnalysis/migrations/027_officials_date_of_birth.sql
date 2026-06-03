-- USFS member birth dates (from directory / ages export), keyed by mbr_number.
--
-- DDL needs ACCESS EXCLUSIVE on officials. Close Streamlit tabs / stop Heroku dynos
-- first, or terminate stale backends (see scripts/pg_clear_idle_transactions.sql).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/027_officials_date_of_birth.sql

SET lock_timeout = '60s';

ALTER TABLE officials_analysis.officials
    ADD COLUMN IF NOT EXISTS date_of_birth date;

COMMENT ON COLUMN officials_analysis.officials.date_of_birth IS
    'Date of birth from USFS member export (DOB column); NULL when unknown.';
