-- Clear stale open transactions before running DDL (migrations, ALTER TABLE).
-- Run in psql or DBeaver while Streamlit / batch jobs are stopped when possible.
--
--   psql "$DATABASE_URL" -f scripts/pg_clear_idle_transactions.sql
--
-- Preview who would be terminated (no changes):
--   psql "$DATABASE_URL" -c "SELECT pid, state, state_change, left(query, 80) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction' ORDER BY state_change;"

SELECT pg_terminate_backend(a.pid) AS terminated, a.pid, a.state, left(a.query, 120) AS query
FROM pg_stat_activity a
WHERE a.datname = current_database()
  AND a.pid <> pg_backend_pid()
  AND a.state = 'idle in transaction'
  AND a.state_change < now() - interval '2 minutes';
