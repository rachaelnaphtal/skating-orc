-- Append a fetchable index file to competition.results_url when missing.
-- Re-run safe: only rows without /index.asp, /index.htm, or /index.html are updated.
--
--   USFS classic leaderboard base paths  -> /index.asp  (same rule as 011)
--   All other base paths (ISU / Swiss Timing / etc.) -> /index.htm
--
-- Run:
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/015_competition_results_url_index_htm.sql

-- USFS leaderboard rows that still lack an index suffix.
UPDATE public.competition
SET results_url = regexp_replace(btrim(results_url), '/+$', '') || '/index.asp'
WHERE results_url IS NOT NULL
  AND btrim(results_url) <> ''
  AND btrim(results_url) !~* '/index\.(asp|htm|html)$'
  AND btrim(results_url) ~* '/leaderboard/results/[0-9]{4}/[0-9]+$';

-- International / FSM / other hosted results (e.g. kraso.sk, results.isu.org paths).
UPDATE public.competition
SET results_url = regexp_replace(btrim(results_url), '/+$', '') || '/index.htm'
WHERE results_url IS NOT NULL
  AND btrim(results_url) <> ''
  AND btrim(results_url) !~* '/index\.(asp|htm|html)$'
  AND btrim(results_url) !~* '/leaderboard/results/[0-9]{4}/[0-9]+$';
