-- Store full IJS index URLs on competition.results_url (append /index.asp for legacy base rows).
-- Safe to re-run: only updates rows that still lack an index filename suffix.

UPDATE public.competition
SET results_url = results_url || '/index.asp'
WHERE results_url IS NOT NULL
  AND btrim(results_url) <> ''
  AND results_url !~* '/index\.(asp|htm)$'
  AND results_url ~* '/leaderboard/results/[0-9]{4}/[0-9]+$';
