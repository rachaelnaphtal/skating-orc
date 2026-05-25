-- Element judge calibration: scenario benchmarks + judge z-scores
--
-- Compares each judge's element deviations to global expectations for the same
-- discipline, element type, GOE given, and panel spread (with coarser fallbacks).
--
-- Run:
--   psql "$DATABASE_URL" -f scripts/element_judge_calibration.sql
--
-- Or paste the whole file into DBeaver / pgAdmin and execute.
--
-- Edit the params block and (optionally) the seasons list below, then run the
-- entire script in one go.
--
-- Time filters (combine as needed):
--   • _calibration_seasons — INSERT one row per competition.year (e.g. 2425, 2526).
--     Leave the table empty (no INSERTs) to include all seasons.
--   • start_date_filter / end_date_filter — filter by competition event date
--     COALESCE(start_date, end_date). NULL = no bound on that side.
--     Competitions with both dates NULL are excluded when either date filter is set.
--   • competition_scope — linked officials competition type (same as Cross-Judge app):
--       all                          — no type filter
--       sectionals_and_championships — SPD/SYS sectionals + US / Synchro Championships (types 1–9)
--       championships_only           — US Championships + US Synchro Championships (types 4, 8)
--       qualifying                   — any linked type except nonqualifying (11)
--     Or INSERT into _calibration_competition_types to pick exact type ids (overrides scope).
--
-- Requires competitions to have officials_analysis_competition_type_id set when using
-- a scope other than 'all' (set at load time in the app or Admin).
--
-- Outputs:
--   • Temp tables you can inspect: _element_scenario_benchmarks, _element_mark_scored
--   • Final result set: judge rankings (mean_abs_z ASC = closer to global norms)

-- =============================================================================
-- 0) Parameters — edit here
-- =============================================================================
DROP TABLE IF EXISTS _calibration_params;
CREATE TEMP TABLE _calibration_params AS
SELECT
    'Singles'::text AS discipline_filter,   -- NULL = all disciplines
    'sectionals_and_championships'::text AS competition_scope,
    '2018-07-01'::date AS start_date_filter,        -- e.g. DATE '2024-07-01', or NULL
    NULL::date AS end_date_filter,          -- e.g. DATE '2025-06-30', or NULL
    3 AS min_panel_size,
    1 AS min_spread,                       -- exclude unanimous panels (spread 0)
    30 AS min_bucket_marks,                -- min marks to use a benchmark bucket
    200 AS min_judge_marks;                -- min marks to include a judge in ranks

-- Season list: competition.year values (text, e.g. 2223, 2425, 2526).
-- Leave empty (skip INSERT) to include all seasons.
DROP TABLE IF EXISTS _calibration_seasons;
CREATE TEMP TABLE _calibration_seasons (
    season_year text PRIMARY KEY
);

-- Optional: exact officials_analysis.competition_type ids (overrides competition_scope).
-- Type ids: 1–3 SPD sectionals, 4 US Championships, 5–7/9 SYS sectionals, 8 US Synchro Champs,
-- 10 NQS, 11 nonqualifying, 12–14 adult/collegiate.
DROP TABLE IF EXISTS _calibration_competition_types;
CREATE TEMP TABLE _calibration_competition_types (
    type_id integer PRIMARY KEY
);

-- Uncomment / edit to restrict seasons (multiple rows OK):
-- INSERT INTO _calibration_seasons (season_year) VALUES
--     ('2223'),
--     ('2324'),
--     ('2425'),
--     ('2526');

-- Or pick explicit competition types (leave empty to use competition_scope above):
-- INSERT INTO _calibration_competition_types (type_id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9);

-- =============================================================================
-- 1) Base marks (one row per judge mark on an element)
-- =============================================================================
DROP TABLE IF EXISTS _element_mark_base;
CREATE TEMP TABLE _element_mark_base AS
WITH panel AS (
    SELECT
        es.element_id,
        MAX(es.judge_score) - MIN(es.judge_score) AS goe_range,
        COUNT(*) AS panel_size
    FROM public.element_score_per_judge es
    GROUP BY es.element_id
    HAVING COUNT(*) >= (SELECT min_panel_size FROM _calibration_params LIMIT 1)
)
SELECT
    es.id AS mark_id,
    es.judge_id,
    es.judge_score::int AS judge_goe,
    es.deviation::double precision AS deviation,
    es.thrown_out,
    dt.name AS discipline,
    COALESCE(et.name, e.element_type) AS element_type,
    pan.goe_range AS panel_spread,
    CASE
        WHEN pan.goe_range = 1 THEN '1'
        WHEN pan.goe_range = 2 THEN '2'
        WHEN pan.goe_range = 3 THEN '3'
        ELSE '4+'
    END AS spread_bucket,
    c.year AS competition_year,
    c.officials_analysis_competition_type_id AS competition_type_id,
    COALESCE(c.start_date, c.end_date) AS competition_event_date
FROM public.element_score_per_judge es
JOIN public.element e ON e.id = es.element_id
JOIN public.skater_segment ss ON ss.id = e.skater_segment_id
JOIN public.segment s ON s.id = ss.segment_id
JOIN public.discipline_type dt ON dt.id = s.discipline_type_id
LEFT JOIN public.element_type et ON et.id = e.element_type_id
JOIN public.competition c ON c.id = s.competition_id
JOIN panel pan ON pan.element_id = e.id
WHERE es.is_rule_error IS NOT TRUE
  AND pan.goe_range >= (SELECT min_spread FROM _calibration_params LIMIT 1)
  AND (
      (SELECT discipline_filter FROM _calibration_params LIMIT 1) IS NULL
      OR dt.name = (SELECT discipline_filter FROM _calibration_params LIMIT 1)
  )
  AND (
      NOT EXISTS (SELECT 1 FROM _calibration_seasons)
      OR c.year IN (SELECT season_year FROM _calibration_seasons)
  )
  AND (
      (
          (SELECT start_date_filter FROM _calibration_params LIMIT 1) IS NULL
          AND (SELECT end_date_filter FROM _calibration_params LIMIT 1) IS NULL
      )
      OR COALESCE(c.start_date, c.end_date) IS NOT NULL
  )
  AND (
      (SELECT start_date_filter FROM _calibration_params LIMIT 1) IS NULL
      OR COALESCE(c.start_date, c.end_date)
          >= (SELECT start_date_filter FROM _calibration_params LIMIT 1)
  )
  AND (
      (SELECT end_date_filter FROM _calibration_params LIMIT 1) IS NULL
      OR COALESCE(c.end_date, c.start_date)
          <= (SELECT end_date_filter FROM _calibration_params LIMIT 1)
  )
  AND (
      (
          EXISTS (SELECT 1 FROM _calibration_competition_types)
          AND c.officials_analysis_competition_type_id IN (
              SELECT type_id FROM _calibration_competition_types
          )
      )
      OR (
          NOT EXISTS (SELECT 1 FROM _calibration_competition_types)
          AND (
              (SELECT competition_scope FROM _calibration_params LIMIT 1) = 'all'
              OR (
                  (SELECT competition_scope FROM _calibration_params LIMIT 1)
                      = 'sectionals_and_championships'
                  AND c.officials_analysis_competition_type_id IN (
                      1, 2, 3, 4, 5, 6, 7, 8, 9
                  )
              )
              OR (
                  (SELECT competition_scope FROM _calibration_params LIMIT 1)
                      = 'championships_only'
                  AND c.officials_analysis_competition_type_id IN (4, 8)
              )
              OR (
                  (SELECT competition_scope FROM _calibration_params LIMIT 1) = 'qualifying'
                  AND c.officials_analysis_competition_type_id IS NOT NULL
                  AND c.officials_analysis_competition_type_id <> 11
              )
          )
      )
  );

CREATE INDEX ON _element_mark_base (discipline, element_type, judge_goe, spread_bucket);
CREATE INDEX ON _element_mark_base (judge_id);

-- =============================================================================
-- 2) Scenario benchmarks (three fallback levels)
--    Level 1: discipline + element_type + judge_goe + spread
--    Level 2: discipline + element_type + judge_goe
--    Level 3: discipline + element_type
-- =============================================================================
DROP TABLE IF EXISTS _element_scenario_benchmarks;
CREATE TEMP TABLE _element_scenario_benchmarks AS
SELECT
    1 AS bench_level,
    discipline,
    element_type,
    judge_goe,
    spread_bucket,
    COUNT(*) AS n_marks,
    AVG(deviation) AS mu_deviation,
    STDDEV(deviation) AS sigma_deviation,
    AVG(ABS(deviation)) AS mu_abs_deviation
FROM _element_mark_base
GROUP BY discipline, element_type, judge_goe, spread_bucket

UNION ALL

SELECT
    2 AS bench_level,
    discipline,
    element_type,
    judge_goe,
    NULL::text AS spread_bucket,
    COUNT(*) AS n_marks,
    AVG(deviation) AS mu_deviation,
    STDDEV(deviation) AS sigma_deviation,
    AVG(ABS(deviation)) AS mu_abs_deviation
FROM _element_mark_base
GROUP BY discipline, element_type, judge_goe

UNION ALL

SELECT
    3 AS bench_level,
    discipline,
    element_type,
    NULL::int AS judge_goe,
    NULL::text AS spread_bucket,
    COUNT(*) AS n_marks,
    AVG(deviation) AS mu_deviation,
    STDDEV(deviation) AS sigma_deviation,
    AVG(ABS(deviation)) AS mu_abs_deviation
FROM _element_mark_base
GROUP BY discipline, element_type;

CREATE INDEX ON _element_scenario_benchmarks (bench_level, discipline, element_type);

-- =============================================================================
-- 3) Judge identity (merge directory-linked aliases)
-- =============================================================================
DROP TABLE IF EXISTS _judge_identity;
CREATE TEMP TABLE _judge_identity AS
SELECT
    j.id AS judge_id,
    CASE
        WHEN jol.status = 'linked' AND jol.official_id IS NOT NULL
            THEN 'official:' || jol.official_id::text
        ELSE 'judge:' || j.id::text
    END AS identity_key,
    COALESCE(o.full_name, j.name) AS display_name
FROM public.judge j
LEFT JOIN public.judge_official_link jol ON jol.judge_id = j.id
LEFT JOIN officials_analysis.officials o
    ON o.id = jol.official_id
   AND jol.status = 'linked';

DROP TABLE IF EXISTS _judge_identity_display;
CREATE TEMP TABLE _judge_identity_display AS
SELECT identity_key, MIN(display_name) AS display_name
FROM _judge_identity
GROUP BY identity_key;

-- =============================================================================
-- 4) Score each mark (finest benchmark with enough sample)
-- =============================================================================
DROP TABLE IF EXISTS _element_mark_scored;
CREATE TEMP TABLE _element_mark_scored AS
SELECT
    m.mark_id,
    ji.identity_key,
    m.discipline,
    m.element_type,
    m.judge_goe,
    m.spread_bucket,
    m.deviation,
    b.bench_level,
    b.mu_deviation,
    b.sigma_deviation,
    CASE
        WHEN b.sigma_deviation IS NULL OR b.sigma_deviation = 0 THEN NULL
        ELSE (m.deviation - b.mu_deviation) / b.sigma_deviation
    END AS z_score
FROM _element_mark_base m
JOIN _judge_identity ji ON ji.judge_id = m.judge_id
LEFT JOIN LATERAL (
    SELECT b1.*
    FROM _element_scenario_benchmarks b1
    WHERE b1.bench_level = 1
      AND b1.discipline = m.discipline
      AND b1.element_type = m.element_type
      AND b1.judge_goe = m.judge_goe
      AND b1.spread_bucket = m.spread_bucket
      AND b1.n_marks >= (SELECT min_bucket_marks FROM _calibration_params LIMIT 1)
      AND b1.sigma_deviation IS NOT NULL
      AND b1.sigma_deviation > 0
    UNION ALL
    SELECT b2.*
    FROM _element_scenario_benchmarks b2
    WHERE b2.bench_level = 2
      AND b2.discipline = m.discipline
      AND b2.element_type = m.element_type
      AND b2.judge_goe = m.judge_goe
      AND b2.n_marks >= (SELECT min_bucket_marks FROM _calibration_params LIMIT 1)
      AND b2.sigma_deviation IS NOT NULL
      AND b2.sigma_deviation > 0
    UNION ALL
    SELECT b3.*
    FROM _element_scenario_benchmarks b3
    WHERE b3.bench_level = 3
      AND b3.discipline = m.discipline
      AND b3.element_type = m.element_type
      AND b3.n_marks >= (SELECT min_bucket_marks FROM _calibration_params LIMIT 1)
      AND b3.sigma_deviation IS NOT NULL
      AND b3.sigma_deviation > 0
    ORDER BY bench_level
    LIMIT 1
) b ON TRUE
WHERE b.bench_level IS NOT NULL;

CREATE INDEX ON _element_mark_scored (identity_key);

-- =============================================================================
-- Optional inspect: scenario benchmarks (level 1, highest volume)
-- =============================================================================
SELECT
    'benchmarks_l1_sample' AS section,
    bench_level,
    discipline,
    element_type,
    judge_goe,
    spread_bucket,
    n_marks,
    ROUND(mu_deviation::numeric, 4) AS mu_deviation,
    ROUND(sigma_deviation::numeric, 4) AS sigma_deviation,
    ROUND(mu_abs_deviation::numeric, 4) AS mu_abs_deviation
FROM _element_scenario_benchmarks
WHERE bench_level = 1
ORDER BY n_marks DESC
LIMIT 40;

-- Coverage summary
SELECT
    'coverage' AS section,
    (SELECT competition_scope FROM _calibration_params LIMIT 1) AS competition_scope,
    (SELECT COUNT(*) FROM _calibration_competition_types) AS competition_type_override_count,
    (SELECT COUNT(*) FROM _calibration_seasons) AS seasons_filter_count,
    (SELECT start_date_filter FROM _calibration_params LIMIT 1) AS start_date_filter,
    (SELECT end_date_filter FROM _calibration_params LIMIT 1) AS end_date_filter,
    (SELECT COUNT(*) FROM _element_mark_base) AS marks_in_base,
    (SELECT COUNT(*) FROM _element_mark_scored) AS marks_with_z,
    (SELECT COUNT(*) FROM _element_mark_base)
        - (SELECT COUNT(*) FROM _element_mark_scored) AS marks_no_benchmark;

-- =============================================================================
-- 5) Judge rankings (main result)
-- =============================================================================
SELECT
    'judge_ranks' AS section,
    d.display_name AS judge_name,
    s.discipline,
    COUNT(*) AS n_marks,
    ROUND(AVG(s.z_score)::numeric, 4) AS mean_z,
    ROUND(AVG(ABS(s.z_score))::numeric, 4) AS mean_abs_z,
    ROUND(SQRT(AVG(s.z_score * s.z_score))::numeric, 4) AS rmse_z,
    ROUND(AVG(s.deviation)::numeric, 4) AS mean_deviation,
    ROUND(AVG(ABS(s.deviation))::numeric, 4) AS mean_abs_deviation,
    ROUND(
        100.0 * AVG(CASE WHEN s.bench_level = 1 THEN 1.0 ELSE 0.0 END)::numeric,
        1
    ) AS pct_finest_benchmark
FROM _element_mark_scored s
JOIN _judge_identity_display d ON d.identity_key = s.identity_key
GROUP BY d.display_name, s.discipline
HAVING COUNT(*) >= (SELECT min_judge_marks FROM _calibration_params LIMIT 1)
ORDER BY mean_abs_z ASC, n_marks DESC;

-- =============================================================================
-- Ad-hoc follow-ups (run separately in same session if temp tables still exist)
-- =============================================================================
-- All benchmarks:
--   SELECT * FROM _element_scenario_benchmarks ORDER BY bench_level, n_marks DESC;
--
-- One judge's scored marks:
--   SELECT * FROM _element_mark_scored s
--   JOIN _judge_identity_display d ON d.identity_key = s.identity_key
--   WHERE d.display_name ILIKE '%Smith%'
--   LIMIT 100;
