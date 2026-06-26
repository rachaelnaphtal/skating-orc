-- PCS fall rule errors: examples and ad-hoc checks
--
-- Rules (program component scores):
--   1 fall in the program  -> no PCS component may exceed 9.5
--   2+ falls in the program -> no PCS component may exceed 8.75
--
-- Fall count from element.notes:
--   Fx (case-insensitive) = 2 falls on that element
--   F without x           = 1 fall on that element
--
-- Applies to singles, pairs, ice dance, solo dance, and synchronized skating,
-- for competitions with season year >= 2425 (2024-25 onward).
--
-- Run:
--   psql "$DATABASE_URL" -f scripts/pcs_fall_rule_errors_examples.sql

WITH element_falls AS (
    SELECT
        e.skater_segment_id,
        SUM(
            CASE
                WHEN e.notes ~* 'Fx' THEN 2
                WHEN e.notes LIKE '%F%' THEN 1
                ELSE 0
            END
        ) AS program_fall_count
    FROM element e
    GROUP BY e.skater_segment_id
),
fall_caps AS (
    SELECT
        skater_segment_id,
        program_fall_count,
        CASE
            WHEN program_fall_count = 1 THEN 9.5
            WHEN program_fall_count >= 2 THEN 8.75
        END AS max_pcs_allowed
    FROM element_falls
    WHERE program_fall_count >= 1
),
violations AS (
    SELECT
        c.id AS competition_id,
        c.name AS competition_name,
        c.year AS competition_year,
        s.name AS segment_name,
        dt.name AS discipline_name,
        sk.name AS skater_name,
        j.name AS judge_name,
        pt.name AS pcs_component,
        fc.program_fall_count,
        fc.max_pcs_allowed,
        p.judge_score,
        p.panel_average,
        p.deviation,
        p.is_rule_error AS flagged_in_db
    FROM pcs_score_per_judge p
    JOIN fall_caps fc
        ON fc.skater_segment_id = p.skater_segment_id
    JOIN skater_segment ss ON ss.id = p.skater_segment_id
    JOIN segment s ON s.id = ss.segment_id
    JOIN competition c ON c.id = s.competition_id
    JOIN discipline_type dt ON dt.id = s.discipline_type_id
    JOIN skater sk ON sk.id = ss.skater_id
    JOIN judge j ON j.id = p.judge_id
    JOIN pcs_type pt ON pt.id = p.pcs_type_id
    WHERE p.judge_score > fc.max_pcs_allowed
      AND dt.id IN (1, 2, 3, 4, 5)  -- singles, pairs, ice dance, solo dance, synchronized
      AND TRIM(c.year) >= '2425'
)
SELECT *
FROM violations
ORDER BY competition_year DESC, competition_name, segment_name, skater_name, pcs_component, judge_name
LIMIT 100;

-- Summary: how many PCS fall violations per judge (same filters as above)
WITH element_falls AS (
    SELECT
        e.skater_segment_id,
        SUM(
            CASE
                WHEN e.notes ~* 'Fx' THEN 2
                WHEN e.notes LIKE '%F%' THEN 1
                ELSE 0
            END
        ) AS program_fall_count
    FROM element e
    GROUP BY e.skater_segment_id
),
fall_caps AS (
    SELECT
        skater_segment_id,
        program_fall_count,
        CASE
            WHEN program_fall_count = 1 THEN 9.5
            WHEN program_fall_count >= 2 THEN 8.75
        END AS max_pcs_allowed
    FROM element_falls
    WHERE program_fall_count >= 1
)
SELECT
    j.name AS judge_name,
    COUNT(*) AS pcs_fall_violations,
    SUM(CASE WHEN p.is_rule_error THEN 1 ELSE 0 END) AS flagged_in_db
FROM pcs_score_per_judge p
JOIN fall_caps fc ON fc.skater_segment_id = p.skater_segment_id
JOIN skater_segment ss ON ss.id = p.skater_segment_id
JOIN segment s ON s.id = ss.segment_id
JOIN competition c ON c.id = s.competition_id
JOIN discipline_type dt ON dt.id = s.discipline_type_id
JOIN judge j ON j.id = p.judge_id
WHERE p.judge_score > fc.max_pcs_allowed
  AND dt.id IN (1, 2, 3, 4, 5)
  AND TRIM(c.year) >= '2425'
GROUP BY j.name
ORDER BY pcs_fall_violations DESC
LIMIT 50;

-- Programs with falls and their element notes (spot-check fall counting, 2425+ only)
SELECT
    c.name AS competition_name,
    c.year AS competition_year,
    s.name AS segment_name,
    dt.name AS discipline_name,
    sk.name AS skater_name,
    e.name AS element_name,
    e.notes,
    CASE
        WHEN e.notes ~* 'Fx' THEN 2
        WHEN e.notes LIKE '%F%' THEN 1
        ELSE 0
    END AS element_fall_count
FROM element e
JOIN skater_segment ss ON ss.id = e.skater_segment_id
JOIN segment s ON s.id = ss.segment_id
JOIN competition c ON c.id = s.competition_id
JOIN discipline_type dt ON dt.id = s.discipline_type_id
JOIN skater sk ON sk.id = ss.skater_id
WHERE e.notes IS NOT NULL
  AND (e.notes ~* 'Fx' OR e.notes LIKE '%F%')
  AND dt.id IN (1, 2, 3, 4, 5)
  AND TRIM(c.year) >= '2425'
ORDER BY c.start_date DESC NULLS LAST, c.name, s.name, sk.name, e.id
LIMIT 50;
