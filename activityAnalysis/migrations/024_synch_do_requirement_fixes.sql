-- Synchronized skating + Data/Video Operator requirement corrections.
-- International = competition types 15, 16, and 17 (ISU Championship, ISU Competition,
-- International Competition).
--
-- Requires migrations 017–023.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/024_synch_do_requirement_fixes.sql

-- ---------------------------------------------------------------------------
-- Data Operator — International maintain (416.2.b figure, 862.2.b synchronized):
-- 2 International or National competitions, 3 seasons
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET
    include_qualifying_national = true,
    display_label = 'Data/Video Operator in ≥2 international or national qualifying competitions (3 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref IN ('416.2.b', '862.2.b')
  AND rs.appointment_type_id = 16
  AND rs.listing_tier = 'international'
  AND r.metric = 'data_operator_competitions';

-- ---------------------------------------------------------------------------
-- Data Operator — ISU promotion (416.3.c figure, 862.3.c synchronized):
-- any national or international competition, 2 seasons
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT v.isu_rule_ref, v.purpose, v.label, 16, v.listing_tier, v.season_window, v.sport, v.sort_order
FROM (
    VALUES
        ('416.3.c', 'promote', 'Data / Video Operator — promote to ISU (figure)', 'international', 2, 'figure', 185),
        ('862.3.c', 'promote', 'Data / Video Operator — promote to ISU (synchronized)', 'international', 2, 'synchronized', 315)
) AS v(isu_rule_ref, purpose, label, listing_tier, season_window, sport, sort_order)
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set rs
    WHERE rs.isu_rule_ref = v.isu_rule_ref
      AND rs.purpose = v.purpose
      AND rs.appointment_type_id = 16
      AND rs.sport = v.sport
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, include_qualifying_national,
    display_label, sort_order
)
SELECT
    rs.id,
    'data_operator_competitions',
    1,
    ARRAY[8],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    true,
    'Data/Video Operator in ≥1 national or international competition (2 seasons)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref IN ('416.3.c', '862.3.c')
  AND rs.purpose = 'promote'
  AND rs.appointment_type_id = 16
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- ---------------------------------------------------------------------------
-- Data Operator — ISU maintain (416.4.b figure, 862.4.b synchronized):
-- 2 International OR (1 ISU Event + 1 National), 3 seasons
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET
    metric = 'competition_alternatives',
    min_value = 1,
    include_qualifying_national = false,
    display_label = 'Data/Video Operator: 2 International OR 1 ISU Event + 1 National (3 seasons)',
    metric_config = $json$
{
  "alternatives": [
    {
      "label": "2 International",
      "requirements": [
        {"scope": "international_all", "min": 2}
      ]
    },
    {
      "label": "1 ISU Event + 1 National",
      "requirements": [
        {"scope": "isu_event", "min": 1},
        {"scope": "national_qualifying", "min": 1}
      ]
    }
  ]
}
$json$::jsonb
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref IN ('416.4.b', '862.4.b')
  AND rs.appointment_type_id = 16
  AND rs.listing_tier = 'isu'
  AND r.metric = 'data_operator_competitions';

-- ---------------------------------------------------------------------------
-- Synchronized ISU Referee promote (828.3.c):
-- Ref ≥2 International; Judge OR TC ≥3 International incl. ≥1 ISU Championship
-- ---------------------------------------------------------------------------
DELETE FROM officials_analysis.international_requirement_rule r
USING officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '828.3.c'
  AND rs.sport = 'synchronized'
  AND r.metric IN (
      'seasons_since_appointed',
      'judge_competitions',
      'judge_championship_or_olympic'
  );

UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Referee in ≥2 international competitions (4 seasons; types 15–17)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '828.3.c'
  AND rs.sport = 'synchronized'
  AND r.metric = 'referee_competitions';

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'competition_alternatives',
    1,
    ARRAY[1, 11],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    $json$
{
  "alternatives": [
    {
      "label": "Judge",
      "role_ids": [1],
      "requirements": [
        {"scope": "international_all", "min": 3},
        {"scope": "isu_championship", "min": 1}
      ]
    },
    {
      "label": "TC (Technical Committee members)",
      "role_ids": [11],
      "requirements": [
        {"scope": "international_all", "min": 3},
        {"scope": "isu_championship", "min": 1}
      ]
    }
  ]
}
$json$::jsonb,
    'Judge or TC: ≥3 International incl. ≥1 ISU Championship (4 seasons)',
    3
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '828.3.c'
  AND rs.sport = 'synchronized'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
        AND r.metric = 'competition_alternatives'
  );

-- ---------------------------------------------------------------------------
-- Synchronized ISU Judge promote (829.3.c): ≥3 International incl. Senior + Junior
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT
    '829.3.c',
    'promote',
    'Synchronized — ISU Judge promote',
    12,
    2,
    'international',
    4,
    'synchronized',
    245
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '829.3.c'
      AND purpose = 'promote'
      AND appointment_type_id = 12
      AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, include_qualifying_national,
    metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'judge_promote_isu',
    1,
    ARRAY[1],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    false,
    $json$
{
  "min_competitions": 3,
  "required": [
    {"kind": "segment_level", "level": "Senior", "min_competitions": 1},
    {"kind": "segment_level", "level": "Junior", "min_competitions": 1}
  ]
}
$json$::jsonb,
    'Judge in ≥3 international competitions incl. Senior and Junior (4 seasons)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '829.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- ---------------------------------------------------------------------------
-- Synchronized ISU TC promote (860.3.c) and TS promote (861.3.c)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT v.isu_rule_ref, v.purpose, v.label, v.appointment_type_id, 2,
    v.listing_tier, v.season_window, v.sport, v.sort_order
FROM (
    VALUES
        ('860.3.c', 'promote', 'Synchronized — ISU TC promote', 15, 'international', 4, 'synchronized', 265),
        ('861.3.c', 'promote', 'Synchronized — ISU TS promote', 14, 'international', 4, 'synchronized', 285)
) AS v(isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order)
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set rs
    WHERE rs.isu_rule_ref = v.isu_rule_ref
      AND rs.purpose = v.purpose
      AND rs.appointment_type_id = v.appointment_type_id
      AND rs.sport = v.sport
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, include_qualifying_national,
    metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'tc_ts_promote_isu',
    1,
    CASE WHEN rs.appointment_type_id = 15 THEN ARRAY[11] ELSE ARRAY[9] END,
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    true,
    $json${"min_competitions": 3, "min_international_competition": 1}$json$::jsonb,
    CASE
        WHEN rs.appointment_type_id = 15 THEN
            'TC in ≥3 competitions incl. ≥1 International Competition (4 seasons; types 15–17)'
        ELSE
            'TS in ≥3 competitions incl. ≥1 International Competition (4 seasons; types 15–17)'
    END,
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref IN ('860.3.c', '861.3.c')
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- ---------------------------------------------------------------------------
-- Synchronized ISU TC maintain (860.4.b):
-- 2 International OR (1 International Competition + 1 National), 3 seasons
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET
    metric = 'competition_alternatives',
    min_value = 1,
    include_qualifying_national = false,
    display_label = 'TC or TS: 2 International OR 1 International + 1 National (3 seasons)',
    metric_config = $json$
{
  "alternatives": [
    {
      "label": "2 International",
      "requirements": [
        {"scope": "international_all", "min": 2}
      ]
    },
    {
      "label": "1 International + 1 National",
      "requirements": [
        {"scope": "international_competition", "min": 1},
        {"scope": "national_qualifying", "min": 1}
      ]
    }
  ]
}
$json$::jsonb
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '860.4.b'
  AND rs.sport = 'synchronized'
  AND rs.listing_tier = 'isu'
  AND r.metric IN ('tc_or_ts_competitions', 'competition_alternatives');

-- ---------------------------------------------------------------------------
-- Synchronized ISU TS maintain (861.4.b): ensure first branch uses international_all
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET
    display_label = 'TC or TS: 2 International OR 1 ISU Event OR 1 International + 1 National (3 seasons)',
    metric_config = jsonb_set(
        r.metric_config,
        '{alternatives,0,requirements,0,scope}',
        '"international_all"'::jsonb
    )
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '861.4.b'
  AND rs.sport = 'synchronized'
  AND rs.listing_tier = 'isu'
  AND r.metric = 'competition_alternatives';
