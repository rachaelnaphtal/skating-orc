-- Figure + synchronized maintain updates: national qualifying pool and ISU TC/TS
-- OR-alternative service (Rule 414.4 / 415.4 / 861.4 style).
--
-- Requires migrations 019 (include_qualifying_national) and 020 (metric_config).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/021_synchro_intl_ts_861_2_national.sql

-- ---------------------------------------------------------------------------
-- Synchronized International TC + TS (860.2.b, 861.2.b): 2 International OR National
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET
    include_qualifying_national = true,
    display_label = 'TC or TS in ≥2 international or national qualifying competitions (3 seasons; Junior/Senior)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref IN ('860.2.b', '861.2.b')
  AND rs.sport = 'synchronized'
  AND rs.listing_tier = 'international'
  AND r.metric = 'tc_or_ts_competitions';

-- ---------------------------------------------------------------------------
-- Figure International TC + TS maintain (414.2.b, 415.2.b): intl OR national pool
-- (414.2.b may already be set by 019; ensure 415.2.b and labels are correct.)
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET
    include_qualifying_national = true,
    display_label = 'TC or TS in ≥2 international or national qualifying competitions (3 seasons; Junior/Senior; discipline)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref IN ('414.2.b', '415.2.b')
  AND rs.sport = 'figure'
  AND rs.listing_tier = 'international'
  AND r.metric = 'tc_or_ts_competitions';

-- ---------------------------------------------------------------------------
-- Figure ISU TC + TS maintain (414.4.b, 415.4.b):
-- 2 International Competitions OR (1 ISU Event + 1 National Competition)
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET
    metric = 'competition_alternatives',
    min_value = 1,
    include_qualifying_national = false,
    display_label = 'TC or TS: 2 International OR 1 ISU Event + 1 National (3 seasons; discipline)',
    metric_config = $json$
{
  "alternatives": [
    {
      "label": "2 International Competitions",
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
  AND rs.isu_rule_ref IN ('414.4.b', '415.4.b')
  AND rs.sport = 'figure'
  AND rs.listing_tier = 'isu'
  AND r.metric IN ('tc_or_ts_competitions', 'competition_alternatives');

-- ---------------------------------------------------------------------------
-- Figure maintain labels (412 / 413): clarify discipline-scoped service
-- ---------------------------------------------------------------------------
UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Referee, Judge, or TC in ≥1 international competition in discipline (3 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '412.2.b'
  AND rs.sport = 'figure'
  AND r.metric = 'combined_roles_competitions';

UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Referee, Judge, or TC in ≥1 international competition in discipline (3 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '413.2.b'
  AND rs.sport = 'figure'
  AND r.metric = 'combined_roles_competitions';

UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Referee, Judge, Trial Judge, or TC in ≥1 international competition in discipline (3 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref IN ('412.4.b', '413.4.b')
  AND rs.sport = 'figure'
  AND rs.listing_tier = 'isu'
  AND r.metric = 'combined_roles_competitions';
