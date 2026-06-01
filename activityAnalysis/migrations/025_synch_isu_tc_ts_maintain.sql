-- Synchronized ISU TC/TS maintain (860.4.b, 861.4.b):
--   • 2 International (types 15–17), OR
--   • 1 ISU Event + 1 other International, OR
--   • 1 ISU Event + 1 National
--
-- International = ISU Championship, ISU Competition, and International Competition.
-- ISU-appointed panels (Challenger Series, ISU Championships) are counted via those types.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/025_synch_isu_tc_ts_maintain.sql

UPDATE officials_analysis.international_requirement_rule r
SET
    metric = 'competition_alternatives',
    min_value = 1,
    include_qualifying_national = false,
    display_label = 'TC or TS: 2 International OR 1 ISU Event + 1 International/National (3 seasons)',
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
      "label": "1 ISU Event + 1 International",
      "requirements": [
        {"scope": "isu_event", "min": 1},
        {"scope": "international_all", "min": 1}
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
  AND rs.isu_rule_ref IN ('860.4.b', '861.4.b')
  AND rs.sport = 'synchronized'
  AND rs.listing_tier = 'isu';

-- Synchronized ISU Referee promote: clarify International includes ISU/Challenger-type events
UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Judge or TC: ≥3 International (Challenger/ISU Championship) incl. ≥1 ISU Championship (4 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '828.3.c'
  AND rs.sport = 'synchronized'
  AND r.metric = 'competition_alternatives';
