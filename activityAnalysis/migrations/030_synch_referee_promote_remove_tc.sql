-- Synchronized ISU Referee promote (828.3.c): drop TC panel-role alternative.
-- Service may be satisfied as Competition Judge only (protocol role 1), not Technical Controller.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/030_synch_referee_promote_remove_tc.sql

UPDATE officials_analysis.international_requirement_rule r
SET
    role_appointment_type_ids = ARRAY[1],
    display_label = 'Judge: ≥3 International (Challenger/ISU Championship) incl. ≥1 ISU Championship (4 seasons)',
    metric_config = $json$
{
  "alternatives": [
    {
      "label": "Judge",
      "role_ids": [1],
      "requirements": [
        {"scope": "international_all", "min": 3},
        {"scope": "isu_championship", "min": 1}
      ]
    }
  ]
}
$json$::jsonb
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '828.3.c'
  AND rs.sport = 'synchronized'
  AND rs.purpose = 'promote'
  AND r.metric = 'competition_alternatives';
