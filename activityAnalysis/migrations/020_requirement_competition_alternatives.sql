-- OR-alternative competition service requirements (e.g. SYS Rule 861.4.b).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/020_requirement_competition_alternatives.sql

ALTER TABLE officials_analysis.international_requirement_rule
    ADD COLUMN IF NOT EXISTS metric_config jsonb;

COMMENT ON COLUMN officials_analysis.international_requirement_rule.metric_config IS
    'JSON config for metrics such as competition_alternatives (OR branches with AND clauses).';

-- Synchronized ISU Technical Specialist maintain (861.4.b):
-- 2 International Competitions OR 1 ISU Event OR (1 International + 1 National).
UPDATE officials_analysis.international_requirement_rule r
SET
    metric = 'competition_alternatives',
    min_value = 1,
    include_qualifying_national = false,
    display_label = 'TC or TS: 2 International OR 1 ISU Event OR 1 International + 1 National (3 seasons)',
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
      "label": "1 ISU Event",
      "requirements": [
        {"scope": "isu_event", "min": 1}
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
  AND rs.isu_rule_ref = '861.4.b'
  AND rs.sport = 'synchronized'
  AND r.metric = 'tc_or_ts_competitions';
