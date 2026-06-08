-- Figure skating International / ISU Referee seminar requirements (Rules 412.2 / 412.3 / 412.4).
-- Requires migration 035_isu_official_seminar.sql.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/036_figure_referee_seminar_requirements.sql

-- International Referee maintain (412.2.b): in-person seminar (4 seasons) OR online (2 seasons).
INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'seminar_alternatives',
    1,
    $json$
{
  "alternatives": [
    {
      "label": "In-person ISU Referee seminar (4 seasons)",
      "requirements": [
        {"in_person": true, "season_window": 4, "min": 1}
      ]
    },
    {
      "label": "Online ISU Referee seminar (2 seasons)",
      "requirements": [
        {"in_person": false, "season_window": 2, "min": 1}
      ]
    }
  ]
}
$json$::jsonb,
    'ISU Referee seminar: in person (4 seasons) or online (2 seasons)',
    2
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '412.2.b'
  AND rs.purpose = 'maintain'
  AND rs.appointment_type_id = 13
  AND rs.listing_tier = 'international'
  AND rs.discipline_id IS NULL
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id AND r.metric = 'seminar_alternatives'
  );

-- ISU Referee maintain (412.4.b): same seminar alternatives.
-- Note: ISU Communications also allow Initial Judges Meeting + Round Table at
-- ISU Championships / OWG / YOG (2 seasons); that activity is not automated yet.
INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'seminar_alternatives',
    1,
    $json$
{
  "alternatives": [
    {
      "label": "In-person ISU Referee seminar (4 seasons)",
      "requirements": [
        {"in_person": true, "season_window": 4, "min": 1}
      ]
    },
    {
      "label": "Online ISU Referee seminar (2 seasons)",
      "requirements": [
        {"in_person": false, "season_window": 2, "min": 1}
      ]
    }
  ]
}
$json$::jsonb,
    'ISU Referee seminar: in person (4 seasons) or online (2 seasons)',
    2
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '412.4.b'
  AND rs.purpose = 'maintain'
  AND rs.appointment_type_id = 13
  AND rs.listing_tier = 'isu'
  AND rs.discipline_id IS NULL
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id AND r.metric = 'seminar_alternatives'
  );

-- Promote to ISU Referee (412.3.c): in-person seminar during 4 preceding seasons.
INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'seminar_count',
    1,
    $json${"in_person": true, "season_window": 4}$json$::jsonb,
    'In-person ISU Referee seminar (4 seasons)',
    7
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '412.3.c'
  AND rs.purpose = 'promote'
  AND rs.appointment_type_id = 13
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id AND r.metric = 'seminar_count'
  );
