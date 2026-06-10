-- Judge / Referee seminar rules: at-event seminars count in a 2-season window only,
-- not the 4-season in-person seminar window.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/039_judge_referee_at_event_seminar_window.sql

UPDATE officials_analysis.international_requirement_rule r
SET
    metric_config = $json$
{
  "alternatives": [
    {
      "label": "In-person ISU seminar (4 seasons)",
      "requirements": [
        {"in_person": true, "at_event": false, "season_window": 4, "min": 1}
      ]
    },
    {
      "label": "Online ISU seminar (2 seasons)",
      "requirements": [
        {"in_person": false, "season_window": 2, "min": 1}
      ]
    },
    {
      "label": "Seminar at designated competition (2 seasons)",
      "requirements": [
        {"at_event": true, "season_window": 2, "min": 1}
      ]
    }
  ]
}
$json$::jsonb,
    display_label = 'ISU seminar: in person (4 seasons), online (2 seasons), or at competition (2 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND r.metric = 'seminar_alternatives'
  AND rs.purpose = 'maintain'
  AND rs.appointment_type_id IN (12, 13)
  AND (
    (rs.sport = 'figure' AND rs.isu_rule_ref IN ('413.2.b', '413.4.b', '412.2.b', '412.4.b'))
    OR (rs.sport = 'synchronized' AND rs.isu_rule_ref IN ('829.2.b', '829.4.b', '828.2.b', '828.4.b'))
  );

UPDATE officials_analysis.international_requirement_rule r
SET
    metric_config = $json${"in_person": true, "at_event": false, "season_window": 4}$json$::jsonb,
    display_label = 'In-person ISU seminar (4 seasons; not at competition)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND r.metric = 'seminar_count'
  AND rs.purpose = 'promote'
  AND rs.appointment_type_id IN (12, 13)
  AND (
    (rs.sport = 'figure' AND rs.isu_rule_ref IN ('413.3.c', '412.3.c'))
    OR (rs.sport = 'synchronized' AND rs.isu_rule_ref IN ('829.3.c', '828.3.c'))
  );
