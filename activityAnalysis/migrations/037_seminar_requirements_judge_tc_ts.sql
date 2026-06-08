-- Seminar requirements: Judge/Referee (figure + synchronized), TC/TS maintain.
-- Requires migrations 035 and 036 (referee figure seeds).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/037_seminar_requirements_judge_tc_ts.sql

-- ---------------------------------------------------------------------------
-- Judge / Referee maintain: in-person (4 seasons) OR online (2 seasons)
-- ---------------------------------------------------------------------------
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
      "label": "In-person ISU seminar (4 seasons)",
      "requirements": [
        {"in_person": true, "season_window": 4, "min": 1}
      ]
    },
    {
      "label": "Online ISU seminar (2 seasons)",
      "requirements": [
        {"in_person": false, "season_window": 2, "min": 1}
      ]
    }
  ]
}
$json$::jsonb,
    'ISU seminar: in person (4 seasons) or online (2 seasons)',
    COALESCE(
        (SELECT MAX(r.sort_order) FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id),
        0
    ) + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.purpose = 'maintain'
  AND rs.appointment_type_id IN (12, 13)
  AND (
    (rs.sport = 'figure' AND rs.isu_rule_ref IN ('413.2.b', '413.4.b', '412.2.b', '412.4.b'))
    OR (rs.sport = 'synchronized' AND rs.isu_rule_ref IN ('829.2.b', '829.4.b', '828.2.b', '828.4.b'))
  )
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id AND r.metric = 'seminar_alternatives'
  );

-- ---------------------------------------------------------------------------
-- Judge / Referee promote: in-person seminar (4 seasons)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'seminar_count',
    1,
    $json${"in_person": true, "season_window": 4}$json$::jsonb,
    'In-person ISU seminar (4 seasons)',
    COALESCE(
        (SELECT MAX(r.sort_order) FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id),
        0
    ) + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.purpose = 'promote'
  AND rs.appointment_type_id IN (12, 13)
  AND (
    (rs.sport = 'figure' AND rs.isu_rule_ref IN ('413.3.c', '412.3.c'))
    OR (rs.sport = 'synchronized' AND rs.isu_rule_ref IN ('829.3.c', '828.3.c'))
  )
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id AND r.metric = 'seminar_count'
  );

-- ---------------------------------------------------------------------------
-- TC / TS maintain (International): any seminar in past 2 seasons
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'seminar_count',
    1,
    $json${"season_window": 2}$json$::jsonb,
    'ISU seminar online or in person (2 seasons)',
    COALESCE(
        (SELECT MAX(r.sort_order) FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id),
        0
    ) + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.purpose = 'maintain'
  AND rs.listing_tier = 'international'
  AND rs.appointment_type_id IN (14, 15)
  AND (
    (rs.sport = 'figure' AND rs.isu_rule_ref IN ('414.2.b', '415.2.b'))
    OR (rs.sport = 'synchronized' AND rs.isu_rule_ref IN ('860.2.b', '861.2.b'))
  )
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id AND r.metric = 'seminar_count'
  );

-- ---------------------------------------------------------------------------
-- TC / TS maintain (ISU): seminar (2 seasons) OR at designated competition
-- ---------------------------------------------------------------------------
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
      "label": "Online or in-person seminar (2 seasons)",
      "requirements": [
        {"season_window": 2, "min": 1}
      ]
    },
    {
      "label": "Seminar at designated competition (2 seasons)",
      "requirements": [
        {"season_window": 2, "min": 1, "at_event": true}
      ]
    }
  ]
}
$json$::jsonb,
    'ISU seminar (2 seasons) or at designated competition',
    COALESCE(
        (SELECT MAX(r.sort_order) FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id),
        0
    ) + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.purpose = 'maintain'
  AND rs.listing_tier = 'isu'
  AND rs.appointment_type_id IN (14, 15)
  AND (
    (rs.sport = 'figure' AND rs.isu_rule_ref IN ('414.4.b', '415.4.b'))
    OR (rs.sport = 'synchronized' AND rs.isu_rule_ref IN ('860.4.b', '861.4.b'))
  )
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id AND r.metric = 'seminar_alternatives'
  );
