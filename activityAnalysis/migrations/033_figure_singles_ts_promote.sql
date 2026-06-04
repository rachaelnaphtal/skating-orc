-- Figure Singles TS promote-to-ISU (415.3.c).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/033_figure_singles_ts_promote.sql

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT
    '415.3.c',
    'promote',
    'ISU Technical Specialist — promote (Singles)',
    14,
    1,
    'international',
    4,
    'figure',
    72
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set rs
    WHERE rs.isu_rule_ref = '415.3.c'
      AND rs.purpose = 'promote'
      AND rs.appointment_type_id = 14
      AND rs.discipline_id = 1
      AND rs.sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, display_label, sort_order
)
SELECT
    rs.id,
    'years_in_grade',
    4,
    '≥4 years in grade as International TS in Singles (listing July 1)',
    0
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '415.3.c'
  AND rs.purpose = 'promote'
  AND rs.appointment_type_id = 14
  AND rs.discipline_id = 1
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = 'years_in_grade'
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
    ARRAY[9],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    true,
    '{"min_competitions": 3, "min_international_competition": 1}'::jsonb,
    'TS in ≥3 competitions (Singles or Pairs segments) incl. ≥1 International Competition (4 seasons)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '415.3.c'
  AND rs.purpose = 'promote'
  AND rs.appointment_type_id = 14
  AND rs.discipline_id = 1
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = 'tc_ts_promote_isu'
  );
