-- Backfill missing tc_ts_promote_isu for Singles TS promote (033 inserted years_in_grade
-- first, so the service rule's NOT EXISTS blocked the second insert).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/034_figure_singles_ts_promote_service_rule.sql

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
