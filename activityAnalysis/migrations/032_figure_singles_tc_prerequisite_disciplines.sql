-- Singles TC promote prerequisite: Judge/Referee in Singles/Pairs; ISU TS in Singles only.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/032_figure_singles_tc_prerequisite_disciplines.sql

UPDATE officials_analysis.international_requirement_rule r
SET
    metric_config = '{"related_discipline_ids": [1, 8, 9], "ts_discipline_ids": [1]}'::jsonb,
    display_label = '≥4 years as International or ISU Judge, International Referee (Singles/Pairs), or ISU TS in Singles (listing July 1)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '414.3.c'
  AND rs.purpose = 'promote'
  AND rs.discipline_id = 1
  AND r.metric = 'years_tc_prerequisite';
