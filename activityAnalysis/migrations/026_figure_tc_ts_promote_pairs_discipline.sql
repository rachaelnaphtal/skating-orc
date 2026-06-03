-- TC/TS promote-to-ISU profiles use directory discipline Pairs (id 8), not
-- Singles/Pairs combined (id 9). Id 9 remains for Judge promote (413.3.c) only.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/026_figure_tc_ts_promote_pairs_discipline.sql

UPDATE officials_analysis.international_requirement_rule_set
SET
    discipline_id = 8,
    label = REPLACE(label, 'Singles/Pairs', 'Pairs')
WHERE purpose = 'promote'
  AND sport = 'figure'
  AND appointment_type_id IN (14, 15)
  AND discipline_id = 9
  AND isu_rule_ref IN ('414.3.c', '415.3.c');
