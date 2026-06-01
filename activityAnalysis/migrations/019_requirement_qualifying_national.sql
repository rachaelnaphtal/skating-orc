-- National (US qualifying) competitions for ISU maintain rules that allow them.
-- In this project: ``public.competition.qualifying = true``, Junior/Senior segments only.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/019_requirement_qualifying_national.sql

ALTER TABLE officials_analysis.international_requirement_rule
    ADD COLUMN IF NOT EXISTS include_qualifying_national boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN officials_analysis.international_requirement_rule.include_qualifying_national IS
    'When true, count panel activity at qualifying=true competitions (excluding adult/collegiate types 12–14) in addition to competition_type_ids.';

-- Figure International TC maintain (414.2.b): TC or TS in 2 international OR national competitions.
UPDATE officials_analysis.international_requirement_rule r
SET
    include_qualifying_national = true,
    display_label = 'TC or TS in ≥2 international or national qualifying competitions (3 seasons; Junior/Senior)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '414.2.b'
  AND r.metric = 'tc_or_ts_competitions';
