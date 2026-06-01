-- Fix competition_alternatives: "2 International" branches must count ISU Events (15–16)
-- as well as International Competition (17). Type-17-only scope remains for mixed
-- "1 International + 1 National" branches.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/023_competition_alternatives_intl_all.sql

UPDATE officials_analysis.international_requirement_rule r
SET
    metric_config = jsonb_set(
        r.metric_config,
        '{alternatives,0,requirements,0,scope}',
        '"international_all"'::jsonb
    )
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND r.metric = 'competition_alternatives'
  AND r.metric_config -> 'alternatives' -> 0 -> 'requirements' -> 0 ->> 'scope'
      = 'international_competition';
