-- Figure Singles TC promote-to-ISU (414.3.c) and prerequisite label refresh.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/031_figure_singles_tc_promote.sql

-- Singles TC promote profile (directory discipline id 1).
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT
    '414.3.c',
    'promote',
    'ISU Technical Controller — promote (Singles)',
    15,
    1,
    'international',
    4,
    'figure',
    62
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set rs
    WHERE rs.isu_rule_ref = '414.3.c'
      AND rs.purpose = 'promote'
      AND rs.appointment_type_id = 15
      AND rs.discipline_id = 1
      AND rs.sport = 'figure'
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
    ARRAY[11],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    true,
    '{"min_competitions": 3, "min_international_competition": 1}'::jsonb,
    'TC in ≥3 competitions incl. ≥1 International Competition (4 seasons; discipline)',
    2
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '414.3.c'
  AND rs.purpose = 'promote'
  AND rs.appointment_type_id = 15
  AND rs.discipline_id = 1
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, metric_config, display_label, sort_order
)
SELECT
    rs.id,
    v.metric,
    4,
    v.metric_config::jsonb,
    v.display_label,
    v.sort_order
FROM officials_analysis.international_requirement_rule_set rs
CROSS JOIN (
    VALUES
        (
            'years_tc_prerequisite',
            '{"related_discipline_ids": [1, 8, 9], "ts_discipline_ids": [1]}',
            '≥4 years as International or ISU Judge, International Referee (Singles/Pairs), or ISU TS in Singles (listing July 1)',
            0
        ),
        (
            'years_in_grade',
            NULL::text,
            '≥4 years in grade as International TC in Singles (listing July 1)',
            1
        )
) AS v(metric, metric_config, display_label, sort_order)
WHERE rs.isu_rule_ref = '414.3.c'
  AND rs.purpose = 'promote'
  AND rs.appointment_type_id = 15
  AND rs.discipline_id = 1
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = v.metric
  );

-- Pairs TC prerequisite: clarify that ISU Judge in Singles/Pairs counts.
UPDATE officials_analysis.international_requirement_rule r
SET display_label = '≥4 years as International or ISU Judge, International Referee (Singles/Pairs), or ISU TS (listing July 1)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '414.3.c'
  AND rs.purpose = 'promote'
  AND rs.discipline_id = 8
  AND r.metric = 'years_tc_prerequisite';
