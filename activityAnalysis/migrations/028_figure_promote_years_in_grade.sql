-- Promote-to-ISU year requirements (July 1 listing reference), figure skating.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/028_figure_promote_years_in_grade.sql

-- ISU Referee promote (412.3.c): replace vague seasons_since_appointed with ISU Judge + Intl Referee years.
DELETE FROM officials_analysis.international_requirement_rule r
USING officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '412.3.c'
  AND rs.purpose = 'promote'
  AND r.metric = 'seasons_since_appointed';

UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 2
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '412.3.c'
  AND rs.purpose = 'promote';

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, display_label, sort_order
)
SELECT
    rs.id,
    v.metric,
    4,
    v.display_label,
    v.sort_order
FROM officials_analysis.international_requirement_rule_set rs
CROSS JOIN (
    VALUES
        ('years_isu_judge', '≥4 years as ISU Judge (listing July 1)', 0),
        ('years_intl_referee', '≥4 years as International Referee in discipline (listing July 1)', 1)
) AS v(metric, display_label, sort_order)
WHERE rs.isu_rule_ref = '412.3.c'
  AND rs.purpose = 'promote'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = v.metric
  );

-- ISU Judge promote (413.3.c): years in current International Judge appointment.
UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '413.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'figure';

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, display_label, sort_order
)
SELECT rs.id, 'years_in_grade', 4,
    '≥4 years in grade in this appointment (listing July 1)', 0
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '413.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = 'years_in_grade'
  );

-- ISU TC promote (414.3.c): prerequisite years + years in TC appointment.
UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 2
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '414.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'figure';

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
            NULL::text,
            '≥4 years as International Judge, Referee (Singles/Pairs), or ISU TS (listing July 1)',
            0
        ),
        (
            'years_in_grade',
            NULL::text,
            '≥4 years in grade as International TC in this discipline (listing July 1)',
            1
        )
) AS v(metric, metric_config, display_label, sort_order)
WHERE rs.isu_rule_ref = '414.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = v.metric
  );

-- Pairs TC: Singles/Pairs judge & referee directory disciplines count toward prerequisite.
UPDATE officials_analysis.international_requirement_rule r
SET metric_config = '{"related_discipline_ids": [1, 8, 9]}'::jsonb
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '414.3.c'
  AND rs.purpose = 'promote'
  AND rs.discipline_id = 8
  AND r.metric = 'years_tc_prerequisite';

-- ISU TS promote (415.3.c): years in TS appointment.
UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '415.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'figure';

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, display_label, sort_order
)
SELECT rs.id, 'years_in_grade', 4,
    '≥4 years in grade as International TS in this discipline (listing July 1)',
    0
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '415.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = 'years_in_grade'
  );
