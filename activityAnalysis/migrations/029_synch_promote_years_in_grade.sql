-- Promote-to-ISU year requirements (July 1 listing reference), synchronized skating.
-- Mirrors migration 028 (figure): Referee, Judge, TC, and TS promote profiles.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/029_synch_promote_years_in_grade.sql

-- ISU Referee promote (828.3.c): ISU Judge + International Referee in synchronized discipline.
DELETE FROM officials_analysis.international_requirement_rule r
USING officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '828.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized'
  AND r.metric = 'seasons_since_appointed';

UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 2
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '828.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized';

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
        (
            'years_intl_referee',
            '≥4 years as International Referee in synchronized discipline (listing July 1)',
            1
        )
) AS v(metric, display_label, sort_order)
WHERE rs.isu_rule_ref = '828.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = v.metric
  );

-- ISU Judge promote (829.3.c): years in current International Judge appointment.
UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '829.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized';

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, display_label, sort_order
)
SELECT rs.id, 'years_in_grade', 4,
    '≥4 years in grade in this appointment (listing July 1)', 0
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '829.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = 'years_in_grade'
  );

-- ISU TC promote (860.3.c): prerequisite years + years in TC appointment.
UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 2
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '860.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized';

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
            '{"related_discipline_ids": [2]}'::text,
            '≥4 years as International Judge, Referee, or ISU TS in synchronized (listing July 1)',
            0
        ),
        (
            'years_in_grade',
            NULL::text,
            '≥4 years in grade as International TC in synchronized (listing July 1)',
            1
        )
) AS v(metric, metric_config, display_label, sort_order)
WHERE rs.isu_rule_ref = '860.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = v.metric
  );

-- ISU TS promote (861.3.c): years in TS appointment.
UPDATE officials_analysis.international_requirement_rule r
SET sort_order = r.sort_order + 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '861.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized';

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, display_label, sort_order
)
SELECT rs.id, 'years_in_grade', 4,
    '≥4 years in grade as International TS in synchronized (listing July 1)', 0
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '861.3.c'
  AND rs.purpose = 'promote'
  AND rs.sport = 'synchronized'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r2
      WHERE r2.rule_set_id = rs.id AND r2.metric = 'years_in_grade'
  );
