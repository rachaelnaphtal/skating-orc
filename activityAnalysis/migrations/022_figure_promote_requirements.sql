-- Figure skating promote-to-ISU service rules (Rules 413.3, 414.3, 415.3).
-- ISU Communications inclusion requirements are not automated (handled separately).
--
-- Requires migrations 017–021 (schema, metric_config, maintain updates).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/022_figure_promote_requirements.sql

-- ---------------------------------------------------------------------------
-- Judge promote to ISU — Singles/Pairs (413.3.c, discipline 9)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT
    '413.3.c',
    'promote',
    'ISU Judge — promote (Singles/Pairs)',
    12,
    9,
    'international',
    4,
    'figure',
    50
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '413.3.c' AND purpose = 'promote' AND appointment_type_id = 12
      AND discipline_id = 9 AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, include_qualifying_national,
    metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'judge_promote_isu',
    1,
    ARRAY[1],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    false,
    $json$
{
  "min_competitions": 4,
  "required": [
    {"kind": "segment_level", "level": "Senior", "min_competitions": 1},
    {"kind": "segment_level", "level": "Junior", "min_competitions": 1},
    {"kind": "segment_discipline_type_id", "discipline_type_id": 2, "min_competitions": 1},
    {"kind": "scope", "scope": "isu_event", "min_competitions": 1, "last_season_only": true}
  ]
}
$json$::jsonb,
    'Judge in ≥4 international competitions incl. Senior, Junior, Pairs, ISU Event (last season)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '413.3.c' AND rs.purpose = 'promote' AND rs.discipline_id = 9
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- ---------------------------------------------------------------------------
-- Judge promote to ISU — Ice Dance (413.3.c, discipline 4)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT
    '413.3.c',
    'promote',
    'ISU Judge — promote (Ice Dance)',
    12,
    4,
    'international',
    4,
    'figure',
    55
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '413.3.c' AND purpose = 'promote' AND appointment_type_id = 12
      AND discipline_id = 4 AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, include_qualifying_national,
    metric_config, display_label, sort_order
)
SELECT
    rs.id,
    'judge_promote_isu',
    1,
    ARRAY[1],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    false,
    $json$
{
  "min_competitions": 3,
  "required": [
    {"kind": "segment_level", "level": "Senior", "min_competitions": 1},
    {"kind": "segment_level", "level": "Junior", "min_competitions": 1},
    {"kind": "scope", "scope": "isu_event", "min_competitions": 1, "last_season_only": true}
  ]
}
$json$::jsonb,
    'Judge in ≥3 international competitions incl. Senior, Junior, ISU Event (last season)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '413.3.c' AND rs.purpose = 'promote' AND rs.discipline_id = 4
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- ---------------------------------------------------------------------------
-- Technical Controller promote to ISU (414.3.c) — Singles/Pairs and Dance
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT v.isu_rule_ref, v.purpose, v.label, v.appointment_type_id, v.discipline_id,
    v.listing_tier, v.season_window, v.sport, v.sort_order
FROM (
    VALUES
        ('414.3.c', 'promote', 'ISU Technical Controller — promote (Singles/Pairs)', 15, 9, 'international', 4, 'figure', 60),
        ('414.3.c', 'promote', 'ISU Technical Controller — promote (Ice Dance)', 15, 4, 'international', 4, 'figure', 65)
) AS v(isu_rule_ref, purpose, label, appointment_type_id, discipline_id, listing_tier, season_window, sport, sort_order)
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set rs
    WHERE rs.isu_rule_ref = v.isu_rule_ref AND rs.purpose = v.purpose
      AND rs.appointment_type_id = v.appointment_type_id
      AND rs.discipline_id = v.discipline_id AND rs.sport = v.sport
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
    $json${"min_competitions": 3, "min_international_competition": 1}$json$::jsonb,
    'TC in ≥3 competitions incl. ≥1 International Competition (4 seasons; discipline)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '414.3.c' AND rs.purpose = 'promote' AND rs.appointment_type_id = 15
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- ---------------------------------------------------------------------------
-- Technical Specialist promote to ISU (415.3.c) — Singles/Pairs and Dance
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT v.isu_rule_ref, v.purpose, v.label, v.appointment_type_id, v.discipline_id,
    v.listing_tier, v.season_window, v.sport, v.sort_order
FROM (
    VALUES
        ('415.3.c', 'promote', 'ISU Technical Specialist — promote (Singles/Pairs)', 14, 9, 'international', 4, 'figure', 70),
        ('415.3.c', 'promote', 'ISU Technical Specialist — promote (Ice Dance)', 14, 4, 'international', 4, 'figure', 75)
) AS v(isu_rule_ref, purpose, label, appointment_type_id, discipline_id, listing_tier, season_window, sport, sort_order)
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set rs
    WHERE rs.isu_rule_ref = v.isu_rule_ref AND rs.purpose = v.purpose
      AND rs.appointment_type_id = v.appointment_type_id
      AND rs.discipline_id = v.discipline_id AND rs.sport = v.sport
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
    $json${"min_competitions": 3, "min_international_competition": 1}$json$::jsonb,
    'TS in ≥3 competitions incl. ≥1 International Competition (4 seasons; discipline)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '415.3.c' AND rs.purpose = 'promote' AND rs.appointment_type_id = 14
  AND rs.sport = 'figure'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- Clarify referee promote labels (412.3.c — service only; Communications not automated)
UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Referee in ≥2 international competitions in discipline (4 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '412.3.c'
  AND rs.purpose = 'promote'
  AND r.metric = 'referee_competitions';

UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Judge in ≥3 ISU Events or OWG in discipline (4 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '412.3.c'
  AND rs.purpose = 'promote'
  AND r.metric = 'judge_competitions';

UPDATE officials_analysis.international_requirement_rule r
SET display_label = 'Judge in ≥2 ISU Championships or OWG in discipline (4 seasons)'
FROM officials_analysis.international_requirement_rule_set rs
WHERE r.rule_set_id = rs.id
  AND rs.isu_rule_ref = '412.3.c'
  AND rs.purpose = 'promote'
  AND r.metric = 'judge_championship_or_olympic';
