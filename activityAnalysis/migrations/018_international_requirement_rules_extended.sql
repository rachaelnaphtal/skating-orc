-- Extend ISU requirement rules: sport (figure vs synchronized) and seeds for Rules
-- 413–416 (figure) and 828–862 (synchronized skating).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/018_international_requirement_rules_extended.sql

ALTER TABLE officials_analysis.international_requirement_rule_set
    ADD COLUMN IF NOT EXISTS sport text NOT NULL DEFAULT 'figure'
        CHECK (sport IN ('figure', 'synchronized'));

COMMENT ON COLUMN officials_analysis.international_requirement_rule_set.sport IS
    'figure = Single/Pairs/Dance (ISU Rules 412–416); synchronized = SYS Rules 828–862.';

-- ---------------------------------------------------------------------------
-- Figure skating: International Judge maintain (413.2.b)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '413.2.b', 'maintain', 'International Judge — maintain (re-list)', 12, 'international', 3, 'figure', 110
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '413.2.b' AND appointment_type_id = 12 AND purpose = 'maintain' AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'combined_roles_competitions', 1, ARRAY[4, 1, 11], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, or TC in ≥1 international competition (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '413.2.b' AND rs.appointment_type_id = 12 AND rs.sport = 'figure'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '413.4.b', 'maintain', 'ISU Judge — maintain (re-list)', 12, 'isu', 3, 'figure', 120
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '413.4.b' AND appointment_type_id = 12 AND listing_tier = 'isu' AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'combined_roles_competitions', 1, ARRAY[4, 1, 11], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, Trial Judge, or TC in ≥1 international competition (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '413.4.b' AND rs.appointment_type_id = 12 AND rs.listing_tier = 'isu'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- ---------------------------------------------------------------------------
-- Figure: International Technical Controller maintain (414.2.b)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '414.2.b', 'maintain', 'International Technical Controller — maintain', 15, 'international', 3, 'figure', 130
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '414.2.b' AND appointment_type_id = 15 AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons; intl only)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '414.2.b' AND rs.appointment_type_id = 15 AND rs.sport = 'figure'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '414.4.b', 'maintain', 'ISU Technical Controller — maintain', 15, 'isu', 3, 'figure', 140
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '414.4.b' AND appointment_type_id = 15 AND listing_tier = 'isu' AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons; intl only)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '414.4.b' AND rs.appointment_type_id = 15 AND rs.listing_tier = 'isu'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- ---------------------------------------------------------------------------
-- Figure: International Technical Specialist maintain (415.2.b)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '415.2.b', 'maintain', 'International Technical Specialist — maintain', 14, 'international', 3, 'figure', 150
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '415.2.b' AND appointment_type_id = 14 AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons; intl only)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '415.2.b' AND rs.appointment_type_id = 14 AND rs.sport = 'figure'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '415.4.b', 'maintain', 'ISU Technical Specialist — maintain', 14, 'isu', 3, 'figure', 160
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '415.4.b' AND appointment_type_id = 14 AND listing_tier = 'isu' AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons; intl only)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '415.4.b' AND rs.appointment_type_id = 14 AND rs.listing_tier = 'isu'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- ---------------------------------------------------------------------------
-- Figure: International Data / Video Operator maintain (416.2.b)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '416.2.b', 'maintain', 'International Data / Video Operator — maintain', 16, 'international', 3, 'figure', 170
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '416.2.b' AND appointment_type_id = 16 AND listing_tier = 'international' AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'data_operator_competitions', 2, ARRAY[8], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Data/Video Operator in ≥2 international competitions (3 seasons; any discipline)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '416.2.b' AND rs.appointment_type_id = 16 AND rs.listing_tier = 'international'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sport, sort_order
)
SELECT '416.4.b', 'maintain', 'ISU Data / Video Operator — maintain', 16, 'isu', 3, 'figure', 180
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '416.4.b' AND appointment_type_id = 16 AND listing_tier = 'isu' AND sport = 'figure'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'data_operator_competitions', 2, ARRAY[8], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Data/Video Operator in ≥2 international competitions (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '416.4.b' AND rs.appointment_type_id = 16 AND rs.listing_tier = 'isu'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- ---------------------------------------------------------------------------
-- Synchronized skating (discipline_id = 2): Referee maintain (828.2.b)
-- ---------------------------------------------------------------------------
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '828.2.b', 'maintain', 'Synchronized — International Referee maintain', 13, 2, 'international', 3, 'synchronized', 210
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '828.2.b' AND appointment_type_id = 13 AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'combined_roles_competitions', 1, ARRAY[4, 1, 11], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, Trial Judge, or TC in ≥1 international competition (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '828.2.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '828.4.b', 'maintain', 'Synchronized — ISU Referee maintain', 13, 2, 'isu', 3, 'synchronized', 220
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '828.4.b' AND appointment_type_id = 13 AND listing_tier = 'isu' AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'combined_roles_competitions', 1, ARRAY[4, 1, 11], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, Trial Judge, or TC in ≥1 international competition (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '828.4.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- Synchronized: promote ISU Referee (828.3.c)
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '828.3.c', 'promote', 'Synchronized — ISU Referee promote', 13, 2, 'international', 4, 'synchronized', 230
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '828.3.c' AND purpose = 'promote' AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order, require_championship_or_olympic
)
SELECT rs.id, v.metric, v.min_value, v.roles, v.comp_types, v.levels, v.display_label, v.sort_order, v.champ
FROM officials_analysis.international_requirement_rule_set rs
CROSS JOIN (
    VALUES
        ('seasons_since_appointed', 4, NULL::integer[], NULL::integer[], NULL::text[],
         '≥4 seasons since appointment date', 1, false),
        ('referee_competitions', 2, ARRAY[4], ARRAY[15, 16, 17], ARRAY['Junior', 'Senior'],
         'Referee in ≥2 international competitions (4 seasons)', 2, false),
        ('judge_competitions', 3, ARRAY[1], ARRAY[15, 16], ARRAY['Junior', 'Senior'],
         'Judge in ≥3 ISU competitions (4 seasons)', 3, false),
        ('judge_championship_or_olympic', 1, ARRAY[1], ARRAY[15, 16], ARRAY['Junior', 'Senior'],
         'Judge in ≥1 ISU Championship (of the judge comps)', 4, true)
) AS v(metric, min_value, roles, comp_types, levels, display_label, sort_order, champ)
WHERE rs.isu_rule_ref = '828.3.c' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- Synchronized: International Judge maintain (829.2.b)
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '829.2.b', 'maintain', 'Synchronized — International Judge maintain', 12, 2, 'international', 3, 'synchronized', 240
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '829.2.b' AND appointment_type_id = 12 AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'combined_roles_competitions', 1, ARRAY[4, 1, 11], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, Trial Judge, or TC in ≥1 international competition (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '829.2.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '829.4.b', 'maintain', 'Synchronized — ISU Judge maintain', 12, 2, 'isu', 3, 'synchronized', 250
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '829.4.b' AND appointment_type_id = 12 AND listing_tier = 'isu' AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'combined_roles_competitions', 1, ARRAY[4, 1, 11], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, Trial Judge, or TC in ≥1 international competition (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '829.4.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- Synchronized: ITC maintain (860.2.b)
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '860.2.b', 'maintain', 'Synchronized — International TC maintain', 15, 2, 'international', 3, 'synchronized', 260
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '860.2.b' AND appointment_type_id = 15 AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '860.2.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '860.4.b', 'maintain', 'Synchronized — ISU TC maintain', 15, 2, 'isu', 3, 'synchronized', 270
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '860.4.b' AND appointment_type_id = 15 AND listing_tier = 'isu' AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '860.4.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- Synchronized: ITS maintain (861.2.b)
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '861.2.b', 'maintain', 'Synchronized — International TS maintain', 14, 2, 'international', 3, 'synchronized', 280
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '861.2.b' AND appointment_type_id = 14 AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '861.2.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '861.4.b', 'maintain', 'Synchronized — ISU TS maintain', 14, 2, 'isu', 3, 'synchronized', 290
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '861.4.b' AND appointment_type_id = 14 AND listing_tier = 'isu' AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'tc_or_ts_competitions', 2, ARRAY[11, 9], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'TC or TS in ≥2 international competitions (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '861.4.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- Synchronized: IDVO maintain (862.2.b) — any discipline counts
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '862.2.b', 'maintain', 'Synchronized — International IDVO maintain', 16, 'international', 3, 'synchronized', 300
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '862.2.b' AND appointment_type_id = 16 AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'data_operator_competitions', 2, ARRAY[8], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Data/Video Operator in ≥2 international competitions (3 seasons; any discipline)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '862.2.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id,
    listing_tier, season_window, sport, sort_order
)
SELECT '862.4.b', 'maintain', 'Synchronized — ISU IDVO maintain', 16, 'isu', 3, 'synchronized', 310
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '862.4.b' AND appointment_type_id = 16 AND listing_tier = 'isu' AND sport = 'synchronized'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, 'data_operator_competitions', 2, ARRAY[8], ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Data/Video Operator in ≥2 international competitions (3 seasons)', 1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '862.4.b' AND rs.sport = 'synchronized'
  AND NOT EXISTS (SELECT 1 FROM officials_analysis.international_requirement_rule r WHERE r.rule_set_id = rs.id);

-- Tag existing referee figure seeds with sport = figure (from migration 017)
UPDATE officials_analysis.international_requirement_rule_set
SET sport = 'figure'
WHERE sport IS NULL OR sport = 'figure';
