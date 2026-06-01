-- ISU Rules 411–417: configurable maintain / promote service requirements for
-- international directory appointments. Evaluated against ``public.segment_official``
-- panel activity at international competitions.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/017_international_requirement_rules.sql

CREATE TABLE IF NOT EXISTS officials_analysis.international_requirement_rule_set (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    isu_rule_ref text NOT NULL,
    purpose text NOT NULL CHECK (purpose IN ('maintain', 'promote')),
    label text NOT NULL,
    appointment_type_id integer NOT NULL,
    directory_level_id integer,
    discipline_id integer,
    listing_tier text NOT NULL DEFAULT 'international'
        CHECK (listing_tier IN ('international', 'isu')),
    season_window integer NOT NULL,
    sort_order integer NOT NULL DEFAULT 0,
    active boolean NOT NULL DEFAULT true,
    CONSTRAINT international_requirement_rule_set_appointment_type_id_fkey
        FOREIGN KEY (appointment_type_id)
        REFERENCES officials_analysis.appointment_types (id),
    CONSTRAINT international_requirement_rule_set_directory_level_id_fkey
        FOREIGN KEY (directory_level_id)
        REFERENCES officials_analysis.levels (id),
    CONSTRAINT international_requirement_rule_set_discipline_id_fkey
        FOREIGN KEY (discipline_id)
        REFERENCES officials_analysis.disciplines (id)
);

COMMENT ON TABLE officials_analysis.international_requirement_rule_set IS
    'ISU listing requirement profile (maintain re-list or promote) per appointment type / discipline.';

COMMENT ON COLUMN officials_analysis.international_requirement_rule_set.listing_tier IS
    'international = Rule 412.2-style International listing; isu = Rule 412.4 ISU listing.';

CREATE TABLE IF NOT EXISTS officials_analysis.international_requirement_rule (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule_set_id integer NOT NULL,
    metric text NOT NULL,
    min_value integer NOT NULL,
    role_appointment_type_ids integer[],
    competition_type_ids integer[],
    segment_levels text[],
    require_championship_or_olympic boolean NOT NULL DEFAULT false,
    sort_order integer NOT NULL DEFAULT 0,
    display_label text,
    CONSTRAINT international_requirement_rule_rule_set_id_fkey
        FOREIGN KEY (rule_set_id)
        REFERENCES officials_analysis.international_requirement_rule_set (id)
        ON DELETE CASCADE
);

COMMENT ON TABLE officials_analysis.international_requirement_rule IS
    'One requirement within a set (AND logic). Metrics implemented in international_requirements.py.';

CREATE INDEX IF NOT EXISTS ix_intl_requirement_rule_set_appt
    ON officials_analysis.international_requirement_rule_set (appointment_type_id, purpose)
    WHERE active;

-- Seed: International Referee maintain (ISU Rule 412.2.b) — all disciplines.
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sort_order
)
SELECT
    '412.2.b',
    'maintain',
    'International Referee — maintain (re-list)',
    13,
    'international',
    3,
    10
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '412.2.b' AND purpose = 'maintain' AND appointment_type_id = 13
      AND listing_tier = 'international' AND discipline_id IS NULL
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id,
    'combined_roles_competitions',
    1,
    ARRAY[4, 1, 11],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, or TC in ≥1 international competition (3 seasons)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '412.2.b' AND rs.purpose = 'maintain' AND rs.appointment_type_id = 13
  AND rs.listing_tier = 'international' AND rs.discipline_id IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- Seed: ISU Referee maintain (ISU Rule 412.4.b) — same service threshold.
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, listing_tier, season_window, sort_order
)
SELECT
    '412.4.b',
    'maintain',
    'ISU Referee — maintain (re-list)',
    13,
    'isu',
    3,
    20
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '412.4.b' AND purpose = 'maintain' AND appointment_type_id = 13
      AND listing_tier = 'isu'
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id,
    'combined_roles_competitions',
    1,
    ARRAY[4, 1, 11],
    ARRAY[15, 16, 17],
    ARRAY['Junior', 'Senior'],
    'Referee, Judge, or TC in ≥1 international competition (3 seasons)',
    1
FROM officials_analysis.international_requirement_rule_set rs
WHERE rs.isu_rule_ref = '412.4.b' AND rs.purpose = 'maintain' AND rs.listing_tier = 'isu'
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- Seed: Promote toward ISU Referee (ISU Rule 412.3 service) — Singles/Pairs.
-- directory_level_id resolved at runtime if level name is International.
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sort_order
)
SELECT
    '412.3.c',
    'promote',
    'ISU Referee — promote (Single & Pair Skating)',
    13,
    9,
    'international',
    4,
    30
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '412.3.c' AND purpose = 'promote' AND appointment_type_id = 13
      AND discipline_id = 9
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, v.metric, v.min_value, v.roles, v.comp_types, v.levels, v.display_label, v.sort_order
FROM officials_analysis.international_requirement_rule_set rs
CROSS JOIN (
    VALUES
        ('seasons_since_appointed', 4, NULL::integer[], NULL::integer[], NULL::text[],
         '≥4 seasons since appointment date', 1),
        ('referee_competitions', 2, ARRAY[4], ARRAY[15, 16, 17], ARRAY['Junior', 'Senior'],
         'Referee in ≥2 international competitions (4 seasons)', 2),
        ('judge_competitions', 3, ARRAY[1], ARRAY[15, 16], ARRAY['Junior', 'Senior'],
         'Judge in ≥3 ISU competitions (4 seasons)', 3),
        ('judge_championship_or_olympic', 2, ARRAY[1], ARRAY[15, 16], ARRAY['Junior', 'Senior'],
         'Judge in ≥2 ISU Championships or Olympic Games (4 seasons)', 4)
) AS v(metric, min_value, roles, comp_types, levels, display_label, sort_order)
WHERE rs.isu_rule_ref = '412.3.c' AND rs.purpose = 'promote' AND rs.discipline_id = 9
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );

-- Seed: Promote toward ISU Referee — Ice Dance (discipline_id 4).
INSERT INTO officials_analysis.international_requirement_rule_set (
    isu_rule_ref, purpose, label, appointment_type_id, discipline_id,
    listing_tier, season_window, sort_order
)
SELECT
    '412.3.c',
    'promote',
    'ISU Referee — promote (Ice Dance)',
    13,
    4,
    'international',
    4,
    40
WHERE NOT EXISTS (
    SELECT 1 FROM officials_analysis.international_requirement_rule_set
    WHERE isu_rule_ref = '412.3.c' AND purpose = 'promote' AND appointment_type_id = 13
      AND discipline_id = 4
);

INSERT INTO officials_analysis.international_requirement_rule (
    rule_set_id, metric, min_value, role_appointment_type_ids,
    competition_type_ids, segment_levels, display_label, sort_order
)
SELECT rs.id, v.metric, v.min_value, v.roles, v.comp_types, v.levels, v.display_label, v.sort_order
FROM officials_analysis.international_requirement_rule_set rs
CROSS JOIN (
    VALUES
        ('seasons_since_appointed', 4, NULL::integer[], NULL::integer[], NULL::text[],
         '≥4 seasons since appointment date', 1),
        ('referee_competitions', 2, ARRAY[4], ARRAY[15, 16, 17], ARRAY['Junior', 'Senior'],
         'Referee in ≥2 international competitions (4 seasons)', 2),
        ('judge_competitions', 3, ARRAY[1], ARRAY[15, 16], ARRAY['Junior', 'Senior'],
         'Judge in ≥3 ISU competitions (4 seasons)', 3),
        ('judge_championship_or_olympic', 2, ARRAY[1], ARRAY[15, 16], ARRAY['Junior', 'Senior'],
         'Judge in ≥2 ISU Championships or Olympic Games (4 seasons)', 4)
) AS v(metric, min_value, roles, comp_types, levels, display_label, sort_order)
WHERE rs.isu_rule_ref = '412.3.c' AND rs.purpose = 'promote' AND rs.discipline_id = 4
  AND NOT EXISTS (
      SELECT 1 FROM officials_analysis.international_requirement_rule r
      WHERE r.rule_set_id = rs.id
  );
