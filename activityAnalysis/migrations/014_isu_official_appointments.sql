-- Normalize ISU officials so one person has one stable isu_official.id, while
-- discipline / appointment / level combinations live in separate appointment rows.
--
-- Uses a real staging table (not ON COMMIT DROP temp) so clients that autocommit
-- per statement (DBeaver, etc.) still see the table for later statements.

ALTER TABLE officials_analysis.isu_official
    ADD COLUMN IF NOT EXISTS federation_name TEXT;

CREATE TABLE IF NOT EXISTS officials_analysis.isu_official_appointment (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    isu_official_id INTEGER NOT NULL
        REFERENCES officials_analysis.isu_official (id) ON DELETE CASCADE,
    discipline TEXT NOT NULL DEFAULT '',
    appointment_type TEXT NOT NULL DEFAULT '',
    level TEXT NOT NULL DEFAULT '',
    season TEXT NOT NULL,
    communication_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT isu_official_appointment_unique
        UNIQUE (isu_official_id, discipline, appointment_type, level, season)
);

CREATE INDEX IF NOT EXISTS idx_isu_official_appointment_isu_official_id
    ON officials_analysis.isu_official_appointment (isu_official_id);

DROP TABLE IF EXISTS officials_analysis._isu_official_dedupe_stg;

CREATE TABLE officials_analysis._isu_official_dedupe_stg AS
SELECT
    id,
    MIN(id) OVER (PARTITION BY federation_code, name_normalized) AS keeper_id,
    ROW_NUMBER() OVER (PARTITION BY federation_code, name_normalized ORDER BY id) AS rn
FROM officials_analysis.isu_official;

-- If older loads created one row per season, collapse references to the
-- lowest id per federation/name so judge and segment links stay stable.
UPDATE public.judge_isu_official_link l
SET isu_official_id = d.keeper_id
FROM officials_analysis._isu_official_dedupe_stg d
WHERE l.isu_official_id = d.id
  AND d.rn > 1;

UPDATE public.isu_official_name_alias a
SET isu_official_id = d.keeper_id
FROM officials_analysis._isu_official_dedupe_stg d
WHERE a.isu_official_id = d.id
  AND d.rn > 1;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'segment_official'
          AND column_name = 'isu_official_id'
    ) THEN
        UPDATE public.segment_official so
        SET isu_official_id = d.keeper_id
        FROM officials_analysis._isu_official_dedupe_stg d
        WHERE so.isu_official_id = d.id
          AND d.rn > 1;
    END IF;
END $$;

-- 013-style one row per season on isu_official → appointment rows on keeper id.
INSERT INTO officials_analysis.isu_official_appointment (
    isu_official_id,
    discipline,
    appointment_type,
    level,
    season,
    communication_ref
)
SELECT DISTINCT
    d.keeper_id,
    '',
    '',
    '',
    io.season,
    io.communication_ref
FROM officials_analysis._isu_official_dedupe_stg d
JOIN officials_analysis.isu_official io ON io.id = d.id
ON CONFLICT (isu_official_id, discipline, appointment_type, level, season)
DO UPDATE SET
    communication_ref = COALESCE(
        EXCLUDED.communication_ref,
        officials_analysis.isu_official_appointment.communication_ref
    ),
    last_modified = NOW();

-- Appointments already stored on duplicate official ids (re-run / partial load).
INSERT INTO officials_analysis.isu_official_appointment (
    isu_official_id,
    discipline,
    appointment_type,
    level,
    season,
    communication_ref
)
SELECT DISTINCT
    d.keeper_id,
    a.discipline,
    a.appointment_type,
    a.level,
    a.season,
    a.communication_ref
FROM officials_analysis.isu_official_appointment a
JOIN officials_analysis._isu_official_dedupe_stg d ON a.isu_official_id = d.id
WHERE d.rn > 1
ON CONFLICT (isu_official_id, discipline, appointment_type, level, season)
DO UPDATE SET
    communication_ref = COALESCE(
        EXCLUDED.communication_ref,
        officials_analysis.isu_official_appointment.communication_ref
    ),
    last_modified = NOW();

DELETE FROM officials_analysis.isu_official_appointment a
USING officials_analysis._isu_official_dedupe_stg d
WHERE a.isu_official_id = d.id
  AND d.rn > 1;

WITH merged AS (
    SELECT
        d.keeper_id,
        STRING_AGG(DISTINCT io.season, ',' ORDER BY io.season) AS seasons,
        STRING_AGG(DISTINCT io.communication_ref, ',' ORDER BY io.communication_ref)
            FILTER (WHERE io.communication_ref IS NOT NULL AND TRIM(io.communication_ref) <> '')
            AS communication_refs
    FROM officials_analysis._isu_official_dedupe_stg d
    JOIN officials_analysis.isu_official io ON io.id = d.id
    GROUP BY d.keeper_id
)
UPDATE officials_analysis.isu_official io
SET season = COALESCE(merged.seasons, io.season),
    communication_ref = COALESCE(merged.communication_refs, io.communication_ref),
    last_modified = NOW()
FROM merged
WHERE io.id = merged.keeper_id;

DELETE FROM officials_analysis.isu_official io
USING officials_analysis._isu_official_dedupe_stg d
WHERE io.id = d.id
  AND d.rn > 1;

DROP TABLE IF EXISTS officials_analysis._isu_official_dedupe_stg;

ALTER TABLE officials_analysis.isu_official
    DROP CONSTRAINT IF EXISTS isu_official_roster_unique;

ALTER TABLE officials_analysis.isu_official
    ADD CONSTRAINT isu_official_roster_unique
        UNIQUE (federation_code, name_normalized);
