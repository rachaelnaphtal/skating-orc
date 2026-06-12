-- Link USA ISU-protocol judge rows to the same USFS directory official as their
-- domestic protocol alias. Fixes split identities (e.g. Whitney Luke / Whitney LUKE)
-- so international marks count under the US-linked identity.
--
-- Run the PREVIEW block first, then the INSERT block.
-- After applying, refresh element ranking shard + summary caches (see precompute script).

-- ---------------------------------------------------------------------------
-- PREVIEW: rows that would be inserted/updated
-- ---------------------------------------------------------------------------
WITH us_domestic AS (
    SELECT
        jol.judge_id AS us_judge_id,
        jol.official_id,
        j.name AS us_protocol_name,
        lower(regexp_replace(trim(o.full_name), '\s+', ' ', 'g')) AS match_key
    FROM judge_official_link jol
    JOIN judge j ON j.id = jol.judge_id
    JOIN officials_analysis.officials o ON o.id = jol.official_id
    WHERE jol.status = 'linked'
      AND jol.official_id IS NOT NULL
),
isu_international AS (
    SELECT
        jiol.judge_id AS isu_judge_id,
        jiol.isu_official_id,
        j.name AS isu_protocol_name,
        io.name_normalized AS match_key
    FROM judge_isu_official_link jiol
    JOIN judge j ON j.id = jiol.judge_id
    JOIN officials_analysis.isu_official io ON io.id = jiol.isu_official_id
    WHERE upper(io.federation_code) = 'USA'
),
pairs AS (
    SELECT DISTINCT ON (ii.isu_judge_id)
        ii.isu_judge_id,
        ud.us_judge_id,
        ud.official_id,
        ud.us_protocol_name,
        ii.isu_protocol_name,
        o.full_name AS directory_name,
        existing.status AS existing_link_status,
        existing.official_id AS existing_official_id
    FROM isu_international ii
    JOIN us_domestic ud ON ud.match_key = ii.match_key
    JOIN officials_analysis.officials o ON o.id = ud.official_id
    LEFT JOIN judge_official_link existing ON existing.judge_id = ii.isu_judge_id
    ORDER BY ii.isu_judge_id, ud.us_judge_id
)
SELECT
    isu_judge_id,
    us_judge_id,
    official_id,
    directory_name,
    us_protocol_name,
    isu_protocol_name,
    existing_link_status,
    existing_official_id,
    CASE
        WHEN existing_link_status = 'linked'
             AND existing_official_id = official_id THEN 'already correct'
        WHEN existing_link_status IS NOT NULL THEN 'will update'
        ELSE 'will insert'
    END AS action
FROM pairs
ORDER BY lower(directory_name), isu_judge_id;


-- ---------------------------------------------------------------------------
-- APPLY: upsert judge_official_link for each ISU judge id (skips already-correct)
-- ---------------------------------------------------------------------------
-- Uncomment BEGIN through COMMIT to run.

-- BEGIN;

-- WITH us_domestic AS (
--     SELECT
--         jol.judge_id AS us_judge_id,
--         jol.official_id,
--         lower(regexp_replace(trim(o.full_name), '\s+', ' ', 'g')) AS match_key
--     FROM judge_official_link jol
--     JOIN officials_analysis.officials o ON o.id = jol.official_id
--     WHERE jol.status = 'linked'
--       AND jol.official_id IS NOT NULL
-- ),
-- isu_international AS (
--     SELECT
--         jiol.judge_id AS isu_judge_id,
--         io.name_normalized AS match_key
--     FROM judge_isu_official_link jiol
--     JOIN officials_analysis.isu_official io ON io.id = jiol.isu_official_id
--     WHERE upper(io.federation_code) = 'USA'
-- ),
-- pairs AS (
--     SELECT DISTINCT ON (ii.isu_judge_id)
--         ii.isu_judge_id,
--         ud.us_judge_id,
--         ud.official_id,
--         o.full_name AS directory_name
--     FROM isu_international ii
--     JOIN us_domestic ud ON ud.match_key = ii.match_key
--     JOIN officials_analysis.officials o ON o.id = ud.official_id
--     ORDER BY ii.isu_judge_id, ud.us_judge_id
-- )
-- INSERT INTO judge_official_link (judge_id, official_id, status, note, updated_at)
-- SELECT
--     p.isu_judge_id,
--     p.official_id,
--     'linked',
--     'merged with US judge ' || p.us_judge_id::text
--         || ' (' || p.directory_name || ')',
--     NOW()
-- FROM pairs p
-- LEFT JOIN judge_official_link existing ON existing.judge_id = p.isu_judge_id
-- WHERE existing.judge_id IS NULL
--    OR existing.status IS DISTINCT FROM 'linked'
--    OR existing.official_id IS DISTINCT FROM p.official_id
-- ON CONFLICT (judge_id) DO UPDATE SET
--     official_id = EXCLUDED.official_id,
--     status = 'linked',
--     note = EXCLUDED.note,
--     updated_at = NOW();

-- COMMIT;

-- Whitney Luke (production): if preview does not list judge 2711, ensure
-- judge_isu_official_link exists first, then re-run preview. Manual fallback:
--
-- INSERT INTO judge_official_link (judge_id, official_id, status, note, updated_at)
-- VALUES (2711, 815, 'linked', 'merged with US judge 126 (Whitney Luke)', NOW())
-- ON CONFLICT (judge_id) DO UPDATE SET
--     official_id = EXCLUDED.official_id,
--     status = 'linked',
--     note = EXCLUDED.note,
--     updated_at = NOW();
