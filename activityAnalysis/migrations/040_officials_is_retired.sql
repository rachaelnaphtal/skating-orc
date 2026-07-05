-- Retired USFS officials (not in the active directory; synced from Retired_officials.xlsx).
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/040_officials_is_retired.sql

SET lock_timeout = '60s';

-- Remove mistaken column if an earlier draft of this migration was applied.
ALTER TABLE officials_analysis.officials
    DROP COLUMN IF EXISTS is_retired;

DROP INDEX IF EXISTS officials_analysis.idx_officials_is_retired;

CREATE TABLE IF NOT EXISTS officials_analysis.retired_official (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    display_name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    source_workbook TEXT NOT NULL DEFAULT 'activityAnalysis/Retired_officials.xlsx',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_modified TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT retired_official_name_normalized_key UNIQUE (name_normalized)
);

COMMENT ON TABLE officials_analysis.retired_official IS
    'Retired officials from Retired_officials.xlsx. Not members of officials_analysis.officials.';

COMMENT ON COLUMN officials_analysis.retired_official.name_normalized IS
    'Lowercase normalized display name for matching assignment / protocol spellings.';

CREATE INDEX IF NOT EXISTS idx_retired_official_name_normalized
    ON officials_analysis.retired_official (name_normalized);
