-- Map a results/protocol name (normalized) to directory official when they differ
-- (e.g. name change). Used by DatabaseLoader.replace_segment_officials when resolving
-- official_id. Normalize aliases the same way as application code: lowercase, single
-- spaces between tokens (see database_loader._normalize_person_name).
--
-- Example:
--   INSERT INTO public.official_name_alias (alias_normalized, official_id, note)
--   VALUES (
--     lower(regexp_replace(trim('Former Legal Name'), '\s+', ' ', 'g')),
--     12345,
--     'Married name on protocol'
--   );
-- Or from Python: alias_normalized = " ".join("Former Legal Name".lower().split())
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/004_public_official_name_alias.sql

CREATE TABLE IF NOT EXISTS public.official_name_alias (
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    alias_normalized text NOT NULL,
    official_id integer NOT NULL,
    note text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT official_name_alias_pkey PRIMARY KEY (id),
    CONSTRAINT official_name_alias_alias_normalized_key UNIQUE (alias_normalized),
    CONSTRAINT official_name_alias_official_id_fkey
        FOREIGN KEY (official_id)
        REFERENCES officials_analysis.officials(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_official_name_alias_official_id
    ON public.official_name_alias (official_id);
