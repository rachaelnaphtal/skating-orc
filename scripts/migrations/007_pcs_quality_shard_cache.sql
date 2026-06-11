-- Per-season, per-discipline PCS marks shards for PCS quality analysis.

CREATE TABLE IF NOT EXISTS pcs_quality_shard_cache (
    shard_key VARCHAR(24) PRIMARY KEY,
    season_year VARCHAR(8) NOT NULL,
    discipline_type_id INTEGER NOT NULL,
    competition_scope VARCHAR(32) NOT NULL,
    event_start_iso VARCHAR(10),
    event_end_iso VARCHAR(10),
    data_fingerprint VARCHAR(64) NOT NULL,
    marks_payload BYTEA NOT NULL,
    n_marks INTEGER,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pcs_quality_shard_season_disc
    ON pcs_quality_shard_cache (season_year, discipline_type_id);
