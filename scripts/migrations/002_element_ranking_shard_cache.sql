-- Per-season, per-discipline marks shards for element deviation ranking.

CREATE TABLE IF NOT EXISTS element_deviation_ranking_shard_cache (
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

CREATE INDEX IF NOT EXISTS idx_element_ranking_shard_season_disc
    ON element_deviation_ranking_shard_cache (season_year, discipline_type_id);
