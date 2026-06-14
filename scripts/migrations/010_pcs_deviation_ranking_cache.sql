-- PCS deviation ranking shard, σ̂, and summary caches.

CREATE TABLE IF NOT EXISTS pcs_deviation_ranking_shard_cache (
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

CREATE INDEX IF NOT EXISTS idx_pcs_deviation_ranking_shard_season_disc
    ON pcs_deviation_ranking_shard_cache (season_year, discipline_type_id);

CREATE TABLE IF NOT EXISTS pcs_deviation_ranking_sigma_cache (
    sigma_key VARCHAR(24) PRIMARY KEY,
    benchmark_start_season_year VARCHAR(8),
    benchmark_end_season_year VARCHAR(8),
    scope_json TEXT NOT NULL,
    data_fingerprint VARCHAR(64) NOT NULL,
    params_payload BYTEA NOT NULL,
    floor_sigma NUMERIC(8, 4),
    min_bin_count INTEGER,
    n_marks INTEGER,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pcs_deviation_ranking_sigma_seasons
    ON pcs_deviation_ranking_sigma_cache (benchmark_start_season_year, benchmark_end_season_year);

CREATE TABLE IF NOT EXISTS pcs_deviation_ranking_shard_summary_cache (
    cache_key VARCHAR(24) PRIMARY KEY,
    shard_key VARCHAR(24) NOT NULL,
    sigma_key VARCHAR(24) NOT NULL,
    floor_sigma NUMERIC(8, 4) NOT NULL,
    data_fingerprint VARCHAR(64) NOT NULL,
    summary_payload BYTEA NOT NULL,
    n_marks INTEGER,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pcs_deviation_shard_summary_shard_sigma
    ON pcs_deviation_ranking_shard_summary_cache (shard_key, sigma_key);
