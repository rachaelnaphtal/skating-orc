-- Per-shard mergeable judge summaries (avoids re-annotating raw marks when σ̂ is cached).

CREATE TABLE IF NOT EXISTS element_deviation_ranking_shard_summary_cache (
    cache_key VARCHAR(24) PRIMARY KEY,
    shard_key VARCHAR(24) NOT NULL,
    sigma_key VARCHAR(24) NOT NULL,
    floor_sigma NUMERIC(8, 4) NOT NULL,
    data_fingerprint VARCHAR(64) NOT NULL,
    summary_payload BYTEA NOT NULL,
    n_marks INTEGER,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_elem_shard_summary_shard_sigma
    ON element_deviation_ranking_shard_summary_cache (shard_key, sigma_key);
