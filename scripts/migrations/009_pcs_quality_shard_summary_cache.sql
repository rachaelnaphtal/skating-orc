-- Per-shard mergeable PCS component stats (avoids loading raw marks on cache-only reads).

CREATE TABLE IF NOT EXISTS pcs_quality_shard_summary_cache (
    cache_key VARCHAR(24) PRIMARY KEY,
    shard_key VARCHAR(24) NOT NULL UNIQUE,
    data_fingerprint VARCHAR(64) NOT NULL,
    summary_payload BYTEA NOT NULL,
    n_marks INTEGER,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pcs_quality_summary_shard
    ON pcs_quality_shard_summary_cache (shard_key);
