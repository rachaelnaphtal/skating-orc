-- Element deviation ranking precompute cache (pickled payloads).
-- Run once against the judging database, or rely on ensure_element_ranking_cache_table().

CREATE TABLE IF NOT EXISTS element_deviation_ranking_cache (
    cache_key VARCHAR(24) PRIMARY KEY,
    start_season_year VARCHAR(8),
    end_season_year VARCHAR(8),
    run_params_json TEXT NOT NULL,
    data_fingerprint VARCHAR(64) NOT NULL,
    result_payload BYTEA NOT NULL,
    ctrl_payload BYTEA,
    params_payload BYTEA,
    n_raw_marks INTEGER,
    n_judges INTEGER,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_element_ranking_cache_seasons
    ON element_deviation_ranking_cache (start_season_year, end_season_year);
