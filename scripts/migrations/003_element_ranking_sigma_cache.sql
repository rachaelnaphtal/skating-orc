-- σ̂ benchmark parameters (fitted on a wide season×discipline mark pool).

CREATE TABLE IF NOT EXISTS element_deviation_ranking_sigma_cache (
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

CREATE INDEX IF NOT EXISTS idx_element_ranking_sigma_seasons
    ON element_deviation_ranking_sigma_cache (
        benchmark_start_season_year,
        benchmark_end_season_year
    );
