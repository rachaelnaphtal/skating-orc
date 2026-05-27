-- Per-competition × discipline × judge aggregates for cross-judge benchmarking.

CREATE TABLE IF NOT EXISTS cross_judge_competition_shard (
    competition_id INTEGER NOT NULL,
    discipline_type_id INTEGER NOT NULL,
    judge_id INTEGER NOT NULL,
    competition_year VARCHAR(8) NOT NULL,
    pcs_total INTEGER NOT NULL DEFAULT 0,
    pcs_throwouts INTEGER NOT NULL DEFAULT 0,
    pcs_anomalies INTEGER NOT NULL DEFAULT 0,
    pcs_rule_errors INTEGER NOT NULL DEFAULT 0,
    pcs_sum_deviation DOUBLE PRECISION NOT NULL DEFAULT 0,
    pcs_sum_abs_deviation DOUBLE PRECISION NOT NULL DEFAULT 0,
    elem_total INTEGER NOT NULL DEFAULT 0,
    elem_throwouts INTEGER NOT NULL DEFAULT 0,
    elem_anomalies INTEGER NOT NULL DEFAULT 0,
    elem_rule_errors INTEGER NOT NULL DEFAULT 0,
    elem_sum_deviation DOUBLE PRECISION NOT NULL DEFAULT 0,
    elem_sum_abs_deviation DOUBLE PRECISION NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (competition_id, discipline_type_id, judge_id)
);

CREATE INDEX IF NOT EXISTS idx_cross_judge_shard_competition
    ON cross_judge_competition_shard (competition_id);

CREATE INDEX IF NOT EXISTS idx_cross_judge_shard_year
    ON cross_judge_competition_shard (competition_year);
