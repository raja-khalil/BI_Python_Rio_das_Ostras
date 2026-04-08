CREATE SCHEMA IF NOT EXISTS saude;

CREATE TABLE IF NOT EXISTS saude.bi_pipeline_watermark (
    pipeline_name VARCHAR(120) NOT NULL,
    data_source VARCHAR(120) NOT NULL,
    last_success_date DATE,
    last_run_started_at TIMESTAMPTZ,
    last_run_finished_at TIMESTAMPTZ,
    last_status VARCHAR(20),
    last_row_count BIGINT,
    last_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, data_source)
);

CREATE TABLE IF NOT EXISTS saude.bi_pipeline_execucoes (
    run_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(120) NOT NULL,
    data_source VARCHAR(120) NOT NULL,
    execution_mode VARCHAR(30) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL,
    rows_loaded BIGINT NOT NULL DEFAULT 0,
    message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_execucoes_pipeline_started
    ON saude.bi_pipeline_execucoes (pipeline_name, started_at DESC);
