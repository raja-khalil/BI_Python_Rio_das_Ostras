CREATE SCHEMA IF NOT EXISTS saude;

CREATE TABLE IF NOT EXISTS saude.fato_dengue_casos (
    id BIGSERIAL PRIMARY KEY,
    municipio VARCHAR(120) NOT NULL,
    uf CHAR(2),
    data_notificacao DATE,
    semana_epidemiologica INTEGER,
    classificacao_final VARCHAR(80),
    evolucao_caso VARCHAR(80),
    data_carga TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fato_dengue_data_notificacao
    ON saude.fato_dengue_casos (data_notificacao);

CREATE INDEX IF NOT EXISTS idx_fato_dengue_municipio
    ON saude.fato_dengue_casos (municipio);
