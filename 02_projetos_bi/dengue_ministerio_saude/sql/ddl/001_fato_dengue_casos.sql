CREATE SCHEMA IF NOT EXISTS saude;

CREATE TABLE IF NOT EXISTS saude.fato_dengue_casos (
    id BIGSERIAL PRIMARY KEY,
    municipio VARCHAR(120) NOT NULL,
    uf CHAR(2),
    data_notificacao DATE,
    semana_epidemiologica INTEGER,
    classificacao_final VARCHAR(80),
    evolucao_caso VARCHAR(80),
    cs_sexo VARCHAR(1),
    nu_idade_n VARCHAR(8),
    cs_gestant VARCHAR(2),
    cs_raca VARCHAR(2),
    cs_escol_n VARCHAR(4),
    id_unidade VARCHAR(20),
    hospitaliz VARCHAR(2),
    data_carga TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fato_dengue_data_notificacao
    ON saude.fato_dengue_casos (data_notificacao);

CREATE INDEX IF NOT EXISTS idx_fato_dengue_municipio
    ON saude.fato_dengue_casos (municipio);
