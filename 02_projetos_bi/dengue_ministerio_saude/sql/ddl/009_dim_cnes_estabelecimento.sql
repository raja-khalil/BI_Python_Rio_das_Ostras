CREATE TABLE IF NOT EXISTS saude.dim_cnes_estabelecimento (
    cnes VARCHAR(20) PRIMARY KEY,
    nome_fantasia VARCHAR(255),
    nome_empresarial VARCHAR(255),
    razao_social VARCHAR(255),
    uf VARCHAR(2),
    municipio VARCHAR(120),
    competencia VARCHAR(20),
    fonte VARCHAR(80),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_cnes_estab_uf ON saude.dim_cnes_estabelecimento (uf);
CREATE INDEX IF NOT EXISTS idx_dim_cnes_estab_municipio ON saude.dim_cnes_estabelecimento (municipio);
