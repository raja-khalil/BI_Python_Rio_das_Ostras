CREATE TABLE IF NOT EXISTS saude.dim_ibge_municipio (
    cd_mun CHAR(7) PRIMARY KEY,
    cd_mun6 CHAR(6) NOT NULL,
    nm_mun VARCHAR(120) NOT NULL,
    cd_regiao SMALLINT,
    nm_regiao VARCHAR(40),
    cd_uf CHAR(2),
    nm_uf VARCHAR(60),
    cd_nu VARCHAR(16),
    nm_nu VARCHAR(160),
    cd_aglom VARCHAR(16),
    nm_aglom VARCHAR(160),
    cd_rgint VARCHAR(16),
    nm_rgint VARCHAR(160),
    cd_rgi VARCHAR(16),
    nm_rgi VARCHAR(160),
    cd_concurb VARCHAR(16),
    nm_concurb VARCHAR(160),
    area_km2 NUMERIC(18,6),
    total_pessoas BIGINT,
    total_domicilios BIGINT,
    total_domicilios_particulares BIGINT,
    total_domicilios_coletivos BIGINT,
    media_moradores_dom_part_ocup NUMERIC(10,4),
    perc_dom_part_ocup_imputados NUMERIC(10,6),
    total_dom_part_ocupados BIGINT,
    fonte VARCHAR(120) NOT NULL DEFAULT 'IBGE_CENSO_2022',
    data_carga TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_ibge_municipio_cd_mun6
    ON saude.dim_ibge_municipio (cd_mun6);

CREATE INDEX IF NOT EXISTS idx_dim_ibge_municipio_cd_uf
    ON saude.dim_ibge_municipio (cd_uf);
