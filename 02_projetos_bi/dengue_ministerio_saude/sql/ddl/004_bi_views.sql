CREATE OR REPLACE VIEW saude.vw_dengue_ano AS
SELECT
    EXTRACT(YEAR FROM data_notificacao)::INTEGER AS ano,
    COUNT(*)::BIGINT AS total_casos
FROM saude.fato_dengue_casos
WHERE data_notificacao IS NOT NULL
GROUP BY 1;

CREATE OR REPLACE VIEW saude.vw_dengue_mes_uf AS
SELECT
    EXTRACT(YEAR FROM data_notificacao)::INTEGER AS ano,
    DATE_TRUNC('month', data_notificacao)::DATE AS mes_referencia,
    COALESCE(NULLIF(TRIM(uf), ''), 'NI') AS uf,
    COUNT(*)::BIGINT AS total_casos
FROM saude.fato_dengue_casos
WHERE data_notificacao IS NOT NULL
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW saude.vw_dengue_municipio_ano AS
SELECT
    EXTRACT(YEAR FROM data_notificacao)::INTEGER AS ano,
    COALESCE(NULLIF(TRIM(uf), ''), 'NI') AS uf,
    COALESCE(NULLIF(TRIM(municipio), ''), 'Municipio nao informado') AS municipio,
    COUNT(*)::BIGINT AS total_casos
FROM saude.fato_dengue_casos
WHERE data_notificacao IS NOT NULL
GROUP BY 1, 2, 3;
