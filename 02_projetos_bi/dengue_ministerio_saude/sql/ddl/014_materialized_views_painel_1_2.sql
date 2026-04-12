-- Materialized views de alta performance para os paineis 1 (Situacao Geral)
-- e 2 (Territorio e Risco).

DROP MATERIALIZED VIEW IF EXISTS saude.mv_painel1_mes_uf_classif;
CREATE MATERIALIZED VIEW saude.mv_painel1_mes_uf_classif AS
SELECT
    EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
    DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
    COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') AS uf,
    COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') AS classificacao_final,
    COUNT(*)::BIGINT AS total_casos
FROM saude.fato_dengue_casos f
WHERE f.data_notificacao IS NOT NULL
GROUP BY 1, 2, 3, 4;

CREATE INDEX IF NOT EXISTS idx_mv_p1_mes_uf_classif_mes
    ON saude.mv_painel1_mes_uf_classif (mes_referencia);
CREATE INDEX IF NOT EXISTS idx_mv_p1_mes_uf_classif_uf
    ON saude.mv_painel1_mes_uf_classif (uf);
CREATE INDEX IF NOT EXISTS idx_mv_p1_mes_uf_classif_classif
    ON saude.mv_painel1_mes_uf_classif (classificacao_final);


DROP MATERIALIZED VIEW IF EXISTS saude.mv_painel1_2_mes_municipio_rj;
CREATE MATERIALIZED VIEW saude.mv_painel1_2_mes_municipio_rj AS
WITH base AS (
    SELECT
        EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
        DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
        COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') AS uf,
        COALESCE(NULLIF(TRIM(f.municipio), ''), 'Municipio nao informado') AS municipio_raw,
        COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') AS classificacao_final,
        COALESCE(NULLIF(TRIM(f.evolucao_caso), ''), 'NI') AS evolucao_caso
    FROM saude.fato_dengue_casos f
    WHERE f.data_notificacao IS NOT NULL
      AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
),
base_norm AS (
    SELECT
        b.*,
        CASE
            WHEN b.municipio_raw ~ '^[0-9]+$' THEN
                CASE
                    WHEN LENGTH(b.municipio_raw) >= 7 THEN SUBSTRING(b.municipio_raw FROM 1 FOR 6)
                    WHEN LENGTH(b.municipio_raw) = 6 THEN b.municipio_raw
                    ELSE LPAD(b.municipio_raw, 6, '0')
                END
            ELSE NULL
        END AS cd_mun6
    FROM base b
)
SELECT
    b.ano,
    b.mes_referencia,
    b.uf,
    b.municipio_raw AS municipio_codigo,
    COALESCE(d.nm_mun, b.municipio_raw) AS municipio_nome,
    b.cd_mun6,
    d.total_pessoas,
    d.area_km2,
    b.classificacao_final,
    b.evolucao_caso,
    COUNT(*)::BIGINT AS total_casos
FROM base_norm b
LEFT JOIN saude.dim_ibge_municipio d
    ON b.cd_mun6 = d.cd_mun6
GROUP BY
    b.ano,
    b.mes_referencia,
    b.uf,
    b.municipio_raw,
    COALESCE(d.nm_mun, b.municipio_raw),
    b.cd_mun6,
    d.total_pessoas,
    d.area_km2,
    b.classificacao_final,
    b.evolucao_caso;

CREATE INDEX IF NOT EXISTS idx_mv_p1_2_mes_mun_rj_mes
    ON saude.mv_painel1_2_mes_municipio_rj (mes_referencia);
CREATE INDEX IF NOT EXISTS idx_mv_p1_2_mes_mun_rj_nome
    ON saude.mv_painel1_2_mes_municipio_rj (municipio_nome);
CREATE INDEX IF NOT EXISTS idx_mv_p1_2_mes_mun_rj_classif
    ON saude.mv_painel1_2_mes_municipio_rj (classificacao_final);


DROP MATERIALIZED VIEW IF EXISTS saude.mv_painel2_mes_unidade_municipio_rj;
CREATE MATERIALIZED VIEW saude.mv_painel2_mes_unidade_municipio_rj AS
WITH base AS (
    SELECT
        EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
        DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
        COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') AS uf,
        COALESCE(NULLIF(TRIM(f.municipio), ''), 'Municipio nao informado') AS municipio_raw,
        COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') AS classificacao_final,
        COALESCE(NULLIF(TRIM(f.id_unidade), ''), 'NI') AS unidade_notificadora
    FROM saude.fato_dengue_casos f
    WHERE f.data_notificacao IS NOT NULL
      AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
),
base_norm AS (
    SELECT
        b.*,
        CASE
            WHEN b.municipio_raw ~ '^[0-9]+$' THEN
                CASE
                    WHEN LENGTH(b.municipio_raw) >= 7 THEN SUBSTRING(b.municipio_raw FROM 1 FOR 6)
                    WHEN LENGTH(b.municipio_raw) = 6 THEN b.municipio_raw
                    ELSE LPAD(b.municipio_raw, 6, '0')
                END
            ELSE NULL
        END AS cd_mun6
    FROM base b
)
SELECT
    b.ano,
    b.mes_referencia,
    b.uf,
    b.municipio_raw AS municipio_codigo,
    COALESCE(d.nm_mun, b.municipio_raw) AS municipio_nome,
    b.cd_mun6,
    b.classificacao_final,
    b.unidade_notificadora,
    COUNT(*)::BIGINT AS total_casos
FROM base_norm b
LEFT JOIN saude.dim_ibge_municipio d
    ON b.cd_mun6 = d.cd_mun6
GROUP BY
    b.ano,
    b.mes_referencia,
    b.uf,
    b.municipio_raw,
    COALESCE(d.nm_mun, b.municipio_raw),
    b.cd_mun6,
    b.classificacao_final,
    b.unidade_notificadora;

CREATE INDEX IF NOT EXISTS idx_mv_p2_unid_mes
    ON saude.mv_painel2_mes_unidade_municipio_rj (mes_referencia);
CREATE INDEX IF NOT EXISTS idx_mv_p2_unid_municipio_nome
    ON saude.mv_painel2_mes_unidade_municipio_rj (municipio_nome);
CREATE INDEX IF NOT EXISTS idx_mv_p2_unid_id
    ON saude.mv_painel2_mes_unidade_municipio_rj (unidade_notificadora);
CREATE INDEX IF NOT EXISTS idx_mv_p2_unid_classif
    ON saude.mv_painel2_mes_unidade_municipio_rj (classificacao_final);


CREATE OR REPLACE FUNCTION saude.refresh_mvs_painel_1_2()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW saude.mv_painel1_mes_uf_classif;
    REFRESH MATERIALIZED VIEW saude.mv_painel1_2_mes_municipio_rj;
    REFRESH MATERIALIZED VIEW saude.mv_painel2_mes_unidade_municipio_rj;
END;
$$;
