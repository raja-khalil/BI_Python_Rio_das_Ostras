CREATE TABLE IF NOT EXISTS saude.agg_dengue_mensal (
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    uf VARCHAR(10) NOT NULL,
    municipio VARCHAR(120) NOT NULL,
    codigo_municipio CHAR(6),
    regiao VARCHAR(40),
    rgi VARCHAR(160),
    rgint VARCHAR(160),
    populacao BIGINT,
    casos_total BIGINT NOT NULL,
    casos_confirmados BIGINT NOT NULL,
    casos_descartados BIGINT NOT NULL,
    casos_em_investigacao BIGINT NOT NULL,
    obitos BIGINT NOT NULL,
    internacoes BIGINT NOT NULL,
    incidencia NUMERIC(18,6),
    letalidade NUMERIC(18,6),
    taxa_internacao NUMERIC(18,6),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ano, mes, uf, municipio)
);

CREATE INDEX IF NOT EXISTS idx_agg_dengue_mensal_ano_mes
    ON saude.agg_dengue_mensal (ano, mes);

CREATE INDEX IF NOT EXISTS idx_agg_dengue_mensal_municipio
    ON saude.agg_dengue_mensal (municipio);

CREATE OR REPLACE FUNCTION saude.refresh_agg_dengue_mensal()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE saude.agg_dengue_mensal;

    INSERT INTO saude.agg_dengue_mensal (
        ano,
        mes,
        uf,
        municipio,
        codigo_municipio,
        regiao,
        rgi,
        rgint,
        populacao,
        casos_total,
        casos_confirmados,
        casos_descartados,
        casos_em_investigacao,
        obitos,
        internacoes,
        incidencia,
        letalidade,
        taxa_internacao
    )
    WITH agg AS (
        SELECT
            f.ano,
            f.mes,
            f.uf,
            f.municipio,
            f.codigo_municipio,
            COUNT(*)::BIGINT AS casos_total,
            COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI')
                IN ('1', '3', '4', '5', '6', '10', '11', '12')
            )::BIGINT AS casos_confirmados,
            COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = '2'
            )::BIGINT AS casos_descartados,
            COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI')
                IN ('0', '8', '9', 'NI')
            )::BIGINT AS casos_em_investigacao,
            COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(TRIM(f.evolucao), ''), 'NI') = '2'
            )::BIGINT AS obitos,
            COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(TRIM(f.hospitalizacao), ''), 'NI') = '1'
            )::BIGINT AS internacoes
        FROM saude.fato_dengue_analitica f
        WHERE f.data_notificacao IS NOT NULL
        GROUP BY
            f.ano,
            f.mes,
            f.uf,
            f.municipio,
            f.codigo_municipio
    )
    SELECT
        a.ano,
        a.mes,
        a.uf,
        a.municipio,
        a.codigo_municipio,
        t.regiao,
        t.rgi,
        t.rgint,
        t.populacao,
        a.casos_total,
        a.casos_confirmados,
        a.casos_descartados,
        a.casos_em_investigacao,
        a.obitos,
        a.internacoes,
        CASE
            WHEN t.populacao > 0 THEN (a.casos_confirmados::NUMERIC / t.populacao::NUMERIC) * 100000
            ELSE NULL
        END AS incidencia,
        CASE
            WHEN a.casos_confirmados > 0 THEN (a.obitos::NUMERIC / a.casos_confirmados::NUMERIC) * 100
            ELSE NULL
        END AS letalidade,
        CASE
            WHEN a.casos_confirmados > 0 THEN (a.internacoes::NUMERIC / a.casos_confirmados::NUMERIC) * 100
            ELSE NULL
        END AS taxa_internacao
    FROM agg a
    LEFT JOIN saude.dim_territorio t
        ON a.codigo_municipio = t.codigo_municipio;
END;
$$;
