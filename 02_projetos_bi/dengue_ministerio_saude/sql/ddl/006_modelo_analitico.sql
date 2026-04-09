CREATE OR REPLACE VIEW saude.dim_territorio AS
SELECT
    d.cd_mun AS codigo_municipio_7,
    d.cd_mun6 AS codigo_municipio,
    d.nm_mun AS municipio,
    d.cd_uf AS codigo_uf,
    d.nm_uf AS uf_nome,
    d.nm_regiao AS regiao,
    d.nm_rgi AS rgi,
    d.nm_rgint AS rgint,
    d.total_pessoas AS populacao
FROM saude.dim_ibge_municipio d;

CREATE OR REPLACE VIEW saude.fato_dengue_analitica AS
WITH base AS (
    SELECT
        f.id AS id_caso,
        f.data_notificacao,
        NULL::DATE AS data_sintomas,
        EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
        EXTRACT(MONTH FROM f.data_notificacao)::INTEGER AS mes,
        f.semana_epidemiologica,
        COALESCE(NULLIF(TRIM(f.municipio), ''), 'Municipio nao informado') AS municipio_raw,
        COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') AS uf,
        f.classificacao_final,
        f.evolucao_caso AS evolucao,
        NULL::VARCHAR(1) AS hospitalizacao,
        NULL::INTEGER AS idade,
        NULL::VARCHAR(20) AS faixa_etaria,
        NULL::VARCHAR(1) AS sexo,
        NULL::VARCHAR(20) AS raca,
        NULL::VARCHAR(30) AS escolaridade,
        NULL::VARCHAR(1) AS comorbidade,
        NULL::VARCHAR(1) AS sintoma_febre,
        NULL::VARCHAR(1) AS sintoma_cefaleia,
        NULL::VARCHAR(1) AS exame_ns1,
        NULL::VARCHAR(1) AS exame_pcr,
        NULL::VARCHAR(1) AS exame_sorologia,
        NULL::DATE AS dt_encerramento,
        f.data_carga::DATE AS dt_digitacao,
        CASE
            WHEN f.municipio ~ '^[0-9]+$' THEN
                CASE
                    WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                    WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                    ELSE LPAD(f.municipio, 6, '0')
                END
            ELSE NULL
        END AS codigo_municipio
    FROM saude.fato_dengue_casos f
)
SELECT
    b.id_caso,
    b.data_notificacao,
    b.data_sintomas,
    b.ano,
    b.mes,
    b.semana_epidemiologica,
    COALESCE(t.municipio, b.municipio_raw) AS municipio,
    b.uf,
    t.regiao,
    t.rgi,
    t.rgint,
    b.classificacao_final,
    b.evolucao,
    b.hospitalizacao,
    b.idade,
    b.faixa_etaria,
    b.sexo,
    b.raca,
    b.escolaridade,
    b.comorbidade,
    b.sintoma_febre,
    b.sintoma_cefaleia,
    b.exame_ns1,
    b.exame_pcr,
    b.exame_sorologia,
    b.dt_encerramento,
    b.dt_digitacao,
    b.codigo_municipio,
    t.populacao
FROM base b
LEFT JOIN saude.dim_territorio t
    ON b.codigo_municipio = t.codigo_municipio;
