CREATE OR REPLACE VIEW saude.vw_dengue_resumo_mensal AS
SELECT
    municipio,
    DATE_TRUNC('month', data_notificacao)::date AS mes_referencia,
    COUNT(*) AS total_casos
FROM saude.fato_dengue_casos
GROUP BY municipio, DATE_TRUNC('month', data_notificacao)::date;
