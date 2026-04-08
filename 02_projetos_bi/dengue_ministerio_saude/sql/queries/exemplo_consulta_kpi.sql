SELECT
    municipio,
    COUNT(*) AS total_casos,
    MIN(data_notificacao) AS primeiro_registro,
    MAX(data_notificacao) AS ultimo_registro
FROM saude.fato_dengue_casos
GROUP BY municipio
ORDER BY total_casos DESC;
