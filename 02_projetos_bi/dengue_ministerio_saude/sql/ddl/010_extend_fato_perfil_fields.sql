ALTER TABLE saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS nu_idade_n VARCHAR(8);

ALTER TABLE saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS cs_gestant VARCHAR(2);

ALTER TABLE saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS cs_raca VARCHAR(2);

ALTER TABLE saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS cs_escol_n VARCHAR(4);
