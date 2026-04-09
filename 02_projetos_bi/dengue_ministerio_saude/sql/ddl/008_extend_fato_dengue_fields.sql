ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS cs_sexo VARCHAR(1);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS id_unidade VARCHAR(20);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS hospitaliz VARCHAR(2);
