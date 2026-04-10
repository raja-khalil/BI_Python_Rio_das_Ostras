ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS diabetes VARCHAR(1);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS hematolog VARCHAR(1);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS hepatopat VARCHAR(1);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS renal VARCHAR(1);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS hipertensa VARCHAR(1);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS acido_pept VARCHAR(1);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS auto_imune VARCHAR(1);
