ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_chik_s1 DATE;

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_chik_s2 DATE;

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_prnt DATE;

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS res_chiks1 VARCHAR(2);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS res_chiks2 VARCHAR(2);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS resul_prnt VARCHAR(2);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_soro DATE;

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_ns1 DATE;

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_viral DATE;

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_pcr DATE;

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS sorotipo VARCHAR(2);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS histopa_n VARCHAR(2);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS imunoh_n VARCHAR(2);

ALTER TABLE IF EXISTS saude.fato_dengue_casos
    ADD COLUMN IF NOT EXISTS dt_interna DATE;
