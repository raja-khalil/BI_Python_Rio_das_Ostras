"""Utilitarios de evolucao de schema do banco."""

from __future__ import annotations

from sqlalchemy import text

from src.banco.database import get_engine


def garantir_colunas_fato_dengue(schema: str = "saude", table_name: str = "fato_dengue_casos") -> None:
    """Garante colunas adicionais da fato usadas no BI."""
    stmts = [
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS cs_sexo VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS nu_idade_n VARCHAR(8)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS cs_gestant VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS cs_raca VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS cs_escol_n VARCHAR(4)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS id_unidade VARCHAR(20)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS hospitaliz VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS febre VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS mialgia VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS cefaleia VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS exantema VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS vomito VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS nausea VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dor_costas VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS conjuntvit VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS artrite VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS artralgia VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS petequia_n VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS leucopenia VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS laco VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dor_retro VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS resul_soro VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_chik_s1 DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_chik_s2 DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_prnt DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS res_chiks1 VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS res_chiks2 VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS resul_prnt VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_soro DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS resul_ns1 VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_ns1 DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_viral DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS resul_vi_n VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_pcr DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS resul_pcr_ VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS sorotipo VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS histopa_n VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS imunoh_n VARCHAR(2)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS dt_interna DATE",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS diabetes VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS hematolog VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS hepatopat VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS renal VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS hipertensa VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS acido_pept VARCHAR(1)",
        f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS auto_imune VARCHAR(1)",
    ]
    with get_engine().begin() as conn:
        for stmt in stmts:
            conn.execute(text(stmt))
