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
    ]
    with get_engine().begin() as conn:
        for stmt in stmts:
            conn.execute(text(stmt))
