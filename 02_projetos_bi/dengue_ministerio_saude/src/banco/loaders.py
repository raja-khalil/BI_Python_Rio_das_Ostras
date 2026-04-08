"""Funcoes de carga de DataFrames para PostgreSQL."""

from __future__ import annotations

from datetime import date
from typing import Literal

import pandas as pd
from sqlalchemy import text

from src.banco.database import get_engine


IfExistsMode = Literal["append", "replace", "fail"]


def carregar_dataframe_postgres(
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    if_exists: IfExistsMode = "append",
    chunksize: int = 5_000,
) -> None:
    """Carrega DataFrame em tabela PostgreSQL com parametros reutilizaveis."""
    if df.empty:
        return

    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=chunksize,
    )


def truncar_tabela(schema: str, table_name: str) -> None:
    """Executa truncate seguro para recarga historica full."""
    engine = get_engine()
    stmt = text(f"TRUNCATE TABLE {schema}.{table_name}")
    with engine.begin() as conn:
        conn.execute(stmt)


def deletar_por_intervalo_data(
    schema: str,
    table_name: str,
    coluna_data: str,
    data_inicio: date,
    data_fim: date | None = None,
) -> int:
    """Remove janela de dados antes de recarga incremental (evita duplicidade)."""
    engine = get_engine()
    params: dict[str, object] = {"data_inicio": data_inicio}

    if data_fim:
        stmt = text(
            f"""
            DELETE FROM {schema}.{table_name}
            WHERE {coluna_data} >= :data_inicio
              AND {coluna_data} <= :data_fim
            """
        )
        params["data_fim"] = data_fim
    else:
        stmt = text(
            f"""
            DELETE FROM {schema}.{table_name}
            WHERE {coluna_data} >= :data_inicio
            """
        )

    with engine.begin() as conn:
        result = conn.execute(stmt, params)
        return int(result.rowcount or 0)
