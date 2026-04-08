"""Indicadores base para analise de dengue."""

from __future__ import annotations

import pandas as pd


def calcular_total_casos(df: pd.DataFrame) -> int:
    """Retorna total de registros de casos."""
    return int(len(df))


def calcular_casos_por_periodo(df: pd.DataFrame, coluna_data: str) -> pd.DataFrame:
    """Agrupa casos por periodo mensal."""
    serie_data = pd.to_datetime(df[coluna_data], errors="coerce")
    resultado = (
        df.assign(_periodo=serie_data.dt.to_period("M").astype("string"))
        .groupby("_periodo", as_index=False)
        .size()
        .rename(columns={"size": "casos"})
    )
    return resultado


def calcular_incidencia(
    total_casos: int,
    populacao: int,
    multiplicador: int = 100_000,
) -> float:
    """Calcula incidencia padronizada por populacao."""
    if populacao <= 0:
        return 0.0
    return (total_casos / populacao) * multiplicador


def calcular_comparativo(df_atual: pd.DataFrame, df_referencia: pd.DataFrame) -> dict[str, float]:
    """Retorna comparativo simples entre volumes de dois periodos."""
    total_atual = float(len(df_atual))
    total_referencia = float(len(df_referencia))

    if total_referencia == 0:
        variacao_percentual = 0.0
    else:
        variacao_percentual = ((total_atual - total_referencia) / total_referencia) * 100

    return {
        "total_atual": total_atual,
        "total_referencia": total_referencia,
        "variacao_percentual": variacao_percentual,
    }
