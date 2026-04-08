"""Regras de validacao de DataFrames."""

from __future__ import annotations

import pandas as pd


class ValidationError(Exception):
    """Erro de validacao de dados."""


def validar_dataframe_nao_vazio(df: pd.DataFrame) -> None:
    """Garante que DataFrame possui registros."""
    if df.empty:
        raise ValidationError("DataFrame vazio apos ingestao/transformacao")


def validar_colunas_obrigatorias(df: pd.DataFrame, colunas_obrigatorias: list[str]) -> None:
    """Valida presenca de colunas obrigatorias."""
    missing = [col for col in colunas_obrigatorias if col not in df.columns]
    if missing:
        raise ValidationError(f"Colunas obrigatorias ausentes: {missing}")


def validar_tipos_basicos(df: pd.DataFrame, tipo_por_coluna: dict[str, str]) -> None:
    """Valida tipos esperados com base em prefixo de dtype pandas."""
    erros: list[str] = []

    for coluna, tipo_esperado in tipo_por_coluna.items():
        if coluna not in df.columns:
            erros.append(f"Coluna inexistente: {coluna}")
            continue

        dtype_real = str(df[coluna].dtype)
        if not dtype_real.startswith(tipo_esperado):
            erros.append(
                f"Coluna {coluna} com tipo {dtype_real}. Esperado prefixo: {tipo_esperado}"
            )

    if erros:
        raise ValidationError(" | ".join(erros))
