"""Funcoes reutilizaveis de limpeza e padronizacao."""

from __future__ import annotations

import re

import pandas as pd


def _normalizar_nome_coluna(nome: str) -> str:
    normalized = re.sub(r"_+", "_", re.sub(r"\W+", "_", nome.strip().lower())).strip("_")
    return normalized or "coluna"


def _garantir_colunas_unicas(colunas: list[str]) -> list[str]:
    counters: dict[str, int] = {}
    output: list[str] = []

    for coluna in colunas:
        if coluna not in counters:
            counters[coluna] = 0
            output.append(coluna)
            continue

        counters[coluna] += 1
        output.append(f"{coluna}_{counters[coluna]}")

    return output


def normalizar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas para snake_case."""
    normalized = [_normalizar_nome_coluna(str(col)) for col in df.columns]
    unique_columns = _garantir_colunas_unicas(normalized)
    output = df.copy()
    output.columns = unique_columns
    return output


def tratar_espacos(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espacos excedentes em colunas textuais."""
    text_cols = list(df.select_dtypes(include=["object", "string"]).columns)
    for col in text_cols:
        df[col] = df[col].astype("string").str.strip()
    return df


def tratar_nulos(df: pd.DataFrame, fill_value: str = "") -> pd.DataFrame:
    """Aplica preenchimento padrao para valores nulos textuais."""
    text_cols = list(df.select_dtypes(include=["object", "string"]).columns)
    for col in text_cols:
        df[col] = df[col].fillna(fill_value)
    return df


def padronizar_datas(df: pd.DataFrame, colunas_data: list[str] | None = None) -> pd.DataFrame:
    """Converte colunas de data para datetime quando possivel."""
    candidates = colunas_data or [col for col in df.columns if "data" in col.lower()]
    for col in candidates:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def padronizar_texto(df: pd.DataFrame, uppercase: bool = False) -> pd.DataFrame:
    """Padroniza capitalizacao de colunas textuais."""
    text_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in text_cols:
        serie = df[col].astype("string")
        df[col] = serie.str.upper() if uppercase else serie.str.title()
    return df


def pipeline_limpeza_padrao(df: pd.DataFrame) -> pd.DataFrame:
    """Executa pipeline default de limpeza."""
    cleaned = df.copy()
    cleaned = normalizar_nomes_colunas(cleaned)
    cleaned = tratar_espacos(cleaned)
    cleaned = tratar_nulos(cleaned)
    cleaned = padronizar_datas(cleaned)
    return cleaned
