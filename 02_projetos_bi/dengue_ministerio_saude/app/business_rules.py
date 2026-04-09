"""Regras de negocio e recortes oficiais do dashboard."""

from __future__ import annotations

import pandas as pd


TARGET_MUNICIPIO_NOME = "Rio das Ostras"
TARGET_MUNICIPIO_IBGE = "3304524"
TARGET_MUNICIPIO_PREFIX = "330452"
TARGET_UF_SIGLA = "RJ"
TARGET_UF_CODIGO = "33"


def _normalize_str(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def mask_uf_rj(series_uf: pd.Series) -> pd.Series:
    """Retorna mascara booleana para registros do estado do Rio de Janeiro."""
    uf_norm = series_uf.map(_normalize_str)
    return uf_norm.isin({TARGET_UF_SIGLA, TARGET_UF_CODIGO})


def mask_municipio_rio_das_ostras(series_municipio: pd.Series) -> pd.Series:
    """Retorna mascara booleana para registros de Rio das Ostras."""
    municipio_norm = series_municipio.map(_normalize_str)
    return (
        (municipio_norm == TARGET_MUNICIPIO_IBGE)
        | municipio_norm.str.startswith(TARGET_MUNICIPIO_PREFIX)
        | (municipio_norm == "RIO DAS OSTRAS")
    )
