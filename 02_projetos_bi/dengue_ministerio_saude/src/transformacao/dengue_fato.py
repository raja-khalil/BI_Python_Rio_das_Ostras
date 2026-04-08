"""Conformidade de esquema para carga da fato de dengue."""

from __future__ import annotations

import pandas as pd


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _to_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64")


def preparar_fato_dengue(df: pd.DataFrame) -> pd.DataFrame:
    """Monta DataFrame final aderente ao schema saude.fato_dengue_casos."""
    output = pd.DataFrame(index=df.index)

    municipio = df.get("municipio")
    id_municip = df.get("id_municip")
    if municipio is None and id_municip is None:
        output["municipio"] = "NAO INFORMADO"
    elif municipio is None:
        output["municipio"] = id_municip.astype("string")
    else:
        output["municipio"] = municipio.astype("string")
        if id_municip is not None:
            mask_empty = output["municipio"].str.strip().fillna("") == ""
            output.loc[mask_empty, "municipio"] = id_municip.astype("string")

    output["municipio"] = output["municipio"].fillna("NAO INFORMADO")
    output["uf"] = df.get("sg_uf_not", df.get("sg_uf", "")).astype("string").str[:2]
    output["data_notificacao"] = _to_date(df.get("dt_notific", pd.Series(index=df.index)))
    output["semana_epidemiologica"] = _to_int(df.get("sem_not", pd.Series(index=df.index)))
    output["classificacao_final"] = df.get("classi_fin", pd.Series(index=df.index)).astype("string")
    output["evolucao_caso"] = df.get("evolucao", pd.Series(index=df.index)).astype("string")

    return output
