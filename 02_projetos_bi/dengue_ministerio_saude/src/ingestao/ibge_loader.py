"""Carga da dimensao de municipios IBGE para enriquecimento do BI."""

from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import pandas as pd
from sqlalchemy import text

from src.banco.database import get_engine
from src.config.settings import get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)

TARGET_TABLE = "dim_ibge_municipio"


def _normalize_colname(name: str) -> str:
    cleaned = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", cleaned.strip().lower()).strip("_")
    return cleaned


def _digits_or_none(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip()
    if raw in {"", ".", "nan", "None"}:
        return None
    digits = re.sub(r"\D+", "", raw)
    return digits or None


def _to_decimal(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip()
    if raw in {"", ".", "nan", "None"}:
        return None
    raw = raw.replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _to_int(value: object) -> int | None:
    dec = _to_decimal(value)
    if dec is None:
        return None
    return int(round(dec))


def read_ibge_csv(path: Path) -> pd.DataFrame:
    """Le CSV do IBGE e retorna dataframe padronizado para a dimensao."""
    raw = pd.read_csv(path, encoding="utf-8")
    raw.columns = [_normalize_colname(c) for c in raw.columns]

    rename_map = {
        "codigo_do_municipio": "cd_mun",
        "nome_do_municipio": "nm_mun",
        "codigo_da_regiao": "cd_regiao",
        "nome_da_regiao": "nm_regiao",
        "codigo_do_estado": "cd_uf",
        "nome_do_estado": "nm_uf",
        "codigo_da_unidade_territorial": "cd_nu",
        "nome_da_unidade_territorial": "nm_nu",
        "codigo_da_aglomeracao_urbana": "cd_aglom",
        "nome_da_aglomeracao_urbana": "nm_aglom",
        "codigo_da_regiao_geografica_intermediaria": "cd_rgint",
        "nome_da_regiao_intermediaria": "nm_rgint",
        "cd_rgicodigo_da_regiao_geografica_imediata": "cd_rgi",
        "nome_da_regiao_imediata": "nm_rgi",
        "codigo_da_area_de_concentracao_urbana_conurbacao": "cd_concurb",
        "nome_da_area_de_concentracao_urbana": "nm_concurb",
        "area_km2": "area_km2",
        "total_de_pessoas": "total_pessoas",
        "total_de_domicilios": "total_domicilios",
        "total_de_domicilios_particulares": "total_domicilios_particulares",
        "total_de_domicilios_coletivos": "total_domicilios_coletivos",
        "media_de_moradores_em_domicilios_particulares_ocupados": "media_moradores_dom_part_ocup",
        "percentual_de_domicilios_particulares_ocupados_imputados": "perc_dom_part_ocup_imputados",
        "total_de_domicilios_particulares_ocupados": "total_dom_part_ocupados",
    }
    df = raw.rename(columns=rename_map)

    required = {"cd_mun", "nm_mun", "cd_uf", "nm_uf"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV IBGE sem colunas obrigatorias: {sorted(missing)}")

    df["cd_mun"] = df["cd_mun"].map(_digits_or_none).map(lambda x: x.zfill(7) if x else None)
    df["cd_mun6"] = df["cd_mun"].map(lambda x: x[:6] if x else None)
    df["nm_mun"] = df["nm_mun"].astype("string").str.strip()
    df["cd_uf"] = df["cd_uf"].map(_digits_or_none).map(lambda x: x.zfill(2) if x else None)
    df["nm_uf"] = df["nm_uf"].astype("string").str.strip()
    df["cd_regiao"] = df.get("cd_regiao", pd.Series(index=df.index)).map(_to_int)
    df["nm_regiao"] = df.get("nm_regiao", pd.Series(index=df.index)).astype("string").str.strip()
    df["cd_nu"] = df.get("cd_nu", pd.Series(index=df.index)).map(_digits_or_none)
    df["nm_nu"] = df.get("nm_nu", pd.Series(index=df.index)).astype("string").str.strip()
    df["cd_aglom"] = df.get("cd_aglom", pd.Series(index=df.index)).map(_digits_or_none)
    df["nm_aglom"] = df.get("nm_aglom", pd.Series(index=df.index)).astype("string").str.strip()
    df["cd_rgint"] = df.get("cd_rgint", pd.Series(index=df.index)).map(_digits_or_none)
    df["nm_rgint"] = df.get("nm_rgint", pd.Series(index=df.index)).astype("string").str.strip()
    df["cd_rgi"] = df.get("cd_rgi", pd.Series(index=df.index)).map(_digits_or_none)
    df["nm_rgi"] = df.get("nm_rgi", pd.Series(index=df.index)).astype("string").str.strip()
    df["cd_concurb"] = df.get("cd_concurb", pd.Series(index=df.index)).map(_digits_or_none)
    df["nm_concurb"] = df.get("nm_concurb", pd.Series(index=df.index)).astype("string").str.strip()

    df["area_km2"] = df.get("area_km2", pd.Series(index=df.index)).map(_to_decimal)
    df["total_pessoas"] = df.get("total_pessoas", pd.Series(index=df.index)).map(_to_int)
    df["total_domicilios"] = df.get("total_domicilios", pd.Series(index=df.index)).map(_to_int)
    df["total_domicilios_particulares"] = df.get(
        "total_domicilios_particulares", pd.Series(index=df.index)
    ).map(_to_int)
    df["total_domicilios_coletivos"] = df.get(
        "total_domicilios_coletivos", pd.Series(index=df.index)
    ).map(_to_int)
    df["media_moradores_dom_part_ocup"] = df.get(
        "media_moradores_dom_part_ocup", pd.Series(index=df.index)
    ).map(_to_decimal)
    df["perc_dom_part_ocup_imputados"] = df.get(
        "perc_dom_part_ocup_imputados", pd.Series(index=df.index)
    ).map(_to_decimal)
    df["total_dom_part_ocupados"] = df.get("total_dom_part_ocupados", pd.Series(index=df.index)).map(_to_int)
    df["fonte"] = "IBGE_CENSO_2022"

    out_cols = [
        "cd_mun",
        "cd_mun6",
        "nm_mun",
        "cd_regiao",
        "nm_regiao",
        "cd_uf",
        "nm_uf",
        "cd_nu",
        "nm_nu",
        "cd_aglom",
        "nm_aglom",
        "cd_rgint",
        "nm_rgint",
        "cd_rgi",
        "nm_rgi",
        "cd_concurb",
        "nm_concurb",
        "area_km2",
        "total_pessoas",
        "total_domicilios",
        "total_domicilios_particulares",
        "total_domicilios_coletivos",
        "media_moradores_dom_part_ocup",
        "perc_dom_part_ocup_imputados",
        "total_dom_part_ocupados",
        "fonte",
    ]
    out = df[out_cols].copy()
    out = out.dropna(subset=["cd_mun", "nm_mun"])
    return out


def _run_sql_file(path: Path) -> None:
    engine = get_engine()
    sql = path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))


def load_dim_ibge_municipio(csv_path: Path, schema: str) -> int:
    """Carrega dimensao IBGE no banco e retorna total de linhas inseridas."""
    settings = get_settings()
    ddl_path = settings.project_root / "sql" / "ddl" / "005_dim_ibge_municipio.sql"
    _run_sql_file(ddl_path)

    df = read_ibge_csv(csv_path)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{TARGET_TABLE}"))

    df.to_sql(
        name=TARGET_TABLE,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method=None,
        chunksize=1000,
    )
    logger.info("Dimensao IBGE carregada: %s linhas", len(df))
    return int(len(df))
