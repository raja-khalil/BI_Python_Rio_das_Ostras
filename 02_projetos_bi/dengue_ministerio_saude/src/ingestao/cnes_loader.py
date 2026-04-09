"""Carga da dimensão CNES de estabelecimentos para enriquecimento de unidades."""

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

TARGET_TABLE = "dim_cnes_estabelecimento"


def _normalize_colname(name: str) -> str:
    cleaned = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", cleaned.strip().lower()).strip("_")
    return cleaned


def _digits(value: object, width: int | None = None) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip()
    if raw in {"", ".", "nan", "None"}:
        return None
    out = re.sub(r"\D+", "", raw)
    if not out:
        return None
    if width:
        return out.zfill(width)
    return out


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = _normalize_colname(cand)
        if key in cols:
            return cols[key]
    for c in df.columns:
        if _normalize_colname(c) in {_normalize_colname(x) for x in candidates}:
            return c
    return None


def _read_table(path: Path) -> pd.DataFrame:
    # CNES costuma vir em CSV/TXT com latin1 e separador ;.
    for sep, enc in [(";", "latin1"), (";", "utf-8"), (",", "latin1"), (",", "utf-8")]:
        try:
            return pd.read_csv(path, sep=sep, encoding=enc, low_memory=False)
        except Exception:
            continue
    raise ValueError(f"Nao foi possivel ler arquivo CNES: {path}")


def read_cnes_file(path: Path) -> pd.DataFrame:
    raw = _read_table(path)
    raw.columns = [_normalize_colname(c) for c in raw.columns]

    col_cnes = _pick_col(raw, ["cnes", "co_cnes", "codigo_cnes"])
    if col_cnes is None:
        raise ValueError("Arquivo CNES sem coluna de codigo CNES")

    col_nome_fantasia = _pick_col(raw, ["no_fantasia", "nome_fantasia"])
    col_nome_empresarial = _pick_col(raw, ["no_empresarial", "nome_empresarial", "nome"])
    col_razao = _pick_col(raw, ["razao_social", "no_razao_social"])
    col_uf = _pick_col(raw, ["uf", "co_uf_gestor", "sg_uf"])
    col_municipio = _pick_col(raw, ["municipio", "no_municipio_gestor", "no_municipio"])
    col_comp = _pick_col(raw, ["competencia", "cmp", "ano_mes"])

    out = pd.DataFrame()
    out["cnes"] = raw[col_cnes].map(lambda v: _digits(v, width=7))
    out["nome_fantasia"] = raw[col_nome_fantasia].astype("string").str.strip() if col_nome_fantasia else None
    out["nome_empresarial"] = raw[col_nome_empresarial].astype("string").str.strip() if col_nome_empresarial else None
    out["razao_social"] = raw[col_razao].astype("string").str.strip() if col_razao else None
    out["uf"] = raw[col_uf].astype("string").str.strip().str[:2] if col_uf else None
    out["municipio"] = raw[col_municipio].astype("string").str.strip() if col_municipio else None
    out["competencia"] = raw[col_comp].astype("string").str.strip().str[:20] if col_comp else None
    out["fonte"] = "CNES_DATASUS"

    out = out.dropna(subset=["cnes"]).copy()
    out = out.drop_duplicates(subset=["cnes"], keep="first").reset_index(drop=True)
    return out


def _run_sql_file(path: Path) -> None:
    engine = get_engine()
    sql = path.read_text(encoding="utf-8")
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))


def load_dim_cnes_estabelecimento(file_path: Path, schema: str) -> int:
    settings = get_settings()
    ddl_path = settings.project_root / "sql" / "ddl" / "009_dim_cnes_estabelecimento.sql"
    _run_sql_file(ddl_path)

    df = read_cnes_file(file_path)
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
    logger.info("Dimensao CNES carregada: %s linhas", len(df))
    return int(len(df))
