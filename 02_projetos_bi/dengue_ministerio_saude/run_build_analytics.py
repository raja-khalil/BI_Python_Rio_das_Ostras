"""Build da camada analitica: views, dimensoes e agregados."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from src.banco.database import get_engine
from src.config.settings import get_settings
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _run_sql_file(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with get_engine().begin() as conn:
        conn.exec_driver_sql(sql)


def main() -> None:
    settings = get_settings()
    ddl_dir = settings.project_root / "sql" / "ddl"

    ordered_files = [
        ddl_dir / "004_bi_views.sql",
        ddl_dir / "005_dim_ibge_municipio.sql",
        ddl_dir / "006_modelo_analitico.sql",
        ddl_dir / "007_agg_dengue_mensal.sql",
        ddl_dir / "014_materialized_views_painel_1_2.sql",
    ]

    for file in ordered_files:
        if not file.exists():
            raise FileNotFoundError(f"Arquivo DDL nao encontrado: {file}")
        logger.info("Aplicando DDL: %s", file.name)
        _run_sql_file(file)

    try:
        with get_engine().begin() as conn:
            conn.execute(text("SELECT saude.refresh_agg_dengue_mensal()"))
    except Exception as exc:
        logger.warning("Falha ao atualizar agg_dengue_mensal (seguindo com MVs): %s", exc)

    with get_engine().begin() as conn:
        conn.execute(text("SELECT saude.refresh_mvs_painel_1_2()"))

    logger.info("Camada analitica atualizada com sucesso.")


if __name__ == "__main__":
    main()
